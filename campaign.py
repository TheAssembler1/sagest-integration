#!/usr/bin/env python3
"""Minimal wrapper over Flux: ensemble -> post-analysis -> repeat. See README.md.

Runs outside Flux, such as on a login node in tmux. Every member and analysis is its own
Flux job, and Flux schedules and launches all of them. This only adds what Flux has no
feature for: retries, reacting to failure reasons, waiting for all members across retries,
and cycles.

Usage: campaign.py run CONFIG [--dry-run]  |  campaign.py status CONFIG
Exit codes: 0 completed, 1 failed, 3 paused. Rerun the same command to resume; rerunning
a failed campaign retries its failed tasks with fresh retry budgets.
"""
import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time

JOB_FORMAT = "{id.f58}|{result}|{returncode}|{runtime}|{exception.note}"
MISSING_STRIKES = 3      # sightings as "unknown" before believing a job is gone
MAX_POLL_FAILURES = 20   # failed status queries in a row before pausing


class Pause(Exception):
    pass


class PollError(Exception):
    pass


def log(message):
    print(time.strftime("%H:%M:%S"), message, flush=True)


def seconds(duration):
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", str(duration))
    if not match:
        sys.exit(f"config error: bad duration {duration!r} (use e.g. 90s, 30m, 4h)")
    return float(match.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]


def flux(*args, env=None):
    """Run a flux command, returning (exit code, stdout, stderr)."""
    full_env = dict(os.environ, FLUX_F58_FORCE_ASCII="1", **(env or {}))
    try:
        done = subprocess.run(["flux"] + [str(a) for a in args], env=full_env, timeout=120,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              encoding="utf-8", errors="replace")
    except (OSError, subprocess.SubprocessError) as exc:
        return 1, "", str(exc)
    return done.returncode, done.stdout, done.stderr


def poll(job_ids):
    """Return {id: status}, with None for jobs Flux says it doesn't know."""
    code, out, err = flux("jobs", "-n", "-o", JOB_FORMAT, *job_ids)
    return parse_jobs(job_ids, code, out, err)


def parse_jobs(job_ids, code, out, err):
    jobs = {}
    for line in out.splitlines():
        fields = line.split("|", 4)
        if len(fields) == 5 and fields[0] in job_ids:
            rc, runtime = fields[2].strip(), fields[3].strip()
            jobs[fields[0]] = {"result": fields[1].strip() or None,
                               "rc": int(rc) if re.fullmatch(r"-?\d+", rc) else None,
                               "runtime": float(runtime) if runtime else None,
                               "note": fields[4].strip()}
    # Flux can exit 0 here if it found other ids, so read stderr instead of the exit code.
    unknown = set(re.findall(r"JobID (\S+) unknown", err))
    for job_id in job_ids:
        if job_id not in jobs:
            if job_id not in unknown:
                # A failed query doesn't mean the job is gone. Assuming so would
                # resubmit a running ensemble during a brief Flux hiccup.
                raise PollError(f"no status for {job_id} (exit {code}): {err.strip()}")
            jobs[job_id] = None
    return jobs


# ---- config -----------------------------------------------------------------------------

def load_config(path):
    path = os.path.abspath(path)
    with open(path) as handle:
        cfg = json.load(handle)
    base = os.path.dirname(path)
    cfg["config_path"] = path
    cfg["run_dir"] = os.path.normpath(os.path.join(base, cfg["run_dir"]))
    for name in ("ensemble", "analysis"):
        comp = cfg[name]
        comp["command"] = [c.replace("{config_dir}", base) for c in comp["command"]]
        for reason, rule in comp.get("failures", {}).items():
            where = f"{name}.failures.{reason}"
            unknown = set(rule) - {"exit_code", "retries", "set", "time_scale"}
            if unknown or not isinstance(rule.get("retries"), int):
                sys.exit(f"config error: {where} needs integer retries; unknown keys: {unknown}")
            if reason != "TIMEOUT" and not 3 <= rule.get("exit_code", 0) <= 125:
                sys.exit(f"config error: {where} needs an exit_code in 3..125")
    time_limit(cfg, "ensemble"), time_limit(cfg, "analysis")
    return cfg


def time_limit(cfg, kind):
    """A job's limit is its estimated runtime plus the buffer."""
    times = cfg["time"]
    estimate = times["member" if kind == "ensemble" else "analysis"]
    return seconds(estimate) + seconds(times.get("buffer", "0s"))


# ---- state ------------------------------------------------------------------------------

def save(cfg, state):
    fd, temp = tempfile.mkstemp(dir=cfg["run_dir"], prefix=".state-")
    with os.fdopen(fd, "w") as handle:
        json.dump(state, handle, indent=1)
    os.replace(temp, os.path.join(cfg["run_dir"], "state.json"))


def new_task(cfg, kind, name, params):
    return {"kind": kind, "name": name, "status": "PENDING", "params": params,
            "time": time_limit(cfg, kind), "attempt": 0, "job": None, "dir": None, "missing": 0,
            "unpredicted": 0, "predicted": {}, "history": [], "next_cycle": None}


def member_params(cfg, state):
    return dict(cfg["ensemble"].get("params", {}), **state["carry"])


def new_cycle(cfg, state):
    log(f"starting cycle {len(state['cycles'])}")
    state["cycles"].append({"analysis": None, "members": [
        new_task(cfg, "ensemble", f"member_{i}", member_params(cfg, state))
        for i in range(cfg["ensemble"]["size"])]})


# ---- one job ----------------------------------------------------------------------------

def submit_args(cfg, kind, job_name, attempt_dir, limit):
    comp = cfg[kind]
    return ["submit", f"--job-name={job_name}", f"--cwd={attempt_dir}", "--output=stdout.log",
            "--error=stderr.log", "-t", f"{math.ceil(limit)}s", *cfg.get("flux_args", []),
            *comp.get("flux_args", []), *comp["command"]]


def submit(cfg, state, task):
    cycle = len(state["cycles"]) - 1
    attempt_dir = os.path.join(cfg["run_dir"], f"cycle_{cycle}", task["name"],
                               f"attempt_{task['attempt']}")
    os.makedirs(attempt_dir, exist_ok=True)
    task["dir"] = attempt_dir
    params_file = os.path.join(attempt_dir, "params.json")
    with open(params_file, "w") as handle:
        json.dump(task["params"], handle, indent=1)
    env = {"ORCH_DIR": attempt_dir, "ORCH_PARAMS": params_file, "ORCH_CYCLE": str(cycle),
           "ORCH_ATTEMPT": str(task["attempt"]),
           "ORCH_ENSEMBLE_SIZE": str(len(state["cycles"][-1]["members"]))}
    if task["kind"] == "ensemble":
        env["ORCH_MEMBER_ID"] = task["name"].split("_")[1]
    else:
        env["ORCH_MANIFEST"] = os.path.join(cfg["run_dir"], f"cycle_{cycle}", "manifest.json")
    job_name = f"{cfg['name']}-c{cycle}-{task['name']}-a{task['attempt']}"
    code, out, err = flux(*submit_args(cfg, task["kind"], job_name, attempt_dir, task["time"]),
                          env=env)
    if code != 0 or not out.strip():
        finish(cfg, task, {"result": "SUBMIT_FAILED", "rc": None, "runtime": None,
                           "note": err.strip()})
        return
    task["job"], task["status"], task["missing"] = out.split()[-1], "RUNNING", 0
    log(f"{task['name']} attempt {task['attempt']} submitted as {task['job']}")


def read_json(path):
    """Return parsed JSON, or None if the file is absent. Raises ValueError if malformed."""
    if not os.path.exists(path):
        return None
    with open(path) as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def classify(comp, job):
    """Return (kind, reason). Flux's job result and exit code decide everything."""
    if job is None:
        return "lost", "unknown to Flux"
    if job["result"] == "CANCELED":
        return "canceled", "CANCELED"
    failures = comp.get("failures", {})
    for reason, rule in failures.items():
        if "exit_code" in rule and rule["exit_code"] == job["rc"]:
            return "predicted", reason
    if job["result"] == "TIMEOUT" and "TIMEOUT" in failures:
        return "predicted", "TIMEOUT"
    if job["result"] == "COMPLETED" and job["rc"] == 0:
        return "success", None
    return "unpredicted", f"{job['result']} rc={job['rc']} {job['note']}".strip()


def finish(cfg, task, job):
    """Record one ended attempt and decide what happens to the task next."""
    comp = cfg[task["kind"]]
    kind, reason = classify(comp, job)
    if kind == "success" and task["kind"] == "analysis":
        try:
            task["next_cycle"] = read_json(os.path.join(task["dir"], "next_cycle.json"))
        except ValueError as exc:
            kind, reason = "unpredicted", f"bad next_cycle.json: {exc}"
    if kind == "predicted":
        rule = comp["failures"][reason]
        used = task["predicted"].get(reason, 0) + 1
        task["predicted"][reason] = used
        if used > rule["retries"]:
            decision = f"gave up after {rule['retries']} retries"
        else:
            try:
                suggested = read_json(os.path.join(task["dir"], "retry_params.json")) or {}
                task["params"].update(rule.get("set", {}), **suggested)
                task["time"] *= rule.get("time_scale", 1)
                decision = f"retry {used}/{rule['retries']}"
            except ValueError as exc:
                decision = f"gave up: bad retry_params.json: {exc}"
    elif kind == "unpredicted":
        task["unpredicted"] += 1
        limit = comp.get("retries", 0)
        over = task["unpredicted"] > limit
        decision = f"gave up after {limit} retries" if over else \
            f"retry {task['unpredicted']}/{limit}"
    else:
        decision = {"success": "done", "lost": "resubmit (no retry used)",
                    "canceled": "not retried"}[kind]

    task["status"] = {"success": "DONE", "canceled": "CANCELED"}.get(
        kind, "FAILED" if decision.startswith("gave up") else "PENDING")
    task["history"].append({"attempt": task["attempt"], "job": task["job"], "kind": kind,
                            "reason": reason, "decision": decision,
                            "runtime": job["runtime"] if job else None})
    log(f"{task['name']} attempt {task['attempt']}: {kind}"
        f"{' (' + reason + ')' if reason else ''} -> {decision}")


# ---- the campaign -----------------------------------------------------------------------

def drive(cfg, state, tasks):
    """Submit and poll until every task has finished, retrying as needed."""
    poll_failures = 0
    while True:
        for task in [t for t in tasks if t["status"] == "PENDING"]:
            task["attempt"] += 1  # saved first, so a crash mid-submit can't reuse a directory
            save(cfg, state)
            submit(cfg, state, task)
            save(cfg, state)  # record the job id at once; these jobs outlive this process

        running = [t for t in tasks if t["status"] == "RUNNING"]
        if running:
            try:
                jobs = {}
                for start in range(0, len(running), 500):
                    jobs.update(poll([t["job"] for t in running[start:start + 500]]))
                poll_failures = 0
            except PollError as exc:
                poll_failures += 1
                log(f"warning: {exc}")
                if poll_failures >= MAX_POLL_FAILURES:
                    raise Pause(f"cannot read job states: {exc}")
                jobs = {}
            for task in running:
                if task["job"] not in jobs:
                    continue
                job = jobs[task["job"]]
                if job is None:
                    task["missing"] += 1
                    if task["missing"] >= MISSING_STRIKES:
                        finish(cfg, task, None)
                elif job["result"]:
                    finish(cfg, task, job)
        save(cfg, state)

        if not any(t["status"] in ("PENDING", "RUNNING") for t in tasks):
            return
        time.sleep(cfg.get("poll_seconds", 60))


def advance(cfg, state):
    while True:
        cycle = state["cycles"][-1]
        drive(cfg, state, cycle["members"])
        if any(t["status"] != "DONE" for t in cycle["members"]):
            return "failed", "ensemble did not fully succeed; analysis was not run"
        # The dependency: the analysis task only exists once every member is DONE.
        if cycle["analysis"] is None:
            with open(os.path.join(os.path.dirname(os.path.dirname(cycle["members"][0]["dir"])),
                                   "manifest.json"), "w") as handle:
                json.dump({"cycle": len(state["cycles"]) - 1,
                           "members": [{"name": t["name"], "dir": t["dir"], "params": t["params"]}
                                       for t in cycle["members"]]}, handle, indent=1)
            cycle["analysis"] = new_task(cfg, "analysis", "analysis",
                                         dict(cfg["analysis"].get("params", {})))
        drive(cfg, state, [cycle["analysis"]])
        if cycle["analysis"]["status"] != "DONE":
            return "failed", "analysis did not succeed"
        directive = cycle["analysis"]["next_cycle"] or {}
        state["carry"].update(directive.get("params", {}))
        if directive.get("continue", True) is False:
            return "completed", "analysis ended the campaign"
        if len(state["cycles"]) >= cfg.get("cycles", 1):
            return "completed", f"finished {len(state['cycles'])} cycle(s)"
        new_cycle(cfg, state)
        save(cfg, state)


def run(cfg):
    os.makedirs(cfg["run_dir"], exist_ok=True)
    path = os.path.join(cfg["run_dir"], "state.json")
    if os.path.exists(path):
        with open(path) as handle:
            state = json.load(handle)
        if state["status"] == "completed":
            log("already completed")
            return 0
        cycle = state["cycles"][-1]
        for task in cycle["members"] + ([cycle["analysis"]] if cycle["analysis"] else []):
            if task["status"] in ("FAILED", "CANCELED"):
                # Fresh budget and the current config's settings.
                params = member_params(cfg, state) if task["kind"] == "ensemble" \
                    else dict(cfg["analysis"].get("params", {}))
                task.update(status="PENDING", params=params, unpredicted=0, predicted={},
                            time=time_limit(cfg, task["kind"]))
        log(f"resuming at cycle {len(state['cycles']) - 1}")
    else:
        state = {"status": "running", "message": "", "carry": {}, "cycles": []}
        new_cycle(cfg, state)
    state.update(status="running", message="")
    save(cfg, state)
    try:
        status, message = advance(cfg, state)
    except Pause as exc:
        status, message = "paused", str(exc)
    state.update(status=status, message=message)
    save(cfg, state)
    log(f"campaign {status}: {message}")
    return {"completed": 0, "failed": 1, "paused": 3}[status]


# ---- commands ---------------------------------------------------------------------------

def dry_run(cfg):
    size, directory = cfg["ensemble"]["size"], "<attempt dir>"
    print(f"{size} member job(s) per cycle, {cfg.get('cycles', 1)} cycle(s). Example commands:")
    for kind, name in (("ensemble", "member_0"), ("analysis", "analysis")):
        args = submit_args(cfg, kind, f"{cfg['name']}-c0-{name}-a1", directory,
                           time_limit(cfg, kind))
        print("flux " + " ".join(str(a) for a in args))
    return 0


def status(cfg):
    path = os.path.join(cfg["run_dir"], "state.json")
    if not os.path.exists(path):
        print("not started")
        return 0
    with open(path) as handle:
        state = json.load(handle)
    print(f"{state['status']}: {state['message']}")
    runtimes = []
    for index, cycle in enumerate(state["cycles"]):
        tasks = cycle["members"] + ([cycle["analysis"]] if cycle["analysis"] else [])
        done = sum(t["status"] == "DONE" for t in cycle["members"])
        print(f"cycle {index}: {done}/{len(cycle['members'])} members done, analysis "
              f"{cycle['analysis']['status'] if cycle['analysis'] else 'not started'}")
        for task in tasks:
            for entry in task["history"]:
                if entry["kind"] == "success":
                    if task["kind"] == "ensemble" and entry["runtime"]:
                        runtimes.append(entry["runtime"])
                else:
                    print(f"  {task['name']} attempt {entry['attempt']}: {entry['kind']} "
                          f"({entry['reason']}) -> {entry['decision']}")
    if runtimes:
        print(f"member runtime: mean {sum(runtimes) / len(runtimes) / 60:.1f}m, "
              f"max {max(runtimes) / 60:.1f}m, estimate {seconds(cfg['time']['member']) / 60:.1f}m")
    return 0


def inside_flux_job():
    """True inside a flux submit job, or inside a flux batch or alloc allocation."""
    if "FLUX_JOB_ID" in os.environ:
        return True
    code, out, _ = flux("getattr", "instance-level")
    return code == 0 and out.strip() != "0"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["run", "status"])
    parser.add_argument("config")
    parser.add_argument("--dry-run", action="store_true", help="run: only print example commands")
    args = parser.parse_args()
    cfg = load_config(args.config)
    if args.command == "status":
        return status(cfg)
    if args.dry_run:
        return dry_run(cfg)
    if "YOUR_BANK" in cfg.get("flux_args", []):
        sys.exit("config error: replace YOUR_BANK in flux_args")
    if inside_flux_job():
        sys.exit("error: campaign.py must run outside Flux jobs, such as on a login node")
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
