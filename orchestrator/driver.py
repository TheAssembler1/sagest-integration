"""The campaign loop: run the ensemble, gate on all members succeeding, analyse, repeat.

The driver enforces the analysis dependency itself instead of using Flux `afterok`.
A retried member gets a new job id, and when an `afterok` target fails Flux raises a
fatal exception on the dependent job, so `afterok` cannot span retries.

All progress is saved to state.json, so a stopped driver can resume.
"""
import json
import logging
import os
import signal
import tempfile
import time

from . import policy
from .backends import PollError, SubmitError
from .config import duration_seconds

PENDING = "PENDING"        # waiting to be submitted
SUBMITTING = "SUBMITTING"  # about to be submitted; the job id is not yet recorded
ACTIVE = "ACTIVE"          # has a job id
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"          # out of retries, or a fatal predicted failure
CANCELED = "CANCELED"
TERMINAL = (SUCCEEDED, FAILED, CANCELED)

EXIT_COMPLETED, EXIT_FAILED, EXIT_CONFIG, EXIT_PAUSED = 0, 1, 2, 3
# Confirmations needed before believing a job is gone, and failed polls before giving up.
MISSING_STRIKES = 3
MAX_CONSECUTIVE_POLL_FAILURES = 20

log = logging.getLogger("orchestrator")


class Paused(Exception):
    """Stop without failing. Progress is saved and the campaign can be resumed."""


def write_json_atomic(path, data):
    directory = os.path.dirname(path)
    handle, temp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(handle, "w") as out:
            json.dump(data, out, indent=1, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp_path, path)
    except BaseException:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def state_path(run_dir):
    return os.path.join(run_dir, "state.json")


def load_state(run_dir):
    path = state_path(run_dir)
    if not os.path.exists(path):
        return None
    with open(path) as handle:
        return json.load(handle)


def _new_task(kind, member_id, params, resources):
    return {"kind": kind, "member_id": member_id, "status": PENDING, "params": params,
            "resources": resources, "attempt": 0, "budget_used": 0, "unpredicted_failures": 0,
            "predicted_counts": {}, "job_id": None, "attempt_dir": None, "not_before": 0,
            "missing_polls": 0, "next_cycle": None, "history": []}


def _validate_next_cycle(data):
    if not isinstance(data, dict):
        return "must be a JSON object"
    unknown = set(data) - {"continue", "params", "member_params"}
    if unknown:
        return "unknown key(s) {}".format(sorted(unknown))
    if not isinstance(data.get("continue", True), bool):
        return "continue must be true or false"
    if not isinstance(data.get("params", {}), dict):
        return "params must be an object"
    members = data.get("member_params", {})
    if not isinstance(members, dict) or not all(isinstance(v, dict) for v in members.values()):
        return "member_params must map member ids to objects"
    return None


class Campaign(object):
    def __init__(self, config, backend, sleep=time.sleep):
        self.cfg = config
        self.backend = backend
        self.sleep = sleep
        self.run_dir = config["run_dir"]
        self.state = None
        self.stop_requested = False
        self.poll_failures = 0
        self.dirty = False

    # ---- lifecycle ---------------------------------------------------------------------

    def run(self, allow_config_change=False, retry_failed=False):
        """Start or resume the campaign and return a process exit code."""
        os.makedirs(self.run_dir, exist_ok=True)
        self.state = load_state(self.run_dir)
        if self.state is None:
            self.state = {"campaign": self.cfg["campaign_name"], "status": "running", "cycle": 0,
                          "config_hash": self.cfg["config_hash"], "carry": {"params": {},
                                                                           "member_params": {}},
                          "cycles": {}}
            self._start_cycle(0)
        else:
            code = self._prepare_resume(allow_config_change, retry_failed)
            if code is not None:
                return code
        self.state["status"] = "running"
        self._save(force=True)
        try:
            return self._advance()
        except Paused as exc:
            self.state["status"] = "paused"
            self.state["paused_reason"] = str(exc)
            self._save(force=True)
            log.warning("paused: %s. Run the same command again to resume.", exc)
            return EXIT_PAUSED

    def _prepare_resume(self, allow_config_change, retry_failed):
        status = self.state["status"]
        if self.state["config_hash"] != self.cfg["config_hash"]:
            if not allow_config_change:
                log.error("the config changed since this run started; pass "
                          "--allow-config-change to accept that")
                return EXIT_CONFIG
            self.state["config_hash"] = self.cfg["config_hash"]
        if status == "completed":
            log.info("campaign already completed")
            return EXIT_COMPLETED
        cycle = self._cycle()
        tasks = list(cycle["members"].values()) + ([cycle["analysis"]] if cycle["analysis"] else [])
        if status == "failed" and not retry_failed:
            log.error("campaign previously failed; fix the cause and pass --retry-failed")
            return EXIT_FAILED
        for task in tasks:
            if task["status"] == SUBMITTING:
                # The driver died mid-submit, so a job id may never have been recorded.
                # The next attempt uses a new directory, so a stray job cannot collide.
                task["status"] = PENDING
            if retry_failed and task["status"] in (FAILED, CANCELED):
                self._reset_task(task)
        log.info("resuming at cycle %d, phase %s", self.state["cycle"], cycle["phase"])
        return None

    def _reset_task(self, task):
        """Give a failed task a fresh retry budget and the current config's settings."""
        if task["kind"] == "ensemble":
            task["params"] = self._member_params(task["member_id"])
        else:
            task["params"] = dict(self.cfg["analysis"]["params"])
        task["resources"] = dict(self.cfg[task["kind"]]["resources"])
        task.update(status=PENDING, budget_used=0, unpredicted_failures=0, predicted_counts={},
                    not_before=0, job_id=None)

    def _advance(self):
        while True:
            cycle = self._cycle()
            if cycle["phase"] == "ensemble":
                members = list(cycle["members"].values())
                self._drive(members)
                if not all(task["status"] == SUCCEEDED for task in members):
                    return self._finish("failed", "ensemble did not fully succeed; "
                                        "analysis was not run")
                # The gate: this is the only place an analysis task is created.
                self._write_manifest(cycle)
                cycle["analysis"] = _new_task("analysis", None,
                                              dict(self.cfg["analysis"]["params"]),
                                              dict(self.cfg["analysis"]["resources"]))
                cycle["phase"] = "analysis"
                self._save(force=True)
            if cycle["phase"] == "analysis":
                self._drive([cycle["analysis"]])
                if cycle["analysis"]["status"] != SUCCEEDED:
                    return self._finish("failed", "analysis did not succeed")
                cycle["phase"] = "done"
                self._save(force=True)

            directive = cycle["analysis"]["next_cycle"] or {}
            self.state["carry"]["params"].update(directive.get("params", {}))
            for member, overrides in directive.get("member_params", {}).items():
                self.state["carry"]["member_params"].setdefault(str(member), {}).update(overrides)
            index = self.state["cycle"]
            if directive.get("continue", True) is False:
                return self._finish("completed", "analysis stopped the campaign after cycle "
                                    "{}".format(index))
            if index + 1 >= self.cfg["cycles"]:
                return self._finish("completed", "finished {} cycle(s)".format(index + 1))
            self.state["cycle"] = index + 1
            self._start_cycle(index + 1)
            self._save(force=True)

    def _finish(self, status, message):
        self.state["status"] = status
        self.state["message"] = message
        self._save(force=True)
        if status == "completed":
            log.info("campaign completed: %s", message)
            return EXIT_COMPLETED
        log.error("campaign failed: %s", message)
        for task in self._cycle()["members"].values():
            if task["status"] != SUCCEEDED:
                log.error("  member %s: %s after attempt %d", task["member_id"], task["status"],
                          task["attempt"])
        return EXIT_FAILED

    # ---- cycle setup -------------------------------------------------------------------

    def _cycle(self):
        return self.state["cycles"][str(self.state["cycle"])]

    def _member_params(self, member_id):
        params = dict(self.cfg["ensemble"]["params"])
        params.update(self.cfg["ensemble"]["member_params"].get(str(member_id), {}))
        params.update(self.state["carry"]["params"])
        params.update(self.state["carry"]["member_params"].get(str(member_id), {}))
        return params

    def _start_cycle(self, index):
        ensemble = self.cfg["ensemble"]
        log.info("starting cycle %d of %d with %d member(s)", index, self.cfg["cycles"],
                 ensemble["size"])
        self.state["cycles"][str(index)] = {
            "phase": "ensemble", "analysis": None,
            "members": {str(member): _new_task("ensemble", member, self._member_params(member),
                                               dict(ensemble["resources"]))
                        for member in range(ensemble["size"])}}
        self.dirty = True

    def _cycle_dir(self, index=None):
        index = self.state["cycle"] if index is None else index
        return os.path.join(self.run_dir, "cycle_{:04d}".format(index))

    def _manifest_path(self):
        return os.path.join(self._cycle_dir(), "members_manifest.json")

    def _write_manifest(self, cycle):
        members = [cycle["members"][key] for key in sorted(cycle["members"], key=int)]
        write_json_atomic(self._manifest_path(), {
            "cycle": self.state["cycle"],
            "ensemble_size": len(members),
            "members": [{"member_id": task["member_id"], "attempt": task["attempt"],
                         "attempt_dir": task["attempt_dir"], "job_id": task["job_id"],
                         "params": task["params"]} for task in members]})

    # ---- driving tasks -----------------------------------------------------------------

    def _drive(self, tasks):
        """Submit and poll until every task is terminal.

        Other members keep going after one fails permanently, so their finished
        work can be reused by --retry-failed.
        """
        while True:
            deferred = self._submit_ready(tasks)
            active = [task for task in tasks if task["status"] == ACTIVE]
            if active:
                self._poll(active)
            self._save()
            if all(task["status"] in TERMINAL for task in tasks):
                return
            if self.stop_requested:
                raise Paused("stop requested by signal")
            now = time.time()
            busy = any(task["status"] == ACTIVE or
                       (task["status"] == PENDING and task["not_before"] > now) for task in tasks)
            if deferred and not busy:
                raise Paused("{} job(s) need more time than this allocation has left".format(
                    len(deferred)))
            self.sleep(self.cfg["poll_interval_seconds"])

    def _submit_ready(self, tasks):
        """Submit what can finish in the allocation's remaining time; return the rest."""
        if self.stop_requested:
            return []
        now = time.time()
        ready = [t for t in tasks if t["status"] == PENDING and t["not_before"] <= now]
        if not ready:
            return []
        timeleft = self.backend.timeleft()
        deferred, batch = [], []
        for task in ready:
            limit = task["resources"].get("time")
            if timeleft is not None and limit and duration_seconds(limit) > timeleft:
                deferred.append(task)
            else:
                batch.append(task)
        for task in batch:
            task["attempt"] += 1
            task["budget_used"] += 1
            task["status"] = SUBMITTING
        if batch:
            self._save(force=True)
        for task in batch:
            self._submit(task, deferred)
        return deferred

    def _submit(self, task, deferred):
        component = self.cfg[task["kind"]]
        spec = self._build_spec(task, component)
        try:
            task["job_id"] = self.backend.submit(spec)
        except SubmitError as exc:
            if exc.out_of_time:
                # A race with the time check above. Not the job's fault, so no charge.
                task["status"] = PENDING
                task["budget_used"] -= 1
                deferred.append(task)
                return
            self._record_and_apply(task, component, {
                "kind": policy.UNPREDICTED, "reason": "SUBMIT_FAILED", "detail": str(exc),
                "report": {}}, None)
            return
        task["status"] = ACTIVE
        task["missing_polls"] = 0
        self.dirty = True
        log.info("%s attempt %d submitted as %s", self._label(task), task["attempt"],
                 task["job_id"])

    def _build_spec(self, task, component):
        cycle = self.state["cycle"]
        is_member = task["kind"] == "ensemble"
        owner = "member_{:04d}".format(task["member_id"]) if is_member else "analysis"
        owner_dir = os.path.join(self._cycle_dir(), owner)
        attempt_dir = os.path.join(owner_dir, "attempt_{:02d}".format(task["attempt"]))
        os.makedirs(attempt_dir, exist_ok=True)
        task["attempt_dir"] = attempt_dir
        params_file = os.path.join(attempt_dir, "params.json")
        write_json_atomic(params_file, task["params"])

        env = {str(key): str(value) for key, value in component["env"].items()}
        env.update({
            "ORCH_CAMPAIGN": self.cfg["campaign_name"],
            "ORCH_KIND": "member" if is_member else "analysis",
            "ORCH_CYCLE": str(cycle),
            "ORCH_ATTEMPT": str(task["attempt"]),
            "ORCH_ATTEMPT_DIR": attempt_dir,
            "ORCH_PARAMS_FILE": params_file,
            "ORCH_FAILURE_FILE": os.path.join(attempt_dir, policy.FAILURE_FILE),
            "ORCH_ENSEMBLE_SIZE": str(len(self._cycle()["members"])),
        })
        if is_member:
            env["ORCH_MEMBER_ID"] = str(task["member_id"])
        else:
            env["ORCH_MANIFEST"] = self._manifest_path()
            env["ORCH_NEXT_CYCLE_FILE"] = os.path.join(attempt_dir, "next_cycle.json")
        if task["attempt"] > 1:
            # For restarting from a checkpoint.
            env["ORCH_PREV_ATTEMPT_DIR"] = os.path.join(
                owner_dir, "attempt_{:02d}".format(task["attempt"] - 1))
        if cycle > 0:
            previous = self.state["cycles"][str(cycle - 1)]["analysis"]
            env["ORCH_PREV_ANALYSIS_DIR"] = previous["attempt_dir"]

        replacements = {"{attempt_dir}": attempt_dir, "{cycle}": str(cycle),
                        "{attempt}": str(task["attempt"]),
                        "{member_id}": str(task["member_id"]) if is_member else ""}
        command = []
        for token in component["command"]:
            for placeholder, value in replacements.items():
                token = token.replace(placeholder, value)
            command.append(token)
        suffix = "m{:04d}".format(task["member_id"]) if is_member else "analysis"
        return {"name": "{}-c{}-{}-a{}".format(self.cfg["campaign_name"], cycle, suffix,
                                               task["attempt"]),
                "command": command, "cwd": attempt_dir, "env": env,
                "stdout": os.path.join(attempt_dir, "stdout.log"),
                "stderr": os.path.join(attempt_dir, "stderr.log"),
                "resources": task["resources"]}

    def _poll(self, active):
        try:
            statuses = self.backend.poll([task["job_id"] for task in active])
            self.poll_failures = 0
        except PollError as exc:
            self.poll_failures += 1
            log.warning("could not read job states (%d in a row): %s", self.poll_failures, exc)
            if self.poll_failures >= MAX_CONSECUTIVE_POLL_FAILURES:
                raise Paused("could not read job states {} times in a row: {}".format(
                    self.poll_failures, exc))
            return
        for task in active:
            status = statuses.get(task["job_id"])
            if status is None:
                continue
            if status["state"] == "MISSING":
                task["missing_polls"] += 1
                if task["missing_polls"] < MISSING_STRIKES:
                    continue
            elif status["state"] != "INACTIVE":
                task["missing_polls"] = 0
                continue
            self._finished(task, status)

    def _finished(self, task, status):
        component = self.cfg[task["kind"]]
        outcome = policy.classify(component, status, task["attempt_dir"])
        if outcome["kind"] == policy.SUCCESS and task["kind"] == "analysis":
            outcome = self._read_next_cycle(task, outcome)
        self._record_and_apply(task, component, outcome, status)

    def _read_next_cycle(self, task, outcome):
        path = os.path.join(task["attempt_dir"], "next_cycle.json")
        if not os.path.exists(path):
            return outcome
        problem = None
        try:
            with open(path) as handle:
                data = json.load(handle)
            problem = _validate_next_cycle(data)
        except (OSError, ValueError) as exc:
            problem = str(exc)
        if problem:
            return {"kind": policy.UNPREDICTED, "reason": "BAD_NEXT_CYCLE",
                    "detail": "next_cycle.json: {}".format(problem), "report": {}}
        task["next_cycle"] = data
        return outcome

    def _record_and_apply(self, task, component, outcome, status):
        status = status or {}
        entry = {"attempt": task["attempt"], "job_id": task["job_id"], "kind": outcome["kind"],
                 "reason": outcome["reason"], "detail": outcome["detail"],
                 "flux_result": status.get("result"), "returncode": status.get("returncode"),
                 "runtime": status.get("runtime"), "attempt_dir": task["attempt_dir"],
                 "params": task["params"], "resources": task["resources"]}
        task["history"].append(entry)
        entry["decision"] = self._decide(task, component, outcome)
        self.dirty = True
        level = logging.INFO if outcome["kind"] == policy.SUCCESS else logging.WARNING
        log.log(level, "%s attempt %d: %s%s -> %s", self._label(task), task["attempt"],
                outcome["kind"], " ({})".format(outcome["reason"]) if outcome["reason"] else "",
                entry["decision"])

    def _decide(self, task, component, outcome):
        """Apply the outcome to the task, returning the decision for logs and status."""
        kind = outcome["kind"]
        if kind == policy.SUCCESS:
            task["status"] = SUCCEEDED
            return "done"
        if kind == policy.CANCELED:
            task["status"] = CANCELED
            return "not retried: canceled"
        if kind == policy.LOST:
            return self._requeue(task, component, 0, "resubmit (no retry charged)")
        if kind == policy.UNPREDICTED:
            task["unpredicted_failures"] += 1
            limit = component["max_unpredicted_retries"]
            if task["unpredicted_failures"] > limit:
                return self._give_up(task, "used all {} unpredicted retries".format(limit))
            return self._requeue(task, component, component["retry_delay_seconds"],
                                 "retry {}/{}".format(task["unpredicted_failures"], limit))

        reason = outcome["reason"]
        rule = component["predicted_failures"][reason]
        if rule["action"] == "fatal":
            return self._give_up(task, "fatal reason {}".format(reason))
        used = task["predicted_counts"].get(reason, 0) + 1
        task["predicted_counts"][reason] = used
        if used > rule["max_retries"]:
            return self._give_up(task, "used all {} retries for {}".format(rule["max_retries"],
                                                                         reason))
        if rule["action"] == "reconfigure":
            before = (task["params"], task["resources"])
            try:
                policy.apply_changes(task, rule.get("changes", {}),
                                     outcome["report"].get("suggested_params"))
            except policy.ReconfigureError as exc:
                return self._give_up(task, "could not reconfigure: {}".format(exc))
            changed = [key for key in task["params"] if task["params"][key] != before[0].get(key)]
            changed += [key for key in task["resources"]
                        if task["resources"][key] != before[1].get(key)]
            return self._requeue(task, component, 0, "reconfigure {}/{} changing {}".format(
                used, rule["max_retries"], changed or "nothing"))
        return self._requeue(task, component, 0, "retry {}/{}".format(used, rule["max_retries"]))

    def _requeue(self, task, component, delay, decision):
        # The backstop against loops, however the failures were classified.
        if task["budget_used"] >= component["max_total_attempts"]:
            return self._give_up(task, "reached max_total_attempts={}".format(
                component["max_total_attempts"]))
        task["status"] = PENDING
        task["not_before"] = time.time() + delay
        return decision

    @staticmethod
    def _give_up(task, why):
        task["status"] = FAILED
        return "gave up: {}".format(why)

    @staticmethod
    def _label(task):
        return "analysis" if task["kind"] == "analysis" else "member {}".format(task["member_id"])

    def _save(self, force=False):
        if force or self.dirty:
            write_json_atomic(state_path(self.run_dir), self.state)
            self.dirty = False

    def install_signal_handlers(self):
        def request_stop(signum, _frame):
            log.warning("signal %d received; saving state and stopping", signum)
            self.stop_requested = True
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
