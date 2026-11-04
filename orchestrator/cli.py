"""Command line: validate | batch | run | status."""
import argparse
import logging
import os
import sys

from .backends import FluxBackend, make_backend
from .config import ConfigError, human_duration, load_config, time_plan
from .driver import EXIT_CONFIG, Campaign, load_state

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLACEHOLDER_BANK = "YOUR_BANK"


def _setup_logging(run_dir):
    os.makedirs(run_dir, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    root = logging.getLogger("orchestrator")
    root.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(), logging.FileHandler(os.path.join(run_dir, "driver.log"))):
        handler.setFormatter(formatter)
        root.addHandler(handler)


def _print_time_plan(config):
    plan = time_plan(config)
    timing = config.get("timing")
    print("Time plan")
    if timing:
        print("  member:   {} estimate + {} buffer = {} limit".format(
            human_duration(timing["member_estimate"]), human_duration(timing["buffer"]),
            human_duration(plan["member_limit"])))
        print("  analysis: {} estimate + {} buffer = {} limit".format(
            human_duration(timing["analysis_estimate"]), human_duration(timing["buffer"]),
            human_duration(plan["analysis_limit"])))
    if plan["total"] is None:
        print("  no estimates or job time limits are set, so no budget can be calculated")
    else:
        print("  each cycle: {} wave(s) x member limit + analysis limit = {}".format(
            plan["waves"], human_duration(plan["per_cycle"])))
        reserve = timing["allocation_reserve"] if timing else 0
        print("  campaign:   {} cycle(s) + {} reserve = {}".format(
            config["cycles"], human_duration(reserve), human_duration(plan["total"])))
    if plan["request"] is not None:
        print("  allocation: {} node(s) for {}".format(plan["nodes"],
                                                       human_duration(plan["request"])))
    for warning in plan["warnings"]:
        print("  WARNING: " + warning)
    return plan


def _batch_argv(config, plan, extra_flags):
    allocation = dict(config["allocation"] or {})
    allocation["nodes"] = plan["nodes"]
    driver = [sys.executable, os.path.join(PROJECT_ROOT, "bin", "orchestrate"), "run",
              config["config_path"], "--backend", "flux"] + extra_flags
    output = os.path.join(config["run_dir"], "driver-{{id}}.out")
    return FluxBackend().batch_command(allocation, plan["request"], config["campaign_name"],
                                       output, driver)


def cmd_validate(args):
    config = load_config(args.config)
    print("Config OK: {} member(s), {} cycle(s), run dir {}".format(
        config["ensemble"]["size"], config["cycles"], config["run_dir"]))
    plan = _print_time_plan(config)
    if plan["request"] is None:
        print("`batch` needs allocation.time, or a time estimate or limit for both components")
    else:
        print("\nAllocation command:\n  " + " ".join(_batch_argv(config, plan, [])))
    resources = config["ensemble"]["resources"]
    print("\nMember resources: " + " ".join(FluxBackend.resource_args(resources)))
    print("Member command:   " + " ".join(config["ensemble"]["command"]))
    print("Analysis resources: " + " ".join(
        FluxBackend.resource_args(config["analysis"]["resources"])))
    print("Analysis command:   " + " ".join(config["analysis"]["command"]))
    return 0


def cmd_batch(args):
    config = load_config(args.config)
    plan = _print_time_plan(config)
    if plan["request"] is None:
        print("error: set allocation.time, or time estimates or limits for both components",
              file=sys.stderr)
        return EXIT_CONFIG
    if (config["allocation"] or {}).get("bank") == PLACEHOLDER_BANK:
        print("error: set allocation.bank in {} (it is still {})".format(
            config["config_path"], PLACEHOLDER_BANK), file=sys.stderr)
        return EXIT_CONFIG
    os.makedirs(config["run_dir"], exist_ok=True)
    flags = []
    if args.allow_config_change:
        flags.append("--allow-config-change")
    if args.retry_failed:
        flags.append("--retry-failed")
    argv = _batch_argv(config, plan, flags)
    backend = FluxBackend()
    code, out, err = backend._run(argv)
    if code != 0:
        print("flux batch failed: " + err.strip(), file=sys.stderr)
        return code
    print("Submitted driver job {}".format(out.strip()))
    print("Check progress: {} status {}".format(
        os.path.join(PROJECT_ROOT, "bin", "orchestrate"), config["config_path"]))
    return 0


def cmd_run(args):
    config = load_config(args.config)
    _setup_logging(config["run_dir"])
    campaign = Campaign(config, make_backend(args.backend or config["backend"]))
    campaign.install_signal_handlers()
    return campaign.run(allow_config_change=args.allow_config_change,
                        retry_failed=args.retry_failed)


def _runtimes(cycle):
    return [task["history"][-1]["runtime"] for task in cycle["members"].values()
            if task["status"] == "SUCCEEDED" and task["history"][-1].get("runtime") is not None]


def cmd_status(args):
    config = load_config(args.config)
    state = load_state(config["run_dir"])
    if state is None:
        print("No run yet in " + config["run_dir"])
        return 0
    print("Campaign {}: {}{}".format(
        state["campaign"], state["status"],
        " ({})".format(state.get("message") or state.get("paused_reason"))
        if state.get("message") or state.get("paused_reason") else ""))
    measured = []
    for index in sorted(state["cycles"], key=int):
        cycle = state["cycles"][index]
        counts = {}
        for task in cycle["members"].values():
            counts[task["status"]] = counts.get(task["status"], 0) + 1
        analysis = cycle["analysis"]["status"] if cycle["analysis"] else "not started"
        print("Cycle {}: phase={} members {} analysis={}".format(
            index, cycle["phase"],
            " ".join("{}={}".format(key, counts[key]) for key in sorted(counts)), analysis))
        measured += _runtimes(cycle)
        tasks = sorted(cycle["members"].values(), key=lambda task: task["member_id"])
        if cycle["analysis"]:
            tasks.append(cycle["analysis"])
        for task in tasks:
            if not args.all and task["attempt"] <= 1 and task["status"] == "SUCCEEDED":
                continue
            name = "analysis" if task["kind"] == "analysis" else \
                "member {}".format(task["member_id"])
            print("  {}: {} attempt={} job={}".format(name, task["status"], task["attempt"],
                                                      task["job_id"]))
            for entry in task["history"]:
                if entry["kind"] == "success" and not args.all:
                    continue
                print("    attempt {}: {}{} -> {}".format(
                    entry["attempt"], entry["kind"],
                    " " + entry["reason"] if entry["reason"] else "", entry.get("decision")))
                if entry["detail"]:
                    print("      " + entry["detail"])
    if measured:
        line = "Measured member runtime: mean {} max {} over {} run(s)".format(
            human_duration(sum(measured) / len(measured)), human_duration(max(measured)),
            len(measured))
        if config.get("timing"):
            line += "; estimate {}, limit {}".format(
                human_duration(config["timing"]["member_estimate"]),
                human_duration(time_plan(config)["member_limit"]))
        print(line)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="orchestrate", description=__doc__)
    commands = parser.add_subparsers(dest="command")
    commands.required = True

    validate = commands.add_parser("validate", help="check a config and show the time plan")
    validate.add_argument("config")
    validate.set_defaults(func=cmd_validate)

    status = commands.add_parser("status", help="show campaign progress")
    status.add_argument("config")
    status.add_argument("--all", action="store_true", help="list every member")
    status.set_defaults(func=cmd_status)

    run = commands.add_parser("run", help="run the driver here, inside an allocation")
    batch = commands.add_parser("batch", help="submit the driver with `flux batch`")
    for sub in (run, batch):
        sub.add_argument("config")
        sub.add_argument("--allow-config-change", action="store_true")
        sub.add_argument("--retry-failed", action="store_true",
                         help="give failed tasks in the current cycle a fresh retry budget")
    run.add_argument("--backend", choices=["flux", "local"])
    run.set_defaults(func=cmd_run)
    batch.set_defaults(func=cmd_batch)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ConfigError as exc:
        print("config error: {}".format(exc), file=sys.stderr)
        return EXIT_CONFIG
