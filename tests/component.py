#!/usr/bin/env python3
"""Scriptable test component. params["script"] holds one action per attempt:

ok | nooutput | exit:N | crash | sleep:S | report:REASON:RC[:SUGGESTED_JSON] | garbage_report
The analysis also fails with exit 99 if any member result is absent, which proves the gate.
"""
import json
import os
import signal
import sys
import time


def main():
    with open(os.environ["ORCH_PARAMS_FILE"]) as handle:
        params = json.load(handle)
    attempt_dir = os.environ["ORCH_ATTEMPT_DIR"]
    attempt = int(os.environ["ORCH_ATTEMPT"])
    script = params.get("script", ["ok"])
    action = script[min(attempt, len(script)) - 1]

    if os.environ["ORCH_KIND"] == "analysis":
        with open(os.environ["ORCH_MANIFEST"]) as handle:
            manifest = json.load(handle)
        complete = len(manifest["members"]) == int(os.environ["ORCH_ENSEMBLE_SIZE"]) and all(
            os.path.exists(os.path.join(m["attempt_dir"], "result.txt"))
            for m in manifest["members"])
        if not complete:
            sys.exit(99)
        directive = params.get("next_by_cycle", {}).get(os.environ["ORCH_CYCLE"])
        if directive is not None:
            with open(os.environ["ORCH_NEXT_CYCLE_FILE"], "w") as handle:
                handle.write(directive if isinstance(directive, str) else json.dumps(directive))

    if action.startswith("exit:"):
        sys.exit(int(action.split(":")[1]))
    if action == "crash":
        os.kill(os.getpid(), signal.SIGSEGV)
    if action.startswith("sleep:"):
        time.sleep(float(action.split(":")[1]))
    if action == "garbage_report":
        with open(os.environ["ORCH_FAILURE_FILE"], "w") as handle:
            handle.write("{not json")
        sys.exit(1)
    if action.startswith("report:"):
        parts = action.split(":", 3)
        report = {"reason": parts[1], "message": "from component"}
        if len(parts) == 4:
            report["suggested_params"] = json.loads(parts[3])
        with open(os.environ["ORCH_FAILURE_FILE"], "w") as handle:
            json.dump(report, handle)
        sys.exit(int(parts[2]))
    if action != "nooutput":
        with open(os.path.join(attempt_dir, "result.txt"), "w") as handle:
            handle.write("done")


if __name__ == "__main__":
    main()
