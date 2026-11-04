#!/usr/bin/env python3
"""Test job. params["script"] gives one action per attempt: ok | exit:N | sleep:S | crash.
The analysis exits 99 if any member's result is missing, which proves the dependency."""
import json
import os
import signal
import sys
import time

with open(os.environ["ORCH_PARAMS"]) as handle:
    params = json.load(handle)
attempt_dir = os.environ["ORCH_DIR"]
script = params.get("script", ["ok"])
action = script[min(int(os.environ["ORCH_ATTEMPT"]), len(script)) - 1]

if "ORCH_MANIFEST" in os.environ:
    with open(os.environ["ORCH_MANIFEST"]) as handle:
        members = json.load(handle)["members"]
    if len(members) != int(os.environ["ORCH_ENSEMBLE_SIZE"]) or not all(
            os.path.exists(os.path.join(m["dir"], "result.txt")) for m in members):
        sys.exit(99)
    if "next_cycle" in params:
        with open(os.path.join(attempt_dir, "next_cycle.json"), "w") as handle:
            handle.write(params["next_cycle"])

if "retry_params" in params and action != "ok":
    with open(os.path.join(attempt_dir, "retry_params.json"), "w") as handle:
        handle.write(params["retry_params"])
if action.startswith("exit:"):
    sys.exit(int(action[5:]))
if action == "crash":
    os.kill(os.getpid(), signal.SIGSEGV)
if action.startswith("sleep:"):
    time.sleep(float(action[6:]))
with open(os.path.join(attempt_dir, "result.txt"), "w") as handle:
    handle.write("done")
