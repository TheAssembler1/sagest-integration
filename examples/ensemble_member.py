#!/usr/bin/env python3
"""Placeholder member. Replace with your code.

Predicted failure: exit with a code declared under ensemble.failures in the config.
Optionally write $ORCH_DIR/retry_params.json first to change parameters for the retry.
Crashes need no handling; they count as unpredicted failures.
"""
import json
import os
import sys
import time

with open(os.environ["ORCH_PARAMS"]) as handle:
    params = json.load(handle)
member, attempt = int(os.environ["ORCH_MEMBER_ID"]), int(os.environ["ORCH_ATTEMPT"])
print(f"member {member} attempt {attempt}: {params}")

# Demo failures: member 1 crashes once, and member 2 needs a smaller dt.
if params.get("demo_failures") and member == 1 and attempt == 1:
    raise RuntimeError("simulated crash")
if params.get("demo_failures") and member == 2 and params["dt"] > 0.005:
    with open(os.path.join(os.environ["ORCH_DIR"], "retry_params.json"), "w") as handle:
        json.dump({"dt": params["dt"] / 2}, handle)
    sys.exit(20)

time.sleep(params.get("sleep_seconds", 0))
with open(os.path.join(os.environ["ORCH_DIR"], "result.json"), "w") as handle:
    json.dump({"value": member * params["dt"] + params.get("forcing", 0.0)}, handle)
