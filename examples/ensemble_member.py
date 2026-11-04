#!/usr/bin/env python3
"""Placeholder ensemble member. Replace it with your code, keeping the failure protocol.

Inputs come from ORCH_* environment variables and the JSON file at ORCH_PARAMS_FILE.
To end with a predicted failure, write the report and exit with the declared code.
A plain crash needs no handling: the orchestrator counts it as unpredicted.
With several MPI ranks, only rank 0 should write the report.
"""
import json
import os
import sys
import time


def predicted_failure(reason, exit_code, message, suggested_params=None):
    """Report why this attempt failed. The exit code alone also works, without the details."""
    with open(os.environ["ORCH_FAILURE_FILE"], "w") as handle:
        json.dump({"reason": reason, "message": message,
                   "suggested_params": suggested_params or {}}, handle)
    sys.exit(exit_code)


def main():
    with open(os.environ["ORCH_PARAMS_FILE"]) as handle:
        params = json.load(handle)
    member = int(os.environ["ORCH_MEMBER_ID"])
    attempt = int(os.environ["ORCH_ATTEMPT"])
    print("member {} attempt {} cycle {} params {}".format(
        member, attempt, os.environ["ORCH_CYCLE"], params))

    # Demo failure modes, switched on per member from the config's member_params.
    if params.get("crash_on_attempt") == attempt:
        raise RuntimeError("simulated unpredicted crash")
    limit = params.get("stability_limit")
    if limit is not None and params["dt"] > limit:
        predicted_failure("NUMERICAL_INSTABILITY", 20,
                          "dt={} exceeds stability limit {}".format(params["dt"], limit))

    time.sleep(params.get("sleep_seconds", 0))
    result = {"member": member, "value": member * params["dt"] + params.get("forcing", 0.0)}
    with open(os.path.join(os.environ["ORCH_ATTEMPT_DIR"], "result.json"), "w") as handle:
        json.dump(result, handle)


if __name__ == "__main__":
    main()
