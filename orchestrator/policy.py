"""Decide why an attempt ended, and how a predicted failure changes the next attempt."""
import glob
import json
import math
import os

from .config import INT_RESOURCES, duration_seconds, format_seconds

SUCCESS = "success"
PREDICTED = "predicted"
UNPREDICTED = "unpredicted"
CANCELED = "canceled"
LOST = "lost"

FAILURE_FILE = "failure.json"
TIMEOUT_REASON = "TIMEOUT"


class ReconfigureError(Exception):
    pass


def _outcome(kind, reason=None, detail="", report=None):
    return {"kind": kind, "reason": reason, "detail": detail, "report": report or {}}


def _read_report(attempt_dir):
    """Return (report, problem). A malformed report must never read as a clean run."""
    path = os.path.join(attempt_dir, FAILURE_FILE)
    if not os.path.exists(path):
        return None, None
    try:
        with open(path) as handle:
            report = json.load(handle)
    except (OSError, ValueError) as exc:
        return None, "{} is unreadable: {}".format(FAILURE_FILE, exc)
    if not isinstance(report, dict) or not isinstance(report.get("reason"), str):
        return None, "{} has no string 'reason' field".format(FAILURE_FILE)
    if not isinstance(report.get("suggested_params", {}), dict):
        return None, "{}: suggested_params must be an object".format(FAILURE_FILE)
    return report, None


def classify(component, status, attempt_dir):
    """Classify one finished attempt. The order of these checks is the contract:

    1. Job unknown to Flux   -> lost (nothing to learn, costs no retry)
    2. Canceled              -> canceled (a person or tool chose this; never retried)
    3. failure.json present  -> predicted, using the reason it declares
    4. Declared exit code    -> predicted
    5. Flux TIMEOUT          -> predicted, if a TIMEOUT policy is declared
    6. Clean exit            -> success, if every expected output exists
    7. Anything else         -> unpredicted
    """
    policies = component["predicted_failures"]
    result, returncode = status.get("result"), status.get("returncode")
    summary = "flux result={} returncode={}".format(result, returncode)
    if status.get("exception_type"):
        summary += " exception={}: {}".format(status["exception_type"], status.get("exception_note"))

    if status.get("state") == "MISSING":
        return _outcome(LOST, "JOB_UNKNOWN_TO_FLUX", "job id is unknown to this Flux instance")
    if result == "CANCELED":
        return _outcome(CANCELED, "CANCELED", summary)

    report, problem = _read_report(attempt_dir)
    if problem:
        return _outcome(UNPREDICTED, "BAD_FAILURE_REPORT", problem + "; " + summary)
    if report:
        reason = report["reason"]
        message = report.get("message", "")
        if reason in policies:
            return _outcome(PREDICTED, reason, message, report)
        return _outcome(UNPREDICTED, "UNDECLARED_REASON",
                        "component reported {!r}, which has no policy. {}".format(reason, message),
                        report)

    if returncode in component["exit_code_map"]:
        return _outcome(PREDICTED, component["exit_code_map"][returncode], summary)
    if result == "TIMEOUT" and TIMEOUT_REASON in policies:
        return _outcome(PREDICTED, TIMEOUT_REASON, summary)

    if result == "COMPLETED" and returncode == 0:
        missing = [pattern for pattern in component["expected_outputs"]
                   if not glob.glob(os.path.join(attempt_dir, pattern))]
        if missing:
            return _outcome(UNPREDICTED, "MISSING_OUTPUT",
                            "exited 0 but produced nothing matching {}".format(missing))
        return _outcome(SUCCESS)
    return _outcome(UNPREDICTED, result or "UNKNOWN", summary)


def _changed_number(name, current, op, operand):
    if isinstance(current, bool) or not isinstance(current, (int, float)):
        raise ReconfigureError("cannot {} {}: current value is {!r}".format(op, name, current))
    return current * operand if op == "scale" else current + operand


def apply_changes(task, changes, suggested_params):
    """Apply a policy's changes to a task, then the component's own suggestions."""
    params = dict(task["params"])
    resources = dict(task["resources"])
    for name, change in changes.get("params", {}).items():
        op, operand = next(iter(change.items()))
        if op == "set":
            params[name] = operand
        elif name not in params:
            raise ReconfigureError("cannot {} param {}: it is not set".format(op, name))
        else:
            params[name] = _changed_number(name, params[name], op, operand)
    for name, change in changes.get("resources", {}).items():
        op, operand = next(iter(change.items()))
        if op == "set":
            resources[name] = operand
        elif name == "time":
            if resources.get("time") is None:
                raise ReconfigureError("cannot scale time: this job has no time limit")
            resources["time"] = format_seconds(duration_seconds(resources["time"]) * operand)
        else:
            value = _changed_number(name, resources.get(name), op, operand)
            # A job cannot ask for 1.5 nodes, so round resource counts up.
            resources[name] = int(math.ceil(value)) if name in INT_RESOURCES else value
    params.update(suggested_params or {})
    task["params"] = params
    task["resources"] = resources
