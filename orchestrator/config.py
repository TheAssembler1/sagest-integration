"""Load and strictly validate a campaign config. See README.md for the schema."""
import copy
import hashlib
import json
import math
import os
import re


class ConfigError(Exception):
    pass


TOP_KEYS = {"campaign_name", "run_dir", "backend", "cycles", "poll_interval_seconds",
            "timing", "allocation", "ensemble", "analysis"}
TIMING_KEYS = {"member_estimate", "analysis_estimate", "buffer", "allocation_reserve"}
ALLOCATION_KEYS = {"nodes", "time", "max_time", "queue", "bank", "extra_args"}
COMPONENT_KEYS = {"command", "resources", "params", "env", "max_unpredicted_retries",
                  "retry_delay_seconds", "max_total_attempts", "predicted_failures",
                  "expected_outputs"}
ENSEMBLE_ONLY_KEYS = {"size", "member_params"}
RESOURCE_DEFAULTS = {"nodes": None, "tasks": 1, "cores_per_task": 1, "gpus_per_task": 0,
                     "time": None, "exclusive": False, "extra_args": []}
POLICY_KEYS = {"exit_code", "action", "max_retries", "changes", "description"}
ACTIONS = {"retry", "reconfigure", "fatal"}
CHANGE_OPS = {"set", "scale", "add"}
INT_RESOURCES = {"nodes", "tasks", "cores_per_task", "gpus_per_task"}
REASON_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
# 0 is success, 1-2 are generic errors, and 126+ are shell errors or 128+signal.
# Declared reasons must stay outside those so they cannot be confused with a crash.
MIN_EXIT_CODE, MAX_EXIT_CODE = 3, 125

_FSD_RE = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h|d)?$")
_FSD_SECONDS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def duration_seconds(value):
    """Convert a Flux duration to seconds. As with `flux submit -t`, no unit means minutes."""
    match = _FSD_RE.match(str(value).strip())
    if not match:
        raise ValueError("invalid duration {!r} (use e.g. 90s, 30m, 4h)".format(value))
    return float(match.group(1)) * _FSD_SECONDS[match.group(2) or "m"]


def format_seconds(seconds):
    """Render seconds in the whole-second form passed to Flux."""
    return "{}s".format(int(math.ceil(seconds)))


def human_duration(seconds):
    seconds = int(math.ceil(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return "{}h{:02d}m{:02d}s".format(hours, minutes, secs)


def _require(condition, message):
    if not condition:
        raise ConfigError(message)


def _check_keys(obj, allowed, where):
    _require(isinstance(obj, dict), "{} must be an object".format(where))
    unknown = set(obj) - allowed
    _require(not unknown, "{}: unknown key(s) {}; allowed: {}".format(
        where, sorted(unknown), sorted(allowed)))


def _is_int(value, minimum):
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _check_duration(value, where):
    try:
        return duration_seconds(value)
    except ValueError as exc:
        raise ConfigError("{}: {}".format(where, exc))


def _check_changes(changes, where):
    _check_keys(changes, {"params", "resources"}, where)
    for section, entries in changes.items():
        _require(isinstance(entries, dict), "{}.{} must be an object".format(where, section))
        for name, change in entries.items():
            label = "{}.{}.{}".format(where, section, name)
            _require(isinstance(change, dict) and len(change) == 1 and set(change) <= CHANGE_OPS,
                     "{} must have exactly one of {}".format(label, sorted(CHANGE_OPS)))
            op, operand = next(iter(change.items()))
            if op != "set":
                _require(isinstance(operand, (int, float)) and not isinstance(operand, bool),
                         "{}: {} needs a number".format(label, op))
            if section == "resources":
                _require(name in RESOURCE_DEFAULTS, "{}: not a resource".format(label))
                if name == "time":
                    _require(op != "add", "{}: use scale or set for time".format(label))
                    if op == "set":
                        _check_duration(operand, label)


def _check_policies(policies, where):
    _require(isinstance(policies, dict), "{} must be an object".format(where))
    seen_codes = {}
    for reason, policy in policies.items():
        label = "{}.{}".format(where, reason)
        _require(REASON_RE.match(reason), "{}: reason must be UPPER_SNAKE_CASE".format(label))
        _check_keys(policy, POLICY_KEYS, label)
        action = policy.get("action")
        _require(action in ACTIONS, "{}: action must be one of {}".format(label, sorted(ACTIONS)))
        if action != "fatal":
            _require(_is_int(policy.get("max_retries"), 0),
                     "{}: max_retries (integer >= 0) is required for {}".format(label, action))
        if "changes" in policy:
            _require(action == "reconfigure", "{}: changes needs action=reconfigure".format(label))
            _check_changes(policy["changes"], label + ".changes")
        if "exit_code" in policy:
            code = policy["exit_code"]
            _require(_is_int(code, MIN_EXIT_CODE) and code <= MAX_EXIT_CODE,
                     "{}: exit_code must be in {}..{}".format(label, MIN_EXIT_CODE, MAX_EXIT_CODE))
            _require(code not in seen_codes, "{}: exit_code {} already used by {}".format(
                label, code, seen_codes.get(code)))
            seen_codes[code] = reason


def _normalize_component(raw, where, config_dir):
    component = copy.deepcopy(raw)
    command = component.get("command")
    _require(isinstance(command, list) and command and all(isinstance(c, str) for c in command),
             "{}.command must be a non-empty list of strings".format(where))
    component["command"] = [c.replace("{config_dir}", config_dir) for c in command]

    resources = dict(RESOURCE_DEFAULTS)
    given = component.get("resources", {})
    _check_keys(given, set(RESOURCE_DEFAULTS), where + ".resources")
    resources.update(given)
    for name in INT_RESOURCES:
        value = resources[name]
        minimum = 0 if name == "gpus_per_task" else 1
        _require(value is None or _is_int(value, minimum),
                 "{}.resources.{} must be an integer >= {}".format(where, name, minimum))
    if resources["time"] is not None:
        _check_duration(resources["time"], where + ".resources.time")
    if resources["exclusive"]:
        _require(resources["nodes"], where + ".resources: exclusive requires nodes (Flux rule)")
    component["resources"] = resources

    component.setdefault("params", {})
    component.setdefault("env", {})
    component.setdefault("max_unpredicted_retries", 2)
    component.setdefault("retry_delay_seconds", 30)
    component.setdefault("max_total_attempts", 20)
    component.setdefault("predicted_failures", {})
    component.setdefault("expected_outputs", [])
    _require(isinstance(component["params"], dict), where + ".params must be an object")
    _require(isinstance(component["env"], dict), where + ".env must be an object")
    _require(_is_int(component["max_unpredicted_retries"], 0),
             where + ".max_unpredicted_retries must be an integer >= 0")
    _require(_is_int(component["max_total_attempts"], 1),
             where + ".max_total_attempts must be an integer >= 1")
    _require(isinstance(component["retry_delay_seconds"], (int, float))
             and component["retry_delay_seconds"] >= 0,
             where + ".retry_delay_seconds must be >= 0")
    _check_policies(component["predicted_failures"], where + ".predicted_failures")
    component["exit_code_map"] = {
        policy["exit_code"]: reason
        for reason, policy in component["predicted_failures"].items() if "exit_code" in policy}
    return component


def _apply_timing(config, timing):
    """Turn runtime estimates into job limits: limit = estimate + buffer."""
    _check_keys(timing, TIMING_KEYS, "timing")
    for key in ("member_estimate", "analysis_estimate"):
        _require(key in timing, "timing.{} is required when timing is set".format(key))
    buffer_seconds = _check_duration(timing.get("buffer", 0), "timing.buffer")
    reserve = _check_duration(timing.get("allocation_reserve", 0), "timing.allocation_reserve")
    pairs = (("ensemble", "member_estimate"), ("analysis", "analysis_estimate"))
    for name, key in pairs:
        _require(config[name]["resources"]["time"] is None,
                 "{}.resources.time conflicts with timing.{}; set only one".format(name, key))
        estimate = _check_duration(timing[key], "timing." + key)
        config[name]["resources"]["time"] = format_seconds(estimate + buffer_seconds)
    config["timing"] = {
        "member_estimate": duration_seconds(timing["member_estimate"]),
        "analysis_estimate": duration_seconds(timing["analysis_estimate"]),
        "buffer": buffer_seconds,
        "allocation_reserve": reserve,
    }


def time_plan(config):
    """Work out how long the campaign should take and how long an allocation to request.

    Members queue when the allocation is smaller than the ensemble, so the ensemble
    phase takes `waves` member time limits, not one.
    """
    ensemble, analysis = config["ensemble"], config["analysis"]
    member_nodes = ensemble["resources"]["nodes"] or 1
    allocation = config["allocation"] or {}
    nodes = allocation.get("nodes") or ensemble["size"] * member_nodes
    plan = {"nodes": nodes,
            "waves": int(math.ceil(ensemble["size"] * member_nodes / float(nodes))),
            "member_limit": None, "analysis_limit": None,
            "per_cycle": None, "total": None, "request": None, "warnings": []}
    if ensemble["resources"]["time"] and analysis["resources"]["time"]:
        plan["member_limit"] = duration_seconds(ensemble["resources"]["time"])
        plan["analysis_limit"] = duration_seconds(analysis["resources"]["time"])
        plan["per_cycle"] = plan["waves"] * plan["member_limit"] + plan["analysis_limit"]
        reserve = (config.get("timing") or {}).get("allocation_reserve", 0)
        plan["total"] = config["cycles"] * plan["per_cycle"] + reserve
    explicit = allocation.get("time")
    if explicit is not None:
        plan["request"] = duration_seconds(explicit)
    elif plan["total"] is not None:
        plan["request"] = plan["total"]
        if allocation.get("max_time") is not None:
            plan["request"] = min(plan["total"], duration_seconds(allocation["max_time"]))
    if plan["total"] is not None and plan["request"] is not None \
            and plan["request"] < plan["total"]:
        plan["warnings"].append(
            "the requested allocation ({}) is shorter than the planned campaign ({}); it will "
            "pause when time runs out, and you resume it by running `batch` again".format(
                human_duration(plan["request"]), human_duration(plan["total"])))
    return plan


def load_config(path):
    path = os.path.abspath(path)
    try:
        with open(path) as handle:
            raw = json.load(handle)
    except (OSError, ValueError) as exc:
        raise ConfigError("cannot read {}: {}".format(path, exc))
    config_dir = os.path.dirname(path)

    _check_keys(raw, TOP_KEYS, "config")
    for key in ("campaign_name", "run_dir", "ensemble", "analysis"):
        _require(key in raw, "config: missing required key {!r}".format(key))
    _require(re.match(r"^[\w.-]+$", str(raw["campaign_name"])),
             "campaign_name may only contain letters, digits, _ . -")

    config = {
        "campaign_name": raw["campaign_name"],
        "run_dir": os.path.normpath(os.path.join(config_dir, raw["run_dir"])),
        "backend": raw.get("backend", "flux"),
        "cycles": raw.get("cycles", 1),
        "poll_interval_seconds": raw.get("poll_interval_seconds", 15),
        "allocation": raw.get("allocation"),
        "timing": None,
        "config_path": path,
        "config_hash": hashlib.sha256(
            json.dumps(raw, sort_keys=True).encode("utf-8")).hexdigest(),
    }
    _require(config["backend"] in ("flux", "local"), "backend must be flux or local")
    _require(_is_int(config["cycles"], 1), "cycles must be an integer >= 1")
    _require(isinstance(config["poll_interval_seconds"], (int, float))
             and config["poll_interval_seconds"] > 0, "poll_interval_seconds must be > 0")

    ensemble_raw = raw["ensemble"]
    _check_keys(ensemble_raw, COMPONENT_KEYS | ENSEMBLE_ONLY_KEYS, "ensemble")
    _check_keys(raw["analysis"], COMPONENT_KEYS, "analysis")
    _require(_is_int(ensemble_raw.get("size"), 1), "ensemble.size must be an integer >= 1")
    config["ensemble"] = _normalize_component(ensemble_raw, "ensemble", config_dir)
    config["ensemble"].setdefault("member_params", {})
    config["analysis"] = _normalize_component(raw["analysis"], "analysis", config_dir)
    if "timing" in raw:
        _apply_timing(config, raw["timing"])

    size = config["ensemble"]["size"]
    for key, value in config["ensemble"]["member_params"].items():
        _require(re.match(r"^\d+$", key) and int(key) < size,
                 "ensemble.member_params: {!r} is not a member id in 0..{}".format(key, size - 1))
        _require(isinstance(value, dict), "ensemble.member_params.{} must be an object".format(key))

    allocation = config["allocation"]
    if allocation is not None:
        _check_keys(allocation, ALLOCATION_KEYS, "allocation")
        if "nodes" in allocation:
            _require(_is_int(allocation["nodes"], 1), "allocation.nodes must be an integer >= 1")
        for key in ("time", "max_time"):
            if key in allocation:
                _check_duration(allocation[key], "allocation." + key)
        available = allocation.get("nodes") or size * (config["ensemble"]["resources"]["nodes"] or 1)
        for name in ("ensemble", "analysis"):
            nodes = config[name]["resources"]["nodes"] or 1
            _require(nodes <= available,
                     "{} needs {} node(s) per job but the allocation has {}".format(
                         name, nodes, available))
    return config
