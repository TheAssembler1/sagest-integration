#!/usr/bin/env python3
"""Placeholder post-analysis. It only runs once every member has succeeded.

ORCH_MANIFEST lists each member's successful attempt directory. Always use it, because
the directory depends on how many attempts that member needed.

Writing ORCH_NEXT_CYCLE_FILE is optional and steers the next ensemble:
    {"continue": true, "params": {...}, "member_params": {"3": {...}}}
"continue": false ends the campaign early.
"""
import json
import os


def main():
    with open(os.environ["ORCH_MANIFEST"]) as handle:
        manifest = json.load(handle)
    values = []
    for member in manifest["members"]:
        with open(os.path.join(member["attempt_dir"], "result.json")) as handle:
            values.append(json.load(handle)["value"])
    mean = sum(values) / len(values)
    print("cycle {}: mean of {} members = {}".format(manifest["cycle"], len(values), mean))

    with open(os.path.join(os.environ["ORCH_ATTEMPT_DIR"], "analysis.json"), "w") as handle:
        json.dump({"mean": mean, "members": len(values)}, handle)
    with open(os.environ["ORCH_NEXT_CYCLE_FILE"], "w") as handle:
        json.dump({"continue": True, "params": {"forcing": mean}}, handle)


if __name__ == "__main__":
    main()
