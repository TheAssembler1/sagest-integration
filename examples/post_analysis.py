#!/usr/bin/env python3
"""Placeholder analysis. Replace with your code. Runs only after every member succeeds.

Read member directories from $ORCH_MANIFEST, since they depend on how many attempts each took.
Optionally write $ORCH_DIR/next_cycle.json: {"continue": true, "params": {...}}.
"""
import json
import os

with open(os.environ["ORCH_MANIFEST"]) as handle:
    manifest = json.load(handle)
values = []
for member in manifest["members"]:
    with open(os.path.join(member["dir"], "result.json")) as handle:
        values.append(json.load(handle)["value"])
mean = sum(values) / len(values)
print(f"cycle {manifest['cycle']}: mean of {len(values)} members = {mean}")
with open(os.path.join(os.environ["ORCH_DIR"], "next_cycle.json"), "w") as handle:
    json.dump({"continue": True, "params": {"forcing": mean}}, handle)
