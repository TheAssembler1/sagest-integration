# campaign.py user guide

`campaign.py` runs an **ensemble** of N members, then a **post-analysis**, and repeats that for a
number of **cycles**. Each member and each analysis is its own Flux job. Flux schedules and
launches every job. `campaign.py` decides what to submit next: it retries failures, reacts to
failure reasons you declare, and only starts the analysis once every member has succeeded.

This guide covers every command, config setting, file and behavior. The examples come from real
runs on Flux 0.91.

**Contents**

1. [Quick start](#1-quick-start)
2. [How a campaign runs](#2-how-a-campaign-runs)
3. [The two modes: login node and allocation](#3-the-two-modes-login-node-and-allocation)
4. [Commands](#4-commands)
5. [Config reference](#5-config-reference)
6. [Time limits and allocation size](#6-time-limits-and-allocation-size)
7. [Failures and retries](#7-failures-and-retries)
8. [Writing a member script](#8-writing-a-member-script)
9. [Writing an analysis script](#9-writing-an-analysis-script)
10. [Files and folders it creates](#10-files-and-folders-it-creates)
11. [Stopping, resuming and retrying a failed campaign](#11-stopping-resuming-and-retrying-a-failed-campaign)
12. [Exit codes and messages](#12-exit-codes-and-messages)
13. [Troubleshooting](#13-troubleshooting)
14. [Limitations](#14-limitations)

---

## 1. Quick start

You need Python 3 (tested with 3.12 and 3.14) and the `flux` command; nothing else needs installing.

```bash
git clone https://github.com/TheAssembler1/sagest-integration.git
cd sagest-integration
cp configs/smoke.json configs/mine.json        # then edit: set "bank", commands, sizes
./campaign.py run configs/mine.json --dry-run  # check the exact flux commands it will use
./campaign.py run configs/mine.json            # run on a login node (inside tmux)
./campaign.py status configs/mine.json         # check progress from any shell
```

To run inside one allocation instead, use `batch` in place of `run`:

```bash
./campaign.py batch configs/mine.json
```

---

## 2. How a campaign runs

```
cycle 0:  submit member_0 … member_N-1  (one Flux job each)
          poll Flux every poll_seconds
            member failed?  -> retry it, or give up on it (section 7)
          all N members succeeded?
            yes -> write manifest.json, submit the analysis job
            no  -> stop; the campaign has failed and the analysis never runs
          analysis succeeded?
            yes -> read next_cycle.json, start cycle 1
cycle 1:  the same, with any parameters the analysis passed on
...
stop after `cycles` cycles, or earlier if the analysis says so
```

Key points:

- **The analysis dependency is enforced by `campaign.py`, not Flux.** The analysis job is not
  submitted until every member in the cycle has succeeded, however many retries that took.
  Flux's own job dependencies can't do this across retries.
- **Members keep going after one fails for good.** Their work is kept, so a later rerun only
  repeats what failed (section 11).
- **Progress is saved to `state.json` after every change**, so the campaign can always resume.

---

## 3. The two modes: login node and allocation

`run` chooses its mode from where it starts. It asks Flux for `instance-level`:

| Where you start `run` | `instance-level` | Mode |
|---|---|---|
| A login node | 0 | **login** |
| Inside `flux batch` or `flux alloc` (including via `campaign.py batch`) | 1 or more | **allocation** |
| Inside a single `flux submit` job | 0, with `FLUX_JOB_ID` set | **refused** |

The log's first line says which mode it picked, e.g. `running in login mode`.

| | Login mode | Allocation mode |
|---|---|---|
| Start with | `./campaign.py run CONFIG`, kept running in `tmux` or `screen` | `./campaign.py batch CONFIG`, or `run` inside `flux alloc` / `flux batch` |
| Where jobs go | Each job queues on its own in `queue`, charged to `bank` | Into the allocation; jobs share its nodes |
| `queue` and `bank` | Added to every job (`-q`, `-B`) | Added to the allocation only; the allocation rejects them on jobs |
| Members at the same time? | Not guaranteed: each job and retry waits in the queue separately | Yes, up to the allocation's nodes |
| Time checks | Each job's own limit | Also checks each job fits the allocation's remaining time (section 6) |
| If `campaign.py` stops | Jobs keep running in the queue. Rerun `run` to pick them up | The allocation ends and its jobs stop. Rerun `batch`; those jobs are resubmitted at no retry cost |
| Where its log goes | Your terminal | `run_dir/driver-<jobid>.out` (with `batch`) |

**Switching modes on resume** is allowed, for example finishing a paused allocation campaign
from the login node. The one exception: allocation mode refuses to start while jobs submitted
from the login node are still running, because the allocation can't see them and would submit
duplicates. Let those finish by resuming on the login node first.

---

## 4. Commands

```
./campaign.py run    CONFIG [--dry-run]
./campaign.py batch  CONFIG [--dry-run]
./campaign.py status CONFIG
```

### `run`

Starts a new campaign, or resumes the one in `run_dir` if `state.json` exists. It keeps running
until the campaign completes, fails or pauses, printing one line per event:

```
13:24:20 running in login mode
13:24:20 starting cycle 0
13:24:21 member_0 attempt 1 submitted as ffASWjq
13:24:21 member_1 attempt 1 submitted as fjtiDsd
13:24:22 member_1 attempt 1: unpredicted (FAILED rc=1) -> retry 1/2
13:24:22 member_2 attempt 1: predicted (NUMERICAL_INSTABILITY) -> retry 1/3
13:24:24 member_1 attempt 2 submitted as f2w5xhj5
13:24:24 member_0 attempt 1: success -> done
13:24:26 analysis attempt 1 submitted as f48FjCJB
13:24:27 analysis attempt 1: success -> done
13:24:33 campaign completed: finished 2 cycle(s)
```

Each result line reads `<task> attempt <n>: <kind> (<reason>) -> <decision>`. The kinds and
decisions are explained in section 7.

### `batch`

Run this from a **login node**. It works out the allocation size and length (section 6),
submits `flux batch` with `campaign.py run` inside it, prints the allocation's job ID and returns
straight away. The campaign then runs in allocation mode; its log goes to
`run_dir/driver-<jobid>.out`. `batch` refuses to run inside an allocation.

### `--dry-run`

With `run` or `batch`, prints what would be submitted, for both modes, and changes nothing:

```
4 member job(s) per cycle, 2 cycle(s)

login mode, example job commands:
  flux submit --job-name=smoke-c0-member_0-a1 --cwd=<attempt dir> --output=stdout.log --error=stderr.log -t 120s -q pbatch -B mybank python3 /…/examples/ensemble_member.py
  flux submit --job-name=smoke-c0-analysis-a1 --cwd=<attempt dir> --output=stdout.log --error=stderr.log -t 120s -q pbatch -B mybank python3 /…/examples/post_analysis.py

allocation mode, example job commands:
  flux submit --job-name=smoke-c0-member_0-a1 --cwd=<attempt dir> --output=stdout.log --error=stderr.log -t 120s python3 /…/examples/ensemble_member.py
  flux submit --job-name=smoke-c0-analysis-a1 --cwd=<attempt dir> --output=stdout.log --error=stderr.log -t 120s python3 /…/examples/post_analysis.py

batch: 2 wave(s) per cycle, campaign 14.0m, requesting 2 node(s) for 14.0m
  flux batch -N 2 -t 840s --exclusive --job-name=smoke -q pbatch -B mybank --output=/…/runs/smoke/driver-{{id}}.out --wrap /usr/bin/python3 /…/campaign.py run /…/configs/smoke.json
```

Always do a dry run after editing a config.

### `status`

Reads `state.json` and prints progress. Safe to run from any shell at any time, including while
the campaign is running.

```
completed: finished 2 cycle(s)
cycle 0: 4/4 members done, analysis DONE
  member_1 attempt 1: unpredicted (FAILED rc=1) -> retry 1/2
  member_2 attempt 1: predicted (NUMERICAL_INSTABILITY) -> retry 1/3
cycle 1: 4/4 members done, analysis DONE
member runtime: mean 0.0m, max 0.0m, estimate 1.0m
```

- The first line is the campaign status (`running`, `paused`, `completed` or `failed`) and why.
- Each cycle shows how many members are done and the analysis state.
- Every attempt that didn't succeed is listed with its reason and what was decided.
- The last line compares measured runtimes of successful members with your estimate, so you can
  tune `time.member`.

If the campaign hasn't started, it prints `not started`.

---

## 5. Config reference

A config is one JSON file. Paths in it are relative to the config file's folder. A full example
is `configs/tuolumne.json`:

```json
{
  "name": "sagest",
  "run_dir": "../runs/sagest",
  "cycles": 3,
  "poll_seconds": 60,
  "queue": "pbatch",
  "bank": "YOUR_BANK",
  "time": {"member": "45m", "analysis": "15m", "buffer": "15m", "reserve": "1h"},
  "allocation": {"nodes": 16, "max_time": "12h"},
  "ensemble": {
    "size": 16,
    "nodes": 1,
    "flux_args": ["-n4", "-c24", "-g1", "--exclusive"],
    "command": ["python3", "{config_dir}/../examples/ensemble_member.py"],
    "params": {"dt": 0.01},
    "retries": 2,
    "failures": {
      "NUMERICAL_INSTABILITY": {"exit_code": 20, "retries": 3},
      "TRANSIENT_IO": {"exit_code": 21, "retries": 3},
      "BAD_INPUT": {"exit_code": 22, "retries": 0},
      "TIMEOUT": {"retries": 1, "time_scale": 1.5}
    }
  },
  "analysis": {
    "nodes": 1,
    "flux_args": ["-c24", "--exclusive"],
    "command": ["python3", "{config_dir}/../examples/post_analysis.py"],
    "retries": 2
  }
}
```

### Top level

| Key | Required | Default | What it does |
|---|---|---|---|
| `name` | yes | | Campaign name. Used in every job name and as the allocation's job name |
| `run_dir` | yes | | Folder for all state and output, relative to the config file. Use a file system the compute nodes can see. One campaign per folder |
| `cycles` | no | `1` | How many ensemble + analysis rounds to run. The analysis can stop earlier |
| `poll_seconds` | no | `60` | How often to ask Flux for job states. Retries are submitted on the next poll |
| `queue` | no | none | Flux queue (`-q`). Login mode: on every job. Allocation mode: on the allocation only |
| `bank` | no | none | Flux bank (`-B`). Same rules as `queue`. The placeholder `YOUR_BANK` is refused in login mode |
| `time` | yes | | Runtime estimates; see below |
| `allocation` | no | `{}` | Allocation settings for `batch`; see below |
| `ensemble` | yes | | The member jobs; see below |
| `analysis` | yes | | The analysis job; see below |

### `time`

Durations are a number plus a unit: `s`, `m`, `h` or `d`, for example `90s`, `45m`, `1.5h`. A
number without a unit is an error.

| Key | Required | Default | What it does |
|---|---|---|---|
| `member` | yes | | Expected runtime of one member |
| `analysis` | yes | | Expected runtime of the analysis |
| `buffer` | no | `0s` | Added to both estimates to make each job's time limit |
| `reserve` | no | `0s` | Extra allocation time for retries. Used by `batch` only |

### `allocation` (used by `batch`)

| Key | Default | What it does |
|---|---|---|
| `nodes` | `ensemble.size × ensemble.nodes` (all members at once) | Nodes to request |
| `time` | calculated (section 6) | Allocation length. If set, used exactly as given |
| `max_time` | none | Upper limit on the calculated length. Ignored if `time` is set |
| `flux_args` | `[]` | Extra arguments for `flux batch` |

### `ensemble` and `analysis`

Both take the same keys, except `size`, which only `ensemble` has.

| Key | Required | Default | What it does |
|---|---|---|---|
| `size` | yes (ensemble only) | | Number of members. Any positive number |
| `command` | yes | | The program to run, as a list, e.g. `["python3", "run.py"]`. `{config_dir}` is replaced by the config file's folder |
| `nodes` | no | not passed | Nodes per job, passed as `-N`. Also used by `batch` to size the allocation, where missing counts as 1 |
| `flux_args` | no | `[]` | Extra arguments for `flux submit`, passed unchanged, e.g. `["-n4", "-c24", "-g1", "--exclusive"]` |
| `params` | no | `{}` | Parameters written to each job's `params.json` |
| `retries` | no | `0` | Retries allowed for unpredicted failures, per member per cycle |
| `failures` | no | `{}` | Named predicted failures; see section 7 |

### Validation

Every command checks the whole config before doing anything, and lists every problem at once,
then exits with code 2:

```
config error: config.cycels: unknown key (allowed: allocation, analysis, bank, cycles, ensemble, name, poll_seconds, queue, run_dir, time)
config error: time.member: must be a duration greater than 0, like 30m, got "30"
config error: ensemble.retires: unknown key (allowed: command, failures, flux_args, nodes, params, retries, size)
config error: ensemble.flux_args: '-t' is set by campaign.py; remove it
config error: ensemble.failures.NUMERICAL_INSTABILITY.exit_code: must be an integer >= 3 and <= 125, got 1
```

What is checked:

- **Keys:** any key not listed in this section is an error, at every level, so a typo can't be
  silently ignored. Required keys must be present.
- **Types and ranges:** whole numbers must be whole numbers (`true` is not a number), counts
  are at least 1, `retries` at least 0, `poll_seconds` and `time_scale` above 0, `command` a
  non-empty list of strings, `flux_args` a list of strings, `params` and `set` objects.
- **Names:** `name` may only use letters, digits, `_`, `.` and `-`. Failure reason names must be
  UPPER_CASE, like `BAD_INPUT`.
- **Durations:** a number plus `s`, `m`, `h` or `d`. `time.member`, `time.analysis` and the
  allocation times must be above 0.
- **Failure rules:** every rule except `TIMEOUT` needs an `exit_code` from 3 to 125, and no two
  rules in a component may share one.
- **`flux_args`:** may not contain flags `campaign.py` sets itself: `--job-name`, `--cwd`,
  `--output`, `--error`, `-t`/`--time-limit`, `-q`/`--queue` and `-B`/`--bank`; nor
  `-N`/`--nodes` when the component sets `nodes`. In `allocation.flux_args`, `-N`, `--nodes` and
  `--exclusive` are also set already.
- **`allocation`:** set `time` or `max_time`, not both.

Values inside `params` and `set` are passed to your jobs as they are, so they aren't checked.
`--dry-run` validates the config too, so run it after every edit.

---

## 6. Time limits and allocation size

**Every job's time limit** (`-t`) is:

```
job limit = estimate + buffer
```

With the example config, members get 45m + 15m = 1h, and the analysis 15m + 15m = 30m. A `TIMEOUT`
failure rule can multiply a member's limit for its retry (section 7).

**The allocation `batch` requests:**

```
waves      = ceil(size × member nodes / allocation nodes)
allocation = cycles × (waves × member limit + analysis limit) + reserve
```

`waves` is how many rounds the members need when the allocation is smaller than the ensemble.
With the example config: 16 members × 1 node on 16 nodes is 1 wave, so each cycle is
1h + 30m, and 3 cycles plus a 1h reserve is 5h30m. If `allocation.time` is set, that is used
instead. If `max_time` is lower than the result, the request is capped, and `--dry-run` notes
that you should expect to resume in another allocation.

`reserve` is the only time set aside for retries. Without it, any retry can push the campaign
past the allocation's end.

**In allocation mode,** before each submission `campaign.py` asks Flux how long the allocation
has left (`flux job timeleft`). A job whose limit doesn't fit isn't submitted. Once nothing else
is running, the campaign pauses with exit code 3 and the message
`the next job needs more time than this allocation has left`. Rerun `batch` or `run` to
continue. If Flux itself rejects a job for running past the allocation's end, the job goes back
to waiting and no retry is used.

**In login mode** only the job limit applies. Time spent waiting in the queue isn't limited or
predicted.

---

## 7. Failures and retries

### How each ended job is classified

`campaign.py` reads each job's Flux result and exit code, and applies the first rule that
matches:

| # | Condition | Kind | What happens |
|---|---|---|---|
| 1 | Flux has said the job ID is unknown on 3 polls | `lost` | Resubmitted; **no retry used** |
| 2 | Flux result `CANCELED` (e.g. `flux cancel`) | `canceled` | **Not retried**; the campaign fails at the end of the cycle |
| 3 | Exit code matches a rule's `exit_code` | `predicted` | Uses that rule (below) |
| 4 | Flux result `TIMEOUT` and a rule named `TIMEOUT` exists | `predicted` | Uses the `TIMEOUT` rule |
| 5 | Flux result `COMPLETED` and exit code 0 | `success` | Task is done |
| 6 | Anything else: crash, signal, undeclared exit code, unhandled timeout, failed submission | `unpredicted` | Uses the component's `retries` |

A job is lost when Flux no longer knows it, typically because the allocation it ran in has
ended.

### Unpredicted failures

Each member has a counter per cycle. After each unpredicted failure it goes up by one:

- counter ≤ `retries` → submitted again, unchanged. Decision: `retry 1/2`
- counter > `retries` → the member fails. Decision: `gave up after 2 retries`

So `"retries": 2` allows 3 attempts in total. Retries go out on the next poll; there's no delay.

### Predicted failures

Declare each reason under `failures`. The name is your choice; it appears in logs and `status`.

```json
"failures": {
  "NUMERICAL_INSTABILITY": {"exit_code": 20, "retries": 3, "set": {"solver": "implicit"}},
  "TRANSIENT_IO":          {"exit_code": 21, "retries": 3},
  "BAD_INPUT":             {"exit_code": 22, "retries": 0},
  "TIMEOUT":               {"retries": 1, "time_scale": 1.5}
}
```

| Key | Required | What it does |
|---|---|---|
| `exit_code` | yes, except for `TIMEOUT` | The exit code that means this reason. Must be 3 to 125 |
| `retries` | yes | Retries for this reason, counted separately from other reasons and from unpredicted failures. `0` means fatal: the member fails immediately |
| `set` | no | Parameters to set before the retry, e.g. `{"solver": "implicit"}` |
| `time_scale` | no | Multiplies this member's time limit for the retry, e.g. `1.5` |

**Why 3 to 125?** 0 is success, 1 and 2 are what most programs return for any error, and 126
and above mean shell errors or a signal (128 + the signal number).

**What a retry changes,** in order:

1. The rule's `set` values are applied to the member's parameters.
2. If the job wrote `retry_params.json` in its attempt folder, those values are applied on top.
   This lets the job choose its own new values, such as half the time step. If the file isn't
   valid JSON, the member fails with `gave up: bad retry_params.json`.
3. The time limit is multiplied by `time_scale`.

These changes last for that member for the rest of the cycle. The next cycle starts again from
the config plus anything the analysis passed on.

### The analysis

The analysis follows exactly the same rules, using `analysis.retries` and `analysis.failures`.
A successful analysis that writes an invalid `next_cycle.json` counts as an unpredicted failure.

### When a member fails for good

The other members keep running until they finish. Then the campaign stops with status `failed`
and the message `ensemble did not fully succeed; analysis was not run`. See section 11 to retry.

### Jobs with several MPI ranks

Flux reports one exit code per job: the **highest** exit status of any rank, where a rank killed
by a signal counts as 128 + the signal. In tests on Flux:

| Rank 0 | Rank 1 | Job exit code |
|---|---|---|
| exit 20 | exit 0 | 20 |
| exit 0 | exit 20 | 20 |
| exit 20 | exit 25 | 25 |
| exit 20 | segfault | 139 (an unpredicted failure) |

So any rank can report a reason, but a crash or higher code on another rank hides it. Number
your codes so the most important reason is highest. Behavior of `MPI_Abort` hasn't been tested;
check it on your system before relying on it to carry a code.

---

## 8. Writing a member script

A member is any program. Each attempt runs in its own folder and receives these environment
variables:

| Variable | Example | Meaning |
|---|---|---|
| `ORCH_MEMBER_ID` | `3` | Member number, from 0 to size − 1 |
| `ORCH_CYCLE` | `0` | Cycle number, from 0 |
| `ORCH_ATTEMPT` | `2` | Attempt number for this member in this cycle, from 1 |
| `ORCH_ENSEMBLE_SIZE` | `16` | Number of members in this cycle |
| `ORCH_DIR` | `…/cycle_0/member_3/attempt_2` | This attempt's folder, which is also the working directory |
| `ORCH_PARAMS` | `…/attempt_2/params.json` | This attempt's parameters as a JSON object |

Your job also inherits the environment `campaign.py` was started with, so load modules and
activate environments before starting it.

What a member must do:

- **Succeed:** exit 0.
- **Predicted failure:** exit with the declared code. Optionally write `retry_params.json` in
  `$ORCH_DIR` first.
- **Anything else:** no special handling; crashes count as unpredicted failures.
- **Write output into `$ORCH_DIR`.** The analysis finds it through the manifest.
- **Exit nonzero if the output is missing.** `campaign.py` treats exit 0 as success without
  checking files.

To restart from a checkpoint, the previous attempt's folder is
`$(dirname "$ORCH_DIR")/attempt_$((ORCH_ATTEMPT - 1))`.

Python example (see `examples/ensemble_member.py`):

```python
import json, os, sys

params = json.load(open(os.environ["ORCH_PARAMS"]))
member = int(os.environ["ORCH_MEMBER_ID"])

if params["dt"] > 0.005:                       # a failure we know how to fix
    with open(os.path.join(os.environ["ORCH_DIR"], "retry_params.json"), "w") as f:
        json.dump({"dt": params["dt"] / 2}, f) # ask for a smaller time step
    sys.exit(20)                               # NUMERICAL_INSTABILITY

result = run_simulation(member, params)
json.dump(result, open(os.path.join(os.environ["ORCH_DIR"], "result.json"), "w"))
```

Shell example, for a single-task job:

```bash
#!/bin/bash
dt=$(python3 -c "import json,os;print(json.load(open(os.environ['ORCH_PARAMS']))['dt'])")
./my_sim --member "$ORCH_MEMBER_ID" --dt "$dt" --out "$ORCH_DIR/out.h5"
[ -f "$ORCH_DIR/out.h5" ] || exit 1   # no output: treat as a failure
```

**MPI and multi-task jobs.** Flux runs `command` once per task, so with `"flux_args": ["-n4"]`
there are 4 copies. For an MPI program, put the program itself in `command` and its task count in
`flux_args`. Every rank gets the same variables; Flux adds `FLUX_TASK_RANK`. Have only rank 0
write `retry_params.json` and output that should exist once.

Example: member 2 failed once with `NUMERICAL_INSTABILITY` and wrote `{"dt": 0.005}`. Its
`params.json` for each attempt was:

```
cycle_0/member_2/attempt_1/params.json  {"dt": 0.01,  "sleep_seconds": 2, "demo_failures": true}
cycle_0/member_2/attempt_2/params.json  {"dt": 0.005, "sleep_seconds": 2, "demo_failures": true}
```

---

## 9. Writing an analysis script

The analysis runs once per cycle, only after every member has succeeded. It gets the same
variables as a member except `ORCH_MEMBER_ID`, plus:

| Variable | Meaning |
|---|---|
| `ORCH_MANIFEST` | Path to `cycle_<n>/manifest.json`, which lists every member's successful attempt |

**Always find member output through the manifest.** A member's folder depends on how many
attempts it took (`attempt_1`, `attempt_2`, …). The manifest gives each member's name, the folder
of its successful attempt, and the parameters it used:

```json
{
  "cycle": 0,
  "members": [
    {"name": "member_0", "dir": "/…/cycle_0/member_0/attempt_1", "params": {"dt": 0.01}},
    {"name": "member_2", "dir": "/…/cycle_0/member_2/attempt_2", "params": {"dt": 0.005}}
  ]
}
```

### Steering the next cycle

The analysis can write `next_cycle.json` in `$ORCH_DIR`:

```json
{"continue": true, "params": {"forcing": 0.0125}}
```

| Key | Default | What it does |
|---|---|---|
| `continue` | `true` | `false` ends the campaign after this cycle, with status `completed` |
| `params` | `{}` | Added to every member's parameters in all later cycles. Later values replace earlier ones |

Members' parameters in a cycle are `ensemble.params` with all values passed on so far applied on
top. The analysis's own parameters are always just `analysis.params`. The file is optional; if
it's missing, the next cycle uses the same parameters. If it isn't a valid JSON object, the
analysis attempt counts as an unpredicted failure.

See `examples/post_analysis.py`.

---

## 10. Files and folders it creates

Everything goes under `run_dir`. From a real two-cycle run (cycle 1 omitted):

```
state.json                              campaign progress; read by status and resume
driver-<jobid>.out                      the log, when started with batch
cycle_0/
  manifest.json                         written when all members succeed
  member_0/attempt_1/
    params.json                         parameters given to this attempt
    stdout.log  stderr.log              the job's output
    result.json                         whatever your job writes
  member_1/attempt_1/                   crashed (unpredicted), so retried as attempt_2
  member_1/attempt_2/
  member_2/attempt_1/
    retry_params.json                   written by the job before a predicted failure
  member_2/attempt_2/
  analysis/attempt_1/
    next_cycle.json                     written by the analysis
cycle_1/
```

Nothing is deleted or overwritten: every attempt keeps its own folder.

**Job names** in `flux jobs` are `<name>-c<cycle>-<task>-a<attempt>`, e.g.
`sagest-c0-member_1-a2` or `sagest-c0-analysis-a1`.

**`state.json`** records each task's status (`PENDING`, `RUNNING`, `DONE`, `FAILED`,
`CANCELED`), attempt number, Flux job ID, mode, parameters, time limit, retry counters, and one
history entry per attempt:

```json
{"attempt": 1, "job": "fpYXyAP", "kind": "predicted",
 "reason": "NUMERICAL_INSTABILITY", "decision": "retry 1/3", "runtime": 0.05}
```

Read it freely, but don't edit it while `campaign.py` is running.

---

## 11. Stopping, resuming and retrying a failed campaign

**Resume** is always the same command you started with. What happens depends on the saved
status:

| Saved status | Running the command again |
|---|---|
| `running` (it was killed) or `paused` | Continues where it left off. Running jobs it recorded are picked back up |
| `failed` | Retries the failed and canceled tasks of the current cycle with fresh retry budgets. Finished members are kept |
| `completed` | Prints `already completed` and exits 0 |

When retrying a failed task, its parameters and time limit are reset from the **current**
config, plus values passed on by earlier analyses. Changes made by retry rules are discarded.
So you can fix the cause in your config or code, then rerun. Attempt numbers continue from where
they were.

**Config changes on resume** take effect for anything submitted afterwards, without warning.
Changing `ensemble.size` only affects new cycles.

**To stop a campaign:**

- **Login mode:** press Ctrl-C or kill `campaign.py`. Its jobs keep running in the queue. To
  cancel them too, use `flux cancel` with their job IDs from `status` or `flux jobs`.
- **Allocation mode:** `flux cancel <allocation job id>`. Its running jobs end with it, and are
  resubmitted without using a retry when you resume.

**To start over,** use a new `run_dir`, or move the old one away.

---

## 12. Exit codes and messages

| Exit code | Meaning |
|---|---|
| `0` | Completed, or already completed |
| `1` | Failed (a task ran out of retries or was canceled), or refused to start in this place (see the messages below) |
| `2` | Invalid config: every message starts with `config error:`. Nothing was started |
| `3` | Paused: the allocation ran out of time, or Flux couldn't be reached 20 polls in a row. Rerun to resume |

With `batch`, the exit code is the `flux batch` submission's; the campaign's own exit code
becomes the allocation job's exit code.

| Message | Cause | Fix |
|---|---|---|
| `config error: <path>: unknown key (allowed: …)` | A misspelt or unsupported key | Use one of the listed keys |
| `config error: <path>: required` | A required key is missing | Add it (section 5) |
| `config error: <path>: must be …, got …` | Wrong type, range or format | Change the value as described |
| `config error: <path>: '<flag>' is set by campaign.py; remove it` | `flux_args` repeats a managed flag | Remove the flag; use the matching config key instead |
| `config error: cannot read <file>: …` | The file is missing or isn't valid JSON | Fix the path or the JSON syntax |
| `config error: set bank in the config` | `bank` is still `YOUR_BANK` | Set your bank |
| `config error: ensemble needs more nodes than allocation.nodes (N)` | A job needs more nodes than the allocation | Raise `allocation.nodes` |
| `error: start campaign.py on a login node or inside flux batch or flux alloc, not inside a single job` | Started inside a `flux submit` job | Start it on a login node, or use `batch` |
| `error: run batch from a login node` | `batch` inside an allocation | Use `run` there instead |
| `error: jobs submitted from the login node are still tracked; resume on the login node until they finish` | Switched to allocation mode with login-mode jobs running | Resume on the login node first |
| `paused: the next job needs more time than this allocation has left` | Allocation nearly out of time | Rerun `batch`, or `run` on the login node |
| `paused: cannot read job states: …` | `flux jobs` failed 20 times in a row | Check Flux is working, then rerun |
| `warning: no status for <id> …` | One `flux jobs` call failed | Nothing; it retries next poll |

---

## 13. Troubleshooting

**A member keeps failing.** Run `status` to see each attempt's reason, then read
`stderr.log` in that member's attempt folders.

**A member's predicted failure shows up as unpredicted.** Check the exit code in `status`, e.g.
`(FAILED rc=139)`. With several ranks, a crash on another rank raises the job's exit code above
yours (section 7).

**The analysis can't find member output.** Read paths from `$ORCH_MANIFEST` rather than building
them; retried members live in `attempt_2` or later.

**Jobs time out.** Compare `member runtime` in `status` with your estimate, and raise
`time.member` or `time.buffer`. Add a `TIMEOUT` rule to retry with more time automatically.

**A batch campaign paused early.** Each job must fit in the time left. Add `time.reserve`, raise
`allocation.max_time`, or set `allocation.time`, then rerun `batch`.

**A resumed campaign submitted a member again.** Its old job was unknown to Flux, usually
because its allocation ended. This uses no retry. Lost jobs are detected from Flux's wording
`JobID <id> unknown`. Check it on your system with `flux jobs -n -o '{id}' f111111`. If it's
worded differently, campaigns pause instead of resubmitting; fix the pattern in `parse_jobs` in
`campaign.py`.

---

## 14. Limitations

- **Exit 0 means success.** Output files aren't checked.
- **One estimate for all members.** There are no per-member time limits or resources.
- **Retries can change parameters and the time limit,** not nodes or tasks.
- **No delay between retries.** A failed job is resubmitted on the next poll.
- **The analysis can't ask for specific members to be rerun.** It can only change the next cycle
  or stop.
- **Login mode:** members aren't guaranteed to run at the same time, and `campaign.py` must keep
  running to submit retries and the analysis.
- **Allocation mode:** the time check covers one job at a time, so queued jobs can still run out
  of time. They're resubmitted at no retry cost on resume.
- **One `campaign.py` per `run_dir`.** Nothing prevents two, and two would submit duplicate jobs.
- **If `campaign.py` dies just after submitting a job but before recording its ID,** that job
  runs untracked, and the member is submitted again in a new folder.
