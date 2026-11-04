# Ensemble campaign orchestrator

Runs an ensemble of N members, one Flux job each, then a post-analysis job, then the next
ensemble. Failed members are retried or reconfigured, and the analysis starts only after
all N succeed. It uses only the Python standard library.

```
flux batch allocation
└── driver (bin/orchestrate run), with all progress saved in run_dir/state.json
    ├── cycle 0: member 0..N-1, each with its own retries  ──all succeeded──▶  analysis
    ├── cycle 1: same, using anything analysis passed on in next_cycle.json
    └── ...
```

The driver enforces the analysis dependency itself instead of using Flux `afterok`. A
retried member gets a new job ID, and if an `afterok` target fails, Flux raises a fatal
exception on the dependent job, so `afterok` cannot span retries.

## Usage

```bash
bin/orchestrate validate configs/tuolumne.json   # check the config, show the time plan and commands
bin/orchestrate batch    configs/tuolumne.json   # submit the driver with flux batch
bin/orchestrate status   configs/tuolumne.json   # add --all to list every member
bin/orchestrate run configs/smoke.json --backend local   # try it with no Flux at all
```

To resume after a pause, a dead allocation or a time limit, run the same `batch` command again.

Driver exit codes:

| Code | Meaning |
|---|---|
| 0 | Completed |
| 1 | Failed: a task ran out of retries. Fix the cause and rerun with `--retry-failed` |
| 2 | Config error, or the config changed (accept with `--allow-config-change`) |
| 3 | Paused: out of time, stopped by a signal, or Flux unreachable. Rerun to resume |

`--retry-failed` reruns only the failed tasks in the current cycle, with a fresh retry
budget and that task's settings from the current config. Finished members are kept.

## Plugging in your components

Set `ensemble.command` and `analysis.command` in the config. `examples/` has a template for
each. A job runs in its own attempt directory and receives:

| Variable | Set for | Meaning |
|---|---|---|
| `ORCH_PARAMS_FILE` | all | JSON parameters for this attempt |
| `ORCH_ATTEMPT_DIR`, `ORCH_ATTEMPT`, `ORCH_CYCLE` | all | Where to write, and which run this is |
| `ORCH_FAILURE_FILE` | all | Where to write a predicted-failure report |
| `ORCH_ENSEMBLE_SIZE` | all | Number of members in this cycle |
| `ORCH_PREV_ATTEMPT_DIR` | retries | The previous attempt, for restarting from a checkpoint |
| `ORCH_PREV_ANALYSIS_DIR` | cycle > 0 | The last cycle's analysis output |
| `ORCH_MEMBER_ID` | members | 0..N-1 |
| `ORCH_MANIFEST` | analysis | Each member's successful directory. Always read paths from here |
| `ORCH_NEXT_CYCLE_FILE` | analysis | Optional output that steers the next cycle |

Commands may use `{config_dir}`, `{attempt_dir}`, `{cycle}`, `{attempt}` and `{member_id}`.

To steer the next cycle, the analysis writes:
`{"continue": true, "params": {...}, "member_params": {"3": {...}}}`. `"continue": false`
ends the campaign early. These values carry into all later cycles.

## How failures are handled

Each finished attempt is classified by the first rule that matches:

| # | Condition | Result | Consequence |
|---|---|---|---|
| 1 | Flux doesn't know the job (its allocation ended) | lost | Resubmit, no retry charged |
| 2 | Flux result is `CANCELED` | canceled | Never retried |
| 3 | `failure.json` names a declared reason | predicted | Follow that reason's policy |
| 4 | Exit code is declared by a reason | predicted | Follow that reason's policy |
| 5 | Flux result is `TIMEOUT` and a `TIMEOUT` policy exists | predicted | Follow that policy |
| 6 | Exit 0 and all `expected_outputs` exist | success | |
| 7 | Anything else, including undeclared reasons | unpredicted | Use `max_unpredicted_retries` |

A component reports a predicted failure in one of two ways:

- **Exit code only:** `exit 20`. Declared codes must be in 3..125, because 126 and up are
  shell errors and signals.
- **Report file:** write `$ORCH_FAILURE_FILE` and then exit. This can also carry a message
  and new parameters, and it overrides the exit code:
  `{"reason": "NUMERICAL_INSTABILITY", "message": "...", "suggested_params": {"dt": 0.001}}`

Declare the reasons in `predicted_failures`, for either component:

```json
"NUMERICAL_INSTABILITY": {
  "exit_code": 20,
  "action": "reconfigure",
  "max_retries": 3,
  "changes": {"params": {"dt": {"scale": 0.5}}}
}
```

| Action | Effect |
|---|---|
| `retry` | Rerun unchanged, up to `max_retries` |
| `reconfigure` | Apply `changes`, then `suggested_params`, and rerun, up to `max_retries` |
| `fatal` | Stop retrying this task |

`changes` can use `set`, `scale` or `add` on `params` and `resources`. `time` takes only
`set` or `scale`.

Retry budgets are per task and per cycle. Each reason has its own count, which is separate
from `max_unpredicted_retries`. `max_total_attempts` (default 20) is a hard cap on all of
them. A reconfigured value applies to that member for the rest of the cycle, and the next
cycle starts again from the config plus anything the analysis passed on. To keep a change,
have the analysis pass it on.

If a task runs out of retries, the other members still finish so their work can be reused,
and the analysis does not run.

## Time budget

Give the expected runtimes and the buffer, and the job and allocation limits are calculated:

```json
"timing": {
  "member_estimate": "45m",
  "analysis_estimate": "15m",
  "buffer": "15m",
  "allocation_reserve": "1h"
},
"allocation": {"nodes": 16, "max_time": "12h", "queue": "pbatch", "bank": "YOUR_BANK"}
```

```
member limit   = member_estimate + buffer
analysis limit = analysis_estimate + buffer
waves          = ceil(size × nodes per member / allocation nodes)
each cycle     = waves × member limit + analysis limit
campaign       = cycles × each cycle + allocation_reserve
```

- **`buffer`** is added to each job's limit.
- **`allocation_reserve`** is added once, as room for retries. Without it, the plan has no
  time for reruns.
- **`allocation.nodes`** defaults to enough for all members at once.
- **`allocation.time`**, if set, replaces the calculated time. You get a warning if it is
  shorter than the plan.
- **`allocation.max_time`** caps the calculated time. A campaign longer than the cap pauses
  and continues in the next allocation.

Setting both `timing` and a component's `resources.time` is an error.

The driver also checks each job's limit against the allocation's remaining time before
submitting it. If the job can't fit, it isn't submitted, and once nothing else is running
the driver pauses with exit code 3. This covers one job at a time. Queued jobs, such as
later waves, can still run out of time; they are then resubmitted at no charge on resume.

`status` prints measured member runtimes next to the estimate, so you can tune it.

## Notes on Tuolumne

- Put `queue` and `bank` only under `allocation`. Jobs inside an allocation go to its own
  Flux instance, which rejects `-q pbatch`, so `resources` does not accept `queue`.
- [LC's page for Tuolumne](https://hpc.llnl.gov/hardware/compute-platforms/tuolumne) prohibits
  batch jobs and job chaining in `pdebug`, so use `pbatch` for real campaigns. Check the
  current limits with `joblimits`.
- With several MPI ranks, only rank 0 should write the failure report. After the first rank
  exits, Flux waits for `exit-timeout` (30 s in testing) before ending the job.

## Testing

```bash
python3 -m unittest discover -s tests                # local; the real-Flux tests are skipped
flux start --test-size=2 python3 -m unittest discover -s tests   # also runs against real Flux
```

**Smoke test on Tuolumne**, using a short interactive debug allocation:

```bash
flux alloc -N2 -q pdebug -t 30m
python3 -m unittest discover -s tests
bin/orchestrate run configs/smoke.json
bin/orchestrate status configs/smoke.json
flux jobs -n -o '{id}' f111111     # must print: JobID f111111 unknown
```

Run the last command because the driver matches that exact wording to detect lost jobs. If
your Flux version words it differently, resumed campaigns pause with exit code 3 instead of
resubmitting anything. The fix is `UNKNOWN_JOB_RE` in `orchestrator/backends.py`.

## Known limitations

- Don't run two drivers on the same `run_dir`. Nothing prevents it, and it would submit
  duplicate jobs.
- If the driver dies in the moment between submitting a job and recording its ID, that job
  keeps running untracked. It can't overwrite anything, because the rerun uses a new
  attempt directory.
- A job that finished just as its allocation died counts as lost and runs again.
- `ensemble.size` applies from the next cycle if changed during a run.
