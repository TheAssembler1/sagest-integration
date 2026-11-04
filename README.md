# Minimal Flux ensemble wrapper

`campaign.py` runs N ensemble members (one Flux job each), then a post-analysis job once all
N have succeeded, and repeats for a set number of cycles.

It runs **outside Flux**, such as on a login node in `tmux`, and refuses to start inside a Flux
job or allocation. Every member and analysis is its own job in the queue. Flux does all the
scheduling and launching. This adds what Flux has no feature for: retries, reacting to failure
reasons, and waiting for all members across retries.

Flux's own dependencies can't span retries: an `afterok` job is rejected as soon as attempt 1
fails, and `afterany` runs even if every attempt failed.

```bash
./campaign.py run configs/tuolumne.json --dry-run   # print example flux commands
./campaign.py run configs/tuolumne.json             # run or resume; keep this running
./campaign.py status configs/tuolumne.json          # from any shell
```

Exit codes: 0 completed, 1 failed, 3 paused. If `campaign.py` stops, its jobs keep running in
Flux, and running the same command again picks them back up. Rerunning a failed campaign retries
its failed tasks with fresh budgets. Finished members are kept.

Put `run_dir` on a file system that compute nodes can also see.

## Config

See `configs/tuolumne.json`. Top-level `flux_args` go on every job, which suits the queue and
bank. Each component's `flux_args` add its resources. All of them go straight to `flux submit`.

## Time limits

```
job limit = estimate + buffer
```

Set `time.member`, `time.analysis` and `time.buffer`. Time spent waiting in the queue isn't
limited or predicted. `status` shows measured runtimes next to your estimate.

## Failures

| How the job ended | Result |
|---|---|
| Flux result `CANCELED` | Not retried |
| Exit code declared under `failures` | Predicted: uses that reason's `retries` |
| Flux result `TIMEOUT`, with a `TIMEOUT` reason declared | Predicted |
| Exit 0 | Success |
| Anything else, such as a crash | Unpredicted: uses the component's `retries` |
| Job unknown to Flux | Resubmitted, no retry used |

Set `retries` on `ensemble` and on `analysis` for unpredicted failures. It defaults to 0.

```json
"NUMERICAL_INSTABILITY": {"exit_code": 20, "retries": 3, "set": {"solver": "safe"}},
"BAD_INPUT":             {"exit_code": 22, "retries": 0},
"TIMEOUT":               {"retries": 1, "time_scale": 1.5}
```

- `retries: 0` means fatal.
- `set` changes parameters for the retry, and `time_scale` multiplies the time limit.
- Before exiting, a job can write `$ORCH_DIR/retry_params.json` with new parameter values, such
  as a halved `dt`.
- Exit codes must be in 3..125.

Changed values last for that cycle only. To keep one, have the analysis pass it on.

## Your components

Each job runs in its own attempt directory and gets `ORCH_DIR`, `ORCH_PARAMS` (JSON file),
`ORCH_CYCLE`, `ORCH_ATTEMPT` and `ORCH_ENSEMBLE_SIZE`. Members also get `ORCH_MEMBER_ID`. The
analysis also gets `ORCH_MANIFEST`, which lists each member's successful directory.

The analysis can write `$ORCH_DIR/next_cycle.json`:
`{"continue": true, "params": {...}}`. Those values carry into later cycles, and
`"continue": false` ends the campaign early.

## Testing

```bash
flux start --test-size=2 python3 -m unittest discover -s tests
```

On Tuolumne, set your bank in `configs/smoke.json`, then from a login node, outside any
allocation:

```bash
flux getattr instance-level        # must print 0, or campaign.py refuses to start
flux jobs -n -o '{id}' f111111     # must print: JobID f111111 unknown
./campaign.py run configs/smoke.json
./campaign.py status configs/smoke.json
```

Lost jobs are detected from that exact wording. If yours differs, a campaign with a lost job
pauses instead of resubmitting; fix the pattern in `parse_jobs`.

`configs/smoke.json` uses `pbatch` because LC's Tuolumne page prohibits batch jobs and job chaining
in `pdebug`. Check LC's current rules on job counts and long-running login-node processes before
a large campaign.

## Limitations

- Each job and retry queues separately, so members may not run at the same time, and the analysis
  waits in the queue too.
- `campaign.py` must keep running for new jobs to be submitted. If it stops, restart it by hand.
- Exit 0 counts as success, so a member that exits 0 without its output only shows up when the
  analysis fails. Have members exit nonzero if their output is missing.
- Don't run two copies on one `run_dir`.
- If `campaign.py` dies in the moment between a submit and recording the job ID, that job keeps
  running untracked, and its member is submitted again in a new directory.
