"""Job backends: real Flux (through its CLI) and local subprocesses for laptop testing.

A status is a dict with: state, result, returncode, runtime, exception_type, exception_note.
Active jobs have result None. Jobs the scheduler does not know have state "MISSING".
"""
import os
import re
import signal
import subprocess
import time

# Exact-format fields come first and the free-text note last, so a "|" in it is harmless.
FLUX_JOBS_FORMAT = ("{id.f58}|{state}|{result}|{returncode}|{runtime}"
                    "|{exception.type}|{exception.note}")
POLL_CHUNK = 500
CLI_TIMEOUT_SECONDS = 120
# `flux job timeleft` prints UINT32_MAX when the instance has no time limit.
NO_LIMIT_SECONDS = 2 ** 31
# Flux prints this on stderr. It may exit 0 if other requested ids were found,
# so the exit code cannot be used to detect it.
UNKNOWN_JOB_RE = re.compile(r"JobID\s+(\S+)\s+unknown")
OUT_OF_TIME_TEXT = "exceeds remaining instance lifetime"


class SubmitError(Exception):
    def __init__(self, message):
        super(SubmitError, self).__init__(message)
        self.out_of_time = OUT_OF_TIME_TEXT in message


class PollError(Exception):
    """Job states could not be read. Says nothing about whether any job still exists."""


def _ascii_id(job_id):
    return job_id.strip().replace(u"ƒ", "f")


def _missing_status():
    return {"state": "MISSING", "result": None, "returncode": None, "runtime": None,
            "exception_type": "", "exception_note": ""}


def _to_int(text):
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _to_float(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


class FluxBackend(object):
    name = "flux"

    def _run(self, argv, extra_env=None):
        """Run a CLI command, returning (returncode, stdout, stderr). Never raises."""
        env = dict(os.environ)
        env["FLUX_F58_FORCE_ASCII"] = "1"
        env.update(extra_env or {})
        try:
            process = subprocess.run(argv, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, timeout=CLI_TIMEOUT_SECONDS)
        except (OSError, subprocess.SubprocessError) as exc:
            return 1, "", "{}: {}".format(argv[0], exc)
        return (process.returncode, process.stdout.decode("utf-8", "replace"),
                process.stderr.decode("utf-8", "replace"))

    @staticmethod
    def resource_args(resources):
        args = []
        if resources.get("nodes"):
            args += ["-N", str(resources["nodes"])]
        args += ["-n", str(resources.get("tasks") or 1)]
        args += ["-c", str(resources.get("cores_per_task") or 1)]
        if resources.get("gpus_per_task"):
            args += ["-g", str(resources["gpus_per_task"])]
        if resources.get("time"):
            args += ["-t", str(resources["time"])]
        if resources.get("exclusive"):
            args += ["--exclusive"]
        return args + list(resources.get("extra_args") or [])

    def submit_command(self, spec):
        return (["flux", "submit",
                 "--job-name=" + spec["name"],
                 "--cwd=" + spec["cwd"],
                 "--output=" + spec["stdout"],
                 "--error=" + spec["stderr"]]
                + self.resource_args(spec["resources"]) + spec["command"])

    def submit(self, spec):
        # `flux submit` copies its environment into the job, which delivers ORCH_*.
        code, out, err = self._run(self.submit_command(spec), spec["env"])
        lines = out.strip().splitlines()
        if code != 0 or not lines:
            raise SubmitError(err.strip() or "flux submit exited {}".format(code))
        return _ascii_id(lines[-1])

    def poll(self, job_ids):
        statuses = {}
        for start in range(0, len(job_ids), POLL_CHUNK):
            chunk = job_ids[start:start + POLL_CHUNK]
            code, out, err = self._run(["flux", "jobs", "-n", "-o", FLUX_JOBS_FORMAT] + chunk)
            statuses.update(self.parse_jobs_output(chunk, code, out, err))
        return statuses

    @staticmethod
    def parse_jobs_output(requested, code, out, err):
        statuses = {}
        wanted = set(requested)
        for line in out.splitlines():
            fields = line.split("|", 6)
            if len(fields) == 7 and _ascii_id(fields[0]) in wanted:
                statuses[_ascii_id(fields[0])] = {
                    "state": fields[1].strip(),
                    "result": fields[2].strip() or None,
                    "returncode": _to_int(fields[3].strip()),
                    "runtime": _to_float(fields[4].strip()),
                    "exception_type": fields[5].strip(),
                    "exception_note": fields[6].strip(),
                }
        unknown = set(_ascii_id(found) for found in UNKNOWN_JOB_RE.findall(err))
        for job_id in requested:
            if job_id in statuses:
                continue
            if job_id not in unknown:
                # Do not assume a job is gone just because a query failed. Doing that
                # would resubmit a running ensemble during a brief broker hiccup.
                raise PollError("no status for {} (exit {}): {}".format(job_id, code, err.strip()))
            statuses[job_id] = _missing_status()
        return statuses

    def timeleft(self):
        """Seconds until this Flux instance expires, or None if it has no limit."""
        code, out, _ = self._run(["flux", "job", "timeleft"])
        seconds = _to_float(out.strip()) if code == 0 else None
        return None if seconds is None or seconds >= NO_LIMIT_SECONDS else seconds

    def batch_command(self, allocation, seconds, name, output, command):
        args = ["flux", "batch", "-N", str(allocation["nodes"]), "-t", "{}s".format(int(seconds)),
                "--exclusive", "--job-name=" + name, "--output=" + output]
        if allocation.get("queue"):
            args += ["-q", allocation["queue"]]
        if allocation.get("bank"):
            args += ["-B", allocation["bank"]]
        return args + list(allocation.get("extra_args") or []) + ["--wrap"] + command


class LocalBackend(object):
    """Runs each job as a subprocess so that campaign logic can be tried without Flux.

    It ignores resources and time limits.
    """
    name = "local"

    def __init__(self):
        self._jobs = {}
        self._counter = 0

    def submit_command(self, spec):
        return list(spec["command"])

    def submit(self, spec):
        self._counter += 1
        job_id = "local-{}-{}".format(os.getpid(), self._counter)
        env = dict(os.environ)
        env.update(spec["env"])
        try:
            with open(spec["stdout"], "wb") as out, open(spec["stderr"], "wb") as err:
                process = subprocess.Popen(spec["command"], cwd=spec["cwd"], env=env,
                                           stdout=out, stderr=err)
        except OSError as exc:
            raise SubmitError(str(exc))
        self._jobs[job_id] = (process, time.time())
        return job_id

    def poll(self, job_ids):
        statuses = {}
        for job_id in job_ids:
            if job_id not in self._jobs:
                statuses[job_id] = _missing_status()
                continue
            process, started = self._jobs[job_id]
            code = process.poll()
            status = _missing_status()
            status["runtime"] = time.time() - started
            if code is None:
                status["state"] = "RUN"
            else:
                status["state"] = "INACTIVE"
                status["returncode"] = 128 - code if code < 0 else code
                status["result"] = "COMPLETED" if code == 0 else "FAILED"
            statuses[job_id] = status
        return statuses

    def timeleft(self):
        return None

    def terminate_all(self):
        for process, _ in self._jobs.values():
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                process.wait()


def make_backend(name):
    return {"flux": FluxBackend, "local": LocalBackend}[name]()
