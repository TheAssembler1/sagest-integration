"""Run with: python3 -m unittest discover -s tests -v

Campaign tests run against local subprocesses, and again against real Flux when run
inside a Flux instance (`flux start --test-size=2 python3 -m unittest ...`).
"""
import json
import logging
import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from orchestrator import policy  # noqa: E402
from orchestrator.backends import FluxBackend, LocalBackend, PollError, SubmitError  # noqa: E402
from orchestrator.config import ConfigError, load_config, time_plan  # noqa: E402
from orchestrator.driver import (EXIT_COMPLETED, EXIT_CONFIG, EXIT_FAILED,  # noqa: E402
                                 EXIT_PAUSED, MAX_CONSECUTIVE_POLL_FAILURES, Campaign, load_state)

COMPONENT = os.path.join(ROOT, "tests", "component.py")
logging.getLogger("orchestrator").addHandler(logging.NullHandler())
logging.getLogger("orchestrator").propagate = False
IN_FLUX = bool(os.environ.get("FLUX_URI")) and shutil.which("flux") is not None


class Base(unittest.TestCase):
    def setUp(self):
        # On a shared path, because Flux jobs may run somewhere else.
        self.tmp = tempfile.mkdtemp(prefix="orch-test-", dir=os.environ.get("ORCH_TEST_DIR"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def config(self, size=2, cycles=1, ensemble=None, analysis=None, **top):
        raw = {"campaign_name": "t", "run_dir": "run", "cycles": cycles,
               "poll_interval_seconds": 0.05,
               "ensemble": {"size": size, "command": [sys.executable, COMPONENT],
                            "retry_delay_seconds": 0, "expected_outputs": []},
               "analysis": {"command": [sys.executable, COMPONENT], "retry_delay_seconds": 0}}
        raw["ensemble"].update(ensemble or {})
        raw["analysis"].update(analysis or {})
        raw.update(top)
        path = os.path.join(self.tmp, "config.json")
        with open(path, "w") as handle:
            json.dump(raw, handle)
        return load_config(path)


class CampaignCases(object):
    """Behaviour that must hold on every backend."""

    def backend(self):
        raise NotImplementedError

    def run_campaign(self, config, **kwargs):
        code = Campaign(config, self.backend(), sleep=__import__("time").sleep).run(**kwargs)
        return code, load_state(config["run_dir"])

    def script(self, member_scripts, **ensemble):
        ensemble["member_params"] = {str(k): {"script": v} for k, v in member_scripts.items()}
        return ensemble

    def test_any_ensemble_size_gates_analysis(self):
        for size in (1, 3, 16):
            with self.subTest(size=size):
                self.setUp()
                code, state = self.run_campaign(self.config(size=size))
                self.assertEqual(code, EXIT_COMPLETED)
                cycle = state["cycles"]["0"]
                self.assertEqual(len(cycle["members"]), size)
                self.assertEqual(cycle["analysis"]["attempt"], 1)
                self.assertEqual(cycle["analysis"]["status"], "SUCCEEDED")

    def test_unpredicted_crash_retries_then_succeeds(self):
        config = self.config(ensemble=self.script({1: ["crash", "exit:1", "ok"]},
                                                  max_unpredicted_retries=2))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        task = state["cycles"]["0"]["members"]["1"]
        self.assertEqual(task["attempt"], 3)
        self.assertEqual(task["unpredicted_failures"], 2)
        self.assertEqual(state["cycles"]["0"]["analysis"]["status"], "SUCCEEDED")

    def test_unpredicted_retries_exhausted_blocks_analysis(self):
        config = self.config(ensemble=self.script({0: ["exit:1"]}, max_unpredicted_retries=2))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        cycle = state["cycles"]["0"]
        self.assertEqual(cycle["members"]["0"]["attempt"], 3)  # the first run plus 2 retries
        self.assertEqual(cycle["members"]["0"]["status"], "FAILED")
        self.assertEqual(cycle["members"]["1"]["status"], "SUCCEEDED")
        self.assertIsNone(cycle["analysis"])

    def test_predicted_exit_code_reconfigures_without_using_unpredicted_budget(self):
        config = self.config(ensemble=self.script(
            {0: ["exit:20", "exit:20", "ok"]}, params={"dt": 1.0}, max_unpredicted_retries=0,
            predicted_failures={"UNSTABLE": {"exit_code": 20, "action": "reconfigure",
                                             "max_retries": 2,
                                             "changes": {"params": {"dt": {"scale": 0.5}}}}}))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        task = state["cycles"]["0"]["members"]["0"]
        self.assertEqual(task["unpredicted_failures"], 0)
        self.assertEqual(task["predicted_counts"], {"UNSTABLE": 2})
        self.assertEqual([entry["reason"] for entry in task["history"][:2]],
                         ["UNSTABLE", "UNSTABLE"])
        with open(os.path.join(task["attempt_dir"], "params.json")) as handle:
            self.assertEqual(json.load(handle)["dt"], 0.25)

    def test_failure_report_wins_over_exit_code_and_applies_suggestions(self):
        config = self.config(ensemble=self.script(
            {0: ['report:NEEDS_SMALLER_GRID:1:{"grid": 64}', "ok"]}, max_unpredicted_retries=0,
            predicted_failures={"NEEDS_SMALLER_GRID": {"action": "reconfigure",
                                                       "max_retries": 1}}))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        task = state["cycles"]["0"]["members"]["0"]
        self.assertEqual(task["history"][0]["reason"], "NEEDS_SMALLER_GRID")
        self.assertEqual(task["params"]["grid"], 64)

    def test_fatal_reason_never_retries(self):
        config = self.config(ensemble=self.script(
            {0: ["exit:22", "ok"]},
            predicted_failures={"BAD_INPUT": {"exit_code": 22, "action": "fatal"}}))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["attempt"], 1)

    def test_predicted_retries_exhausted(self):
        config = self.config(ensemble=self.script(
            {0: ["exit:21"]},
            predicted_failures={"IO": {"exit_code": 21, "action": "retry", "max_retries": 2}}))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["attempt"], 3)

    def test_undeclared_and_garbage_reports_count_as_unpredicted(self):
        config = self.config(size=2, ensemble=self.script(
            {0: ["report:NOT_DECLARED:5", "ok"], 1: ["garbage_report", "ok"]},
            max_unpredicted_retries=1))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        members = state["cycles"]["0"]["members"]
        self.assertEqual(members["0"]["history"][0]["reason"], "UNDECLARED_REASON")
        self.assertEqual(members["1"]["history"][0]["reason"], "BAD_FAILURE_REPORT")

    def test_exit_zero_without_expected_output_is_a_failure(self):
        config = self.config(size=1, ensemble=self.script(
            {0: ["nooutput", "ok"]}, expected_outputs=["result.txt"], max_unpredicted_retries=1))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        task = state["cycles"]["0"]["members"]["0"]
        self.assertEqual(task["history"][0]["reason"], "MISSING_OUTPUT")

    def test_max_total_attempts_stops_endless_mixed_failures(self):
        config = self.config(size=1, ensemble=self.script(
            {0: ["exit:21"]}, max_total_attempts=2,
            predicted_failures={"IO": {"exit_code": 21, "action": "retry", "max_retries": 50}}))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["attempt"], 2)

    def test_analysis_failure_retries_with_the_same_rules(self):
        config = self.config(analysis={
            "params": {"script": ["exit:30", "exit:1", "ok"], "x": 2},
            "max_unpredicted_retries": 1,
            "predicted_failures": {"NEEDS_X": {"exit_code": 30, "action": "reconfigure",
                                               "max_retries": 1,
                                               "changes": {"params": {"x": {"add": 1}}}}}})
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        task = state["cycles"]["0"]["analysis"]
        self.assertEqual(task["attempt"], 3)
        self.assertEqual(task["params"]["x"], 3)
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["attempt"], 1)

    def test_analysis_steers_next_cycle(self):
        config = self.config(size=2, cycles=3, analysis={"params": {"next_by_cycle": {
            "0": {"params": {"forcing": 7}, "member_params": {"1": {"only_one": True}}},
            "1": {"continue": False}}}})
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        self.assertEqual(sorted(state["cycles"]), ["0", "1"])  # stopped before cycle 2
        members = state["cycles"]["1"]["members"]
        self.assertEqual(members["0"]["params"], {"forcing": 7})
        self.assertEqual(members["1"]["params"], {"forcing": 7, "only_one": True})
        self.assertNotIn("forcing", state["cycles"]["0"]["members"]["0"]["params"])

    def test_malformed_next_cycle_fails_the_analysis_attempt(self):
        config = self.config(size=1, analysis={
            "params": {"next_by_cycle": {"0": "{oops"}}, "max_unpredicted_retries": 1})
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        self.assertEqual(state["cycles"]["0"]["analysis"]["history"][0]["reason"],
                         "BAD_NEXT_CYCLE")

    def test_retry_failed_reuses_finished_members(self):
        config = self.config(ensemble=self.script({0: ["exit:1", "exit:1", "ok"]},
                                                  max_unpredicted_retries=0))
        code, _ = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        self.assertEqual(self.run_campaign(config)[0], EXIT_FAILED)  # refuses without the flag
        code, _ = self.run_campaign(config, retry_failed=True)
        self.assertEqual(code, EXIT_FAILED)  # attempt 2 also fails, with a fresh budget
        code, state = self.run_campaign(config, retry_failed=True)
        self.assertEqual(code, EXIT_COMPLETED)
        members = state["cycles"]["0"]["members"]
        self.assertEqual(members["0"]["attempt"], 3)
        self.assertEqual(members["1"]["attempt"], 1)

    def test_changed_config_needs_explicit_permission(self):
        config = self.config(size=1)
        self.assertEqual(self.run_campaign(config)[0], EXIT_COMPLETED)
        changed = self.config(size=1, poll_interval_seconds=0.1)
        self.assertEqual(self.run_campaign(changed)[0], EXIT_CONFIG)
        self.assertEqual(self.run_campaign(changed, allow_config_change=True)[0], EXIT_COMPLETED)


class LocalCampaignTests(CampaignCases, Base):
    def backend(self):
        backend = LocalBackend()
        self.addCleanup(backend.terminate_all)
        return backend


@unittest.skipUnless(IN_FLUX, "not inside a Flux instance")
class FluxCampaignTests(CampaignCases, Base):
    def backend(self):
        return FluxBackend()

    def test_flux_timeout_reconfigures_time_limit(self):
        config = self.config(size=1, ensemble=self.script(
            {0: ["sleep:30", "ok"]}, resources={"time": "2s"}, max_unpredicted_retries=0,
            predicted_failures={"TIMEOUT": {"action": "reconfigure", "max_retries": 1,
                                            "changes": {"resources": {"time": {"scale": 10}}}}}))
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_COMPLETED)
        task = state["cycles"]["0"]["members"]["0"]
        self.assertEqual(task["history"][0]["flux_result"], "TIMEOUT")
        self.assertEqual(task["history"][0]["reason"], "TIMEOUT")
        self.assertEqual(task["resources"]["time"], "20s")

    def test_job_too_large_for_the_instance_fails_cleanly(self):
        config = self.config(size=1, ensemble={"resources": {"nodes": 500},
                                               "max_unpredicted_retries": 1})
        code, state = self.run_campaign(config)
        self.assertEqual(code, EXIT_FAILED)
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["attempt"], 2)
        self.assertIsNone(state["cycles"]["0"]["analysis"])


class ResumeTests(Base):
    def test_lost_jobs_are_resubmitted_at_no_charge(self):
        config = self.config(size=2, ensemble={"max_unpredicted_retries": 0,
                                               "params": {"script": ["sleep:5", "ok"]}})
        first_backend = LocalBackend()
        self.addCleanup(first_backend.terminate_all)
        first = Campaign(config, first_backend)
        first.stop_requested = True
        original = first._submit_ready
        first._submit_ready = lambda tasks: (setattr(first, "stop_requested", False),
                                             original(tasks),
                                             setattr(first, "stop_requested", True))[1]
        self.assertEqual(first.run(), EXIT_PAUSED)
        state = load_state(config["run_dir"])
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["status"], "ACTIVE")

        backend = LocalBackend()  # a new "allocation" that has never seen those job ids
        self.addCleanup(backend.terminate_all)
        self.assertEqual(Campaign(config, backend).run(), EXIT_COMPLETED)
        task = load_state(config["run_dir"])["cycles"]["0"]["members"]["0"]
        self.assertEqual(task["attempt"], 2)
        self.assertEqual(task["unpredicted_failures"], 0)
        self.assertEqual(task["history"][0]["kind"], policy.LOST)


class TimeBudgetTests(Base):
    class LimitedBackend(LocalBackend):
        def __init__(self, seconds):
            super(TimeBudgetTests.LimitedBackend, self).__init__()
            self.seconds = seconds
            self.submitted = 0

        def timeleft(self):
            return self.seconds

        def submit(self, spec):
            self.submitted += 1
            return super(TimeBudgetTests.LimitedBackend, self).submit(spec)

    TIMING = {"member_estimate": "8m", "analysis_estimate": "3m", "buffer": "2m"}

    def test_job_limit_is_estimate_plus_buffer(self):
        config = self.config(timing=self.TIMING)
        self.assertEqual(config["ensemble"]["resources"]["time"], "600s")
        self.assertEqual(config["analysis"]["resources"]["time"], "300s")

    def test_allocation_time_includes_waves_cycles_and_reserve(self):
        timing = dict(self.TIMING, allocation_reserve="10m")
        config = self.config(size=16, cycles=3, timing=timing, allocation={"nodes": 8},
                             ensemble={"resources": {"nodes": 1}})
        plan = time_plan(config)
        self.assertEqual(plan["waves"], 2)
        self.assertEqual(plan["per_cycle"], 2 * 600 + 300)
        self.assertEqual(plan["total"], 3 * 1500 + 600)
        self.assertEqual(plan["request"], plan["total"])

    def test_allocation_defaults_to_one_slot_per_member(self):
        config = self.config(size=16, timing=self.TIMING, ensemble={"resources": {"nodes": 2}})
        plan = time_plan(config)
        self.assertEqual((plan["nodes"], plan["waves"]), (32, 1))

    def test_max_time_caps_the_request_and_warns(self):
        config = self.config(size=1, cycles=100, timing=self.TIMING,
                             allocation={"nodes": 1, "max_time": "1h"})
        plan = time_plan(config)
        self.assertEqual(plan["request"], 3600)
        self.assertEqual(len(plan["warnings"]), 1)

    def test_short_explicit_allocation_warns(self):
        config = self.config(size=1, timing=self.TIMING, allocation={"nodes": 1, "time": "1m"})
        self.assertEqual(time_plan(config)["request"], 60)
        self.assertEqual(len(time_plan(config)["warnings"]), 1)

    def test_estimate_and_explicit_job_limit_conflict(self):
        with self.assertRaises(ConfigError):
            self.config(timing=self.TIMING, ensemble={"resources": {"time": "1h"}})

    def test_too_little_time_pauses_without_submitting_then_resumes(self):
        config = self.config(size=2, timing=self.TIMING)
        short = self.LimitedBackend(599)  # just under the 600 s member limit
        self.assertEqual(Campaign(config, short).run(), EXIT_PAUSED)
        self.assertEqual(short.submitted, 0)
        state = load_state(config["run_dir"])
        self.assertEqual(state["status"], "paused")
        self.assertEqual(state["cycles"]["0"]["members"]["0"]["attempt"], 0)
        longer = self.LimitedBackend(10000)
        self.addCleanup(longer.terminate_all)
        self.assertEqual(Campaign(config, longer).run(), EXIT_COMPLETED)
        self.assertEqual(longer.submitted, 3)

    def test_analysis_is_also_time_checked_after_the_ensemble(self):
        timing = {"member_estimate": "1m", "analysis_estimate": "10m", "buffer": "0s"}
        config = self.config(size=1, timing=timing)
        backend = self.LimitedBackend(300)
        self.addCleanup(backend.terminate_all)
        self.assertEqual(Campaign(config, backend).run(), EXIT_PAUSED)
        cycle = load_state(config["run_dir"])["cycles"]["0"]
        self.assertEqual(cycle["members"]["0"]["status"], "SUCCEEDED")
        self.assertEqual(cycle["analysis"]["attempt"], 0)


class ClassifyTests(Base):
    def component(self, **kwargs):
        policies = {"X": {"exit_code": 20, "action": "retry", "max_retries": 1}}
        policies.update(kwargs.pop("policies", {}))
        return self.config(ensemble=dict(predicted_failures=policies, **kwargs))["ensemble"]

    def status(self, result, code):
        return {"state": "INACTIVE", "result": result, "returncode": code}

    def test_canceled_is_terminal_even_with_a_declared_exit_code(self):
        outcome = policy.classify(self.component(), self.status("CANCELED", 20), self.tmp)
        self.assertEqual(outcome["kind"], policy.CANCELED)

    def test_timeout_without_policy_is_unpredicted(self):
        outcome = policy.classify(self.component(), self.status("TIMEOUT", 142), self.tmp)
        self.assertEqual((outcome["kind"], outcome["reason"]), (policy.UNPREDICTED, "TIMEOUT"))

    def test_segfault_is_unpredicted(self):
        outcome = policy.classify(self.component(), self.status("FAILED", 139), self.tmp)
        self.assertEqual(outcome["kind"], policy.UNPREDICTED)

    def test_bad_changes_raise_rather_than_corrupting_the_task(self):
        task = {"params": {"a": "text"}, "resources": {"time": None}}
        with self.assertRaises(policy.ReconfigureError):
            policy.apply_changes(task, {"params": {"a": {"scale": 2}}}, None)
        with self.assertRaises(policy.ReconfigureError):
            policy.apply_changes(task, {"params": {"absent": {"add": 1}}}, None)
        with self.assertRaises(policy.ReconfigureError):
            policy.apply_changes(task, {"resources": {"time": {"scale": 2}}}, None)
        self.assertEqual(task["params"], {"a": "text"})

    def test_resource_counts_round_up_and_time_scales(self):
        task = {"params": {}, "resources": {"nodes": 3, "time": "10m"}}
        policy.apply_changes(task, {"resources": {"nodes": {"scale": 1.5},
                                                  "time": {"scale": 2}}}, None)
        self.assertEqual(task["resources"], {"nodes": 5, "time": "1200s"})


class FluxParsingTests(unittest.TestCase):
    """Fixtures are copied from flux-core 0.91 output."""
    IDS = ["fVks52w", "fb2GX5y", "fg5p4tF", "fkv1j9R", "fqijQ8F", "fvVy5pj"]
    OUTPUT = (
        "fVks52w|INACTIVE|COMPLETED|0|0.03||\n"
        "fb2GX5y|INACTIVE|FAILED|20|0.03||\n"
        "fg5p4tF|INACTIVE|FAILED|139|0.64||\n"
        "fkv1j9R|INACTIVE|TIMEOUT|142|0.99|timeout|resource allocation expired\n"
        "fqijQ8F|INACTIVE|CANCELED|143|0.33|cancel|\n"
        "fvVy5pj|INACTIVE|FAILED|20|30.1|exec|sh: rank 0 a|b exit-timeout=30s has expired\n")

    def test_parses_every_result_kind(self):
        statuses = FluxBackend.parse_jobs_output(self.IDS, 0, self.OUTPUT, "")
        self.assertEqual(statuses["fb2GX5y"]["returncode"], 20)
        self.assertEqual(statuses["fkv1j9R"]["result"], "TIMEOUT")
        self.assertIn("a|b", statuses["fvVy5pj"]["exception_note"])

    def test_active_job_has_no_result(self):
        status = FluxBackend.parse_jobs_output(["fA"], 0, "fA|RUN|||1.0||\n", "")["fA"]
        self.assertEqual((status["state"], status["result"], status["returncode"]),
                         ("RUN", None, None))

    def test_header_and_unicode_ids_are_handled(self):
        out = "JOBID|STATE|RESULT|RC|RUNTIME|TYPE|NOTE\nƒA|INACTIVE|COMPLETED|0|1||\n"
        self.assertIn("fA", FluxBackend.parse_jobs_output(["fA"], 0, out, ""))

    def test_unknown_job_is_seen_even_when_flux_exits_zero(self):
        statuses = FluxBackend.parse_jobs_output(
            ["fb2GX5y", "f111111"], 0, "fb2GX5y|INACTIVE|COMPLETED|0|1||\n",
            "JobID f111111 unknown\n")
        self.assertEqual(statuses["f111111"]["state"], "MISSING")
        self.assertEqual(statuses["fb2GX5y"]["state"], "INACTIVE")

    def test_general_error_never_reads_as_job_loss(self):
        with self.assertRaises(PollError):
            FluxBackend.parse_jobs_output(["fA"], 1, "", "Unable to connect to Flux\n")

    def test_unlimited_timeleft_is_none(self):
        backend = FluxBackend()
        backend._run = lambda argv, extra_env=None: (0, "4294967295\n", "")
        self.assertIsNone(backend.timeleft())
        backend._run = lambda argv, extra_env=None: (0, "39\n", "")
        self.assertEqual(backend.timeleft(), 39)
        backend._run = lambda argv, extra_env=None: (1, "", "not in a job")
        self.assertIsNone(backend.timeleft())

    def test_lifetime_rejection_is_recognized(self):
        message = "[Errno 22] job duration (10m) exceeds remaining instance lifetime (39s)"
        self.assertTrue(SubmitError(message).out_of_time)
        self.assertFalse(SubmitError("[Errno 22] 'pbatch' is not a valid queue").out_of_time)

    def test_submit_command_shape(self):
        command = FluxBackend().submit_command({
            "name": "n", "cwd": "/w", "stdout": "/w/o", "stderr": "/w/e", "command": ["a", "b"],
            "resources": {"nodes": 1, "tasks": 4, "cores_per_task": 24, "gpus_per_task": 1,
                          "time": "600s", "exclusive": True, "extra_args": ["--urgency=20"]}})
        self.assertEqual(command, ["flux", "submit", "--job-name=n", "--cwd=/w", "--output=/w/o",
                                   "--error=/w/e", "-N", "1", "-n", "4", "-c", "24", "-g", "1",
                                   "-t", "600s", "--exclusive", "--urgency=20", "a", "b"])


class PollFailureTests(Base):
    class FlakyBackend(LocalBackend):
        failures = 0

        def poll(self, job_ids):
            if self.failures > 0:
                self.failures -= 1
                raise PollError("broker busy")
            return super(PollFailureTests.FlakyBackend, self).poll(job_ids)

    def test_transient_poll_errors_do_not_resubmit_or_charge(self):
        config = self.config(size=2, ensemble={"max_unpredicted_retries": 0})
        backend = self.FlakyBackend()
        backend.failures = 5
        self.addCleanup(backend.terminate_all)
        self.assertEqual(Campaign(config, backend).run(), EXIT_COMPLETED)
        self.assertEqual(load_state(config["run_dir"])["cycles"]["0"]["members"]["0"]["attempt"], 1)

    def test_sustained_poll_failure_pauses_instead_of_guessing(self):
        config = self.config(size=2)
        backend = self.FlakyBackend()
        backend.failures = MAX_CONSECUTIVE_POLL_FAILURES
        self.addCleanup(backend.terminate_all)
        self.assertEqual(Campaign(config, backend).run(), EXIT_PAUSED)
        task = load_state(config["run_dir"])["cycles"]["0"]["members"]["0"]
        self.assertEqual(task["status"], "ACTIVE")
        self.assertEqual(task["attempt"], 1)


class ConfigValidationTests(Base):
    def assert_rejected(self, **kwargs):
        with self.assertRaises(ConfigError):
            self.config(**kwargs)

    def test_rejections(self):
        self.assert_rejected(ensemble={"size": 0})
        self.assert_rejected(ensemble={"sise": 3})
        self.assert_rejected(cycles=0)
        self.assert_rejected(ensemble={"resources": {"queue": "pbatch"}})
        self.assert_rejected(ensemble={"resources": {"exclusive": True}})
        self.assert_rejected(ensemble={"member_params": {"9": {}}})
        self.assert_rejected(allocation={"nodes": 1, "bad": 1})
        self.assert_rejected(allocation={"nodes": 1}, ensemble={"resources": {"nodes": 2}})
        self.assert_rejected(timing={"member_estimate": "1m"})
        self.assert_rejected(timing={"member_estimate": "soon", "analysis_estimate": "1m"})

    def test_policy_rejections(self):
        for bad in ({"lower": {"action": "fatal"}},
                    {"A": {"action": "explode"}},
                    {"A": {"action": "retry"}},
                    {"A": {"action": "fatal", "exit_code": 1}},
                    {"A": {"action": "fatal", "exit_code": 139}},
                    {"A": {"action": "fatal", "exit_code": 20},
                     "B": {"action": "fatal", "exit_code": 20}},
                    {"A": {"action": "retry", "max_retries": 1,
                           "changes": {"params": {"dt": {"scale": 0.5}}}}},
                    {"A": {"action": "reconfigure", "max_retries": 1,
                           "changes": {"params": {"dt": {"halve": 1}}}}},
                    {"A": {"action": "reconfigure", "max_retries": 1,
                           "changes": {"resources": {"time": {"add": 1}}}}}):
            with self.subTest(policy=bad):
                self.assert_rejected(ensemble={"predicted_failures": bad})

    def test_shipped_configs_load(self):
        for name in ("tuolumne.json", "smoke.json"):
            config = load_config(os.path.join(ROOT, "configs", name))
            self.assertGreaterEqual(config["ensemble"]["size"], 1)


if __name__ == "__main__":
    unittest.main()
