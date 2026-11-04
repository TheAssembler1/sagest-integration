"""Run inside Flux: flux start --test-size=2 python3 -m unittest discover -s tests"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import campaign  # noqa: E402

COMPONENT = [sys.executable, os.path.join(ROOT, "tests", "component.py")]
IN_FLUX = bool(os.environ.get("FLUX_URI")) and shutil.which("flux")


class ParsingTests(unittest.TestCase):
    """Fixtures are real flux-core 0.91 output."""

    def test_finished_and_running_jobs(self):
        jobs = campaign.parse_jobs(
            ["fA", "fB", "fC"], 0,
            "fA|COMPLETED|0|1.5|\nfB|TIMEOUT|142|0.9|resource allocation expired\nfC|||1.0|\n", "")
        self.assertEqual((jobs["fA"]["result"], jobs["fA"]["rc"]), ("COMPLETED", 0))
        self.assertEqual(jobs["fB"]["note"], "resource allocation expired")
        self.assertIsNone(jobs["fC"]["result"])

    def test_unknown_job_detected_even_when_flux_exits_zero(self):
        jobs = campaign.parse_jobs(["fA", "f111"], 0, "fA|COMPLETED|0|1|\n", "JobID f111 unknown\n")
        self.assertIsNone(jobs["f111"])

    def test_query_failure_is_not_job_loss(self):
        with self.assertRaises(campaign.PollError):
            campaign.parse_jobs(["fA"], 1, "", "Unable to connect to Flux\n")

    def test_job_limit_is_estimate_plus_buffer(self):
        cfg = {"time": {"member": "45m", "analysis": "15m", "buffer": "15m"}}
        self.assertEqual(campaign.time_limit(cfg, "ensemble"), 3600)
        self.assertEqual(campaign.time_limit(cfg, "analysis"), 1800)

    def test_common_and_component_flux_args_reach_submit(self):
        cfg = {"flux_args": ["-q", "pbatch"],
               "ensemble": {"flux_args": ["-N1"], "command": ["a"]}}
        args = campaign.submit_args(cfg, "ensemble", "n", "/d", 60.5)
        self.assertEqual(args, ["submit", "--job-name=n", "--cwd=/d", "--output=stdout.log",
                                "--error=stderr.log", "-t", "61s", "-q", "pbatch", "-N1", "a"])

    def test_refuses_to_run_inside_a_flux_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.json")
            with open(path, "w") as handle:
                json.dump({"name": "t", "run_dir": "run", "cycles": 1,
                           "time": {"member": "1m", "analysis": "1m"},
                           "ensemble": {"size": 1, "command": ["true"]},
                           "analysis": {"command": ["true"]}}, handle)
            done = subprocess.run([sys.executable, os.path.join(ROOT, "campaign.py"), "run", path],
                                  env=dict(os.environ, FLUX_JOB_ID="fA"), stderr=subprocess.PIPE)
            self.assertNotEqual(done.returncode, 0)
            self.assertIn(b"outside Flux jobs", done.stderr)
            self.assertFalse(os.path.exists(os.path.join(tmp, "run")))


@unittest.skipUnless(IN_FLUX, "needs a Flux instance")
class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="campaign-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def config(self, size=2, cycles=1, scripts=None, ensemble=None, analysis=None):
        raw = {"name": "t", "run_dir": "run", "cycles": cycles, "poll_seconds": 0.2,
               "time": {"member": "1m", "analysis": "1m"},
               "ensemble": dict({"size": size, "command": COMPONENT}, **(ensemble or {})),
               "analysis": dict({"command": COMPONENT}, **(analysis or {}))}
        path = os.path.join(self.tmp, "config.json")
        with open(path, "w") as handle:
            json.dump(raw, handle)
        self.cfg = campaign.load_config(path)
        self.scripts = scripts or {}
        return self.cfg

    def go(self, cfg=None):
        """Run the campaign, giving each named member its own script."""
        cfg = cfg or self.cfg
        original = campaign.member_params
        scripts = self.scripts

        counter = {"next": 0}

        def params_for_member(c, state):
            params = original(c, state)
            member = counter["next"] % c["ensemble"]["size"]
            counter["next"] += 1
            if member in scripts:
                params["script"] = scripts[member]
            return params
        campaign.member_params = params_for_member
        try:
            code = campaign.run(cfg)
        finally:
            campaign.member_params = original
        with open(os.path.join(cfg["run_dir"], "state.json")) as handle:
            return code, json.load(handle)

    def test_any_size_runs_analysis_once_after_all_members(self):
        for size in (1, 5, 16):
            with self.subTest(size=size):
                self.tmp = tempfile.mkdtemp(prefix="campaign-test-")
                self.addCleanup(shutil.rmtree, self.tmp, True)
                self.config(size=size)
                code, state = self.go()
                self.assertEqual(code, 0)
                self.assertEqual(len(state["cycles"][0]["members"]), size)
                self.assertEqual(state["cycles"][0]["analysis"]["attempt"], 1)

    def test_unpredicted_retries_then_succeeds(self):
        self.config(scripts={1: ["crash", "exit:1", "ok"]}, ensemble={"retries": 2})
        code, state = self.go()
        self.assertEqual(code, 0)
        self.assertEqual(state["cycles"][0]["members"][1]["unpredicted"], 2)

    def test_unpredicted_retries_exhausted_blocks_analysis(self):
        self.config(scripts={0: ["exit:1"]}, ensemble={"retries": 1})
        code, state = self.go()
        self.assertEqual(code, 1)
        self.assertEqual(state["cycles"][0]["members"][0]["attempt"], 2)
        self.assertEqual(state["cycles"][0]["members"][1]["status"], "DONE")
        self.assertIsNone(state["cycles"][0]["analysis"])

    def test_predicted_reconfigure_uses_its_own_budget(self):
        self.config(size=1, scripts={0: ["exit:20", "ok"]}, ensemble={
            "retries": 0, "params": {"dt": 1, "retry_params": '{"dt": 0.5}'},
            "failures": {"UNSTABLE": {"exit_code": 20, "retries": 1, "set": {"solver": "safe"}}}})
        code, state = self.go()
        self.assertEqual(code, 0)
        task = state["cycles"][0]["members"][0]
        self.assertEqual(task["history"][0]["reason"], "UNSTABLE")
        self.assertEqual(task["unpredicted"], 0)
        with open(os.path.join(task["dir"], "params.json")) as handle:
            params = json.load(handle)
        self.assertEqual((params["dt"], params["solver"]), (0.5, "safe"))

    def test_zero_retries_means_fatal(self):
        self.config(size=1, scripts={0: ["exit:22", "ok"]}, ensemble={
            "retries": 5, "failures": {"BAD_INPUT": {"exit_code": 22, "retries": 0}}})
        code, state = self.go()
        self.assertEqual(code, 1)
        self.assertEqual(state["cycles"][0]["members"][0]["attempt"], 1)

    def test_timeout_scales_time_limit(self):
        self.config(size=1, scripts={0: ["sleep:30", "ok"]}, ensemble={
            "failures": {"TIMEOUT": {"retries": 1, "time_scale": 10}}})
        self.cfg["time"]["member"] = "2s"
        code, state = self.go()
        self.assertEqual(code, 0)
        task = state["cycles"][0]["members"][0]
        self.assertEqual(task["history"][0]["reason"], "TIMEOUT")
        self.assertEqual(task["time"], 20)

    def test_analysis_retries_and_steers_next_cycle(self):
        self.config(cycles=3, analysis={
            "retries": 1,
            "params": {"script": ["exit:1", "ok"],
                       "next_cycle": '{"params": {"forcing": 7}, "continue": false}'}})
        code, state = self.go()
        self.assertEqual(code, 0)
        self.assertEqual(state["cycles"][0]["analysis"]["attempt"], 2)
        self.assertEqual(len(state["cycles"]), 1)  # the analysis ended it early

    def test_analysis_parameters_reach_next_cycle(self):
        self.config(cycles=2, analysis={"params": {"next_cycle": '{"params": {"forcing": 7}}'}})
        code, state = self.go()
        self.assertEqual(code, 0)
        self.assertEqual(state["cycles"][1]["members"][0]["params"]["forcing"], 7)
        self.assertNotIn("forcing", state["cycles"][0]["members"][0]["params"])

    def test_malformed_next_cycle_fails_the_analysis(self):
        self.config(size=1, analysis={"retries": 0, "params": {"next_cycle": "{oops"}})
        code, state = self.go()
        self.assertEqual(code, 1)
        self.assertIn("bad next_cycle", state["cycles"][0]["analysis"]["history"][0]["reason"])

    def test_rerun_retries_failed_tasks_and_keeps_finished_ones(self):
        self.config(scripts={0: ["exit:1", "ok"]}, ensemble={"retries": 0})
        self.assertEqual(self.go()[0], 1)
        code, state = self.go()
        self.assertEqual(code, 0)
        self.assertEqual(state["cycles"][0]["members"][0]["attempt"], 2)
        self.assertEqual(state["cycles"][0]["members"][1]["attempt"], 1)
        self.assertEqual(self.go()[0], 0)  # already complete

    def test_lost_job_resubmitted_without_using_a_retry(self):
        self.config(size=1, ensemble={"retries": 0})
        self.assertEqual(self.go()[0], 0)
        path = os.path.join(self.cfg["run_dir"], "state.json")
        with open(path) as handle:
            state = json.load(handle)
        state["status"] = "paused"
        state["cycles"][0]["analysis"] = None
        state["cycles"][0]["members"][0].update(status="RUNNING", job="f111111")
        with open(path, "w") as handle:
            json.dump(state, handle)
        code, state = self.go()
        self.assertEqual(code, 0)
        task = state["cycles"][0]["members"][0]
        self.assertEqual(task["history"][-2]["kind"], "lost")
        self.assertEqual((task["attempt"], task["unpredicted"]), (2, 0))

    def test_resume_reattaches_to_job_still_running(self):
        self.config(size=1, scripts={0: ["sleep:4"]}, ensemble={"retries": 0})
        real_poll = campaign.poll
        self.addCleanup(setattr, campaign, "poll", real_poll)

        def driver_dies(_job_ids):
            raise KeyboardInterrupt
        campaign.poll = driver_dies
        with self.assertRaises(KeyboardInterrupt):
            self.go()
        campaign.poll = real_poll
        path = os.path.join(self.cfg["run_dir"], "state.json")
        with open(path) as handle:
            first_job = json.load(handle)["cycles"][0]["members"][0]["job"]
        code, state = self.go()
        self.assertEqual(code, 0)
        task = state["cycles"][0]["members"][0]
        self.assertEqual((task["attempt"], task["job"]), (1, first_job))

    def test_failed_status_queries_do_not_resubmit(self):
        self.config(size=1, ensemble={"retries": 0})
        real_flux = campaign.flux
        self.addCleanup(setattr, campaign, "flux", real_flux)
        failures = {"left": 5}

        def flaky(*args, **kwargs):
            if args[0] == "jobs" and failures["left"]:
                failures["left"] -= 1
                return 1, "", "Unable to connect to Flux"
            return real_flux(*args, **kwargs)
        campaign.flux = flaky
        code, state = self.go()
        self.assertEqual(code, 0)
        self.assertEqual(state["cycles"][0]["members"][0]["attempt"], 1)


if __name__ == "__main__":
    unittest.main()
