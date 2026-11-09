"""Run inside Flux: flux start --test-size=2 python3 -m unittest discover -s tests"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

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

    def test_queue_and_bank_only_on_login_node_jobs(self):
        cfg = {"queue": "pbatch", "bank": "b",
               "ensemble": {"nodes": 2, "flux_args": ["-n4"], "command": ["a"]}}
        head = ["submit", "--job-name=n", "--cwd=/d", "--output=stdout.log",
                "--error=stderr.log", "-t", "61s"]
        self.assertEqual(campaign.submit_args(cfg, "login", "ensemble", "n", "/d", 60.5),
                         head + ["-q", "pbatch", "-B", "b", "-N", "2", "-n4", "a"])
        self.assertEqual(campaign.submit_args(cfg, "allocation", "ensemble", "n", "/d", 60.5),
                         head + ["-N", "2", "-n4", "a"])

    def test_mode_comes_from_where_it_starts(self):
        real_flux = campaign.flux
        self.addCleanup(setattr, campaign, "flux", real_flux)
        cases = [((0, "1\n", ""), {}, "allocation"), ((0, "0\n", ""), {}, "login"),
                 ((1, "", "no flux"), {}, "login"),
                 ((0, "0\n", ""), {"FLUX_JOB_ID": "fA"}, None),
                 ((0, "2\n", ""), {"FLUX_JOB_ID": "fA"}, "allocation")]
        for result, env, expected in cases:
            campaign.flux = lambda *args, **kwargs: result
            with unittest.mock.patch.dict(os.environ, env):
                if "FLUX_JOB_ID" not in env:
                    os.environ.pop("FLUX_JOB_ID", None)
                self.assertEqual(campaign.detect_mode(), expected, (result, env))

    def test_allocation_plan_and_batch_command(self):
        cfg = {"name": "x", "run_dir": "/r", "config_path": "/c.json", "cycles": 3,
               "queue": "pbatch", "bank": "b",
               "time": {"member": "8m", "analysis": "3m", "buffer": "2m", "reserve": "10m"},
               "allocation": {"nodes": 8}, "ensemble": {"size": 16, "nodes": 1},
               "analysis": {}}
        times = campaign.plan(cfg)
        self.assertEqual((times["nodes"], times["waves"]), (8, 2))
        self.assertEqual(times["total"], 3 * (2 * 600 + 300) + 600)
        self.assertEqual(times["request"], times["total"])
        args = campaign.batch_args(cfg)
        self.assertEqual(args[:9], ["batch", "-N", "8", "-t", "5100s", "--exclusive",
                                    "--job-name=x", "-q", "pbatch"])
        self.assertIn("--wrap", args)
        self.assertEqual(args[-2:], ["run", "/c.json"])
        cfg["allocation"]["max_time"] = "1h"
        self.assertEqual(campaign.plan(cfg)["request"], 3600)
        cfg["allocation"] = {"time": "2h"}
        self.assertEqual(campaign.plan(cfg)["request"], 7200)
        self.assertEqual(campaign.plan(cfg)["nodes"], 16)

    def test_refuses_to_run_inside_a_single_job(self):
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
            self.assertIn(b"not inside a single job", done.stderr)
            self.assertFalse(os.path.exists(os.path.join(tmp, "run")))


class ValidationTests(unittest.TestCase):
    def base(self):
        return {"name": "t", "run_dir": "run", "cycles": 2, "poll_seconds": 5,
                "queue": "pbatch", "bank": "b",
                "time": {"member": "1m", "analysis": "1m", "buffer": "30s", "reserve": "0s"},
                "allocation": {"nodes": 2, "max_time": "1h", "flux_args": ["--urgency=20"]},
                "ensemble": {"size": 4, "nodes": 1, "command": ["run"], "flux_args": ["-n4"],
                             "params": {"dt": 0.1}, "retries": 2,
                             "failures": {"BAD": {"exit_code": 20, "retries": 0,
                                                  "set": {"a": 1}, "time_scale": 2},
                                          "TIMEOUT": {"retries": 1, "time_scale": 1.5}}},
                "analysis": {"command": ["an"]}}

    def errors_for(self, change):
        cfg = self.base()
        change(cfg)
        return campaign.validate(cfg)

    def assert_rejected(self, change, *fragments):
        errors = self.errors_for(change)
        text = "\n".join(errors)
        for fragment in fragments:
            self.assertIn(fragment, text)

    def test_valid_and_shipped_configs_pass(self):
        self.assertEqual(campaign.validate(self.base()), [])
        for name in ("tuolumne.json", "smoke.json"):
            with open(os.path.join(ROOT, "configs", name)) as handle:
                self.assertEqual(campaign.validate(json.load(handle)), [], name)

    def test_unknown_keys_at_every_level(self):
        self.assert_rejected(lambda c: c.update(cycels=3), "config.cycels: unknown key")
        self.assert_rejected(lambda c: c["time"].update(bufer="1m"), "time.bufer: unknown key")
        self.assert_rejected(lambda c: c["allocation"].update(node=2),
                             "allocation.node: unknown key")
        self.assert_rejected(lambda c: c["ensemble"].update(retires=1),
                             "ensemble.retires: unknown key")
        self.assert_rejected(lambda c: c["analysis"].update(size=1), "analysis.size: unknown key")
        self.assert_rejected(lambda c: c["ensemble"]["failures"]["BAD"].update(retry=1),
                             "ensemble.failures.BAD.retry: unknown key")

    def test_missing_required_keys(self):
        self.assert_rejected(lambda c: c.pop("name"), "config.name: required")
        self.assert_rejected(lambda c: c["time"].pop("member"), "time.member: required")
        self.assert_rejected(lambda c: c["ensemble"].pop("size"), "ensemble.size: required")
        self.assert_rejected(lambda c: c["analysis"].pop("command"), "analysis.command: required")
        self.assert_rejected(lambda c: c["ensemble"]["failures"]["BAD"].pop("retries"),
                             "ensemble.failures.BAD.retries: required")
        self.assert_rejected(lambda c: c["ensemble"]["failures"]["BAD"].pop("exit_code"),
                             "BAD.exit_code: required (only TIMEOUT can omit it)")

    def test_wrong_types_and_ranges(self):
        cases = [
            (lambda c: c.update(cycles=0), "config.cycles: must be an integer >= 1"),
            (lambda c: c.update(cycles=True), "config.cycles: must be an integer"),
            (lambda c: c.update(poll_seconds=0), "config.poll_seconds: must be a number > 0"),
            (lambda c: c.update(name="my run"), "config.name: must be letters"),
            (lambda c: c["time"].update(member="30"), "time.member: must be a duration"),
            (lambda c: c["time"].update(member="0s"), "time.member: must be a duration greater"),
            (lambda c: c["time"].update(buffer=60), "time.buffer: must be a duration"),
            (lambda c: c["ensemble"].update(size="16"), "ensemble.size: must be an integer"),
            (lambda c: c["ensemble"].update(command="run.sh"), "ensemble.command: must be a non-empty list"),
            (lambda c: c["ensemble"].update(command=[]), "ensemble.command: must be a non-empty list"),
            (lambda c: c["ensemble"].update(retries=-1), "ensemble.retries: must be an integer >= 0"),
            (lambda c: c["ensemble"].update(flux_args="-n4"), "ensemble.flux_args: must be a list"),
            (lambda c: c["ensemble"].update(params=[1]), "ensemble.params: must be an object"),
            (lambda c: c["ensemble"]["failures"]["BAD"].update(exit_code=1),
             "BAD.exit_code: must be an integer >= 3 and <= 125"),
            (lambda c: c["ensemble"]["failures"]["BAD"].update(exit_code=139), "<= 125"),
            (lambda c: c["ensemble"]["failures"]["BAD"].update(time_scale=0), "time_scale: must be a number > 0"),
            (lambda c: c.update(time=[]), "config.time: must be an object"),
        ]
        for change, fragment in cases:
            with self.subTest(fragment=fragment):
                self.assert_rejected(change, fragment)

    def test_failure_rules(self):
        self.assert_rejected(lambda c: c["ensemble"]["failures"].update(
            OTHER={"exit_code": 20, "retries": 1}), "OTHER.exit_code: 20 is already used by BAD")
        self.assert_rejected(lambda c: c["ensemble"]["failures"].update(
            bad_input={"exit_code": 22, "retries": 0}), "bad_input: reason names must be UPPER_CASE")
        self.assert_rejected(lambda c: c["ensemble"]["failures"].update(X=[]),
                             "ensemble.failures.X: must be an object")

    def test_flags_campaign_sets_are_refused(self):
        for arg in ("-t", "-t60", "--time-limit=60", "--job-name=x", "--output=o", "-q",
                    "--queue=pbatch", "-Bbank", "--cwd=/tmp", "--error=e"):
            with self.subTest(arg=arg):
                self.assert_rejected(lambda c: c["ensemble"].update(flux_args=[arg]),
                                     f"ensemble.flux_args: {arg!r} is set by campaign.py")
        self.assert_rejected(lambda c: c["ensemble"].update(flux_args=["-N2"]), "'-N2' is set")
        self.assertEqual(self.errors_for(lambda c: (c["ensemble"].pop("nodes"),
                                                    c["ensemble"].update(flux_args=["-N2"]))), [])
        self.assertEqual(self.errors_for(lambda c: c["ensemble"].update(
            flux_args=["--tasks-per-node=4", "-n4", "-c24", "-g1", "--exclusive"])), [])
        self.assert_rejected(lambda c: c["allocation"].update(flux_args=["--exclusive"]),
                             "allocation.flux_args: '--exclusive' is set")
        self.assert_rejected(lambda c: c["allocation"].update(time="2h"),
                             "allocation: set time or max_time, not both")

    def test_other_config_errors_exit_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.json")
            cfg = self.base()
            cfg["bank"] = "YOUR_BANK"
            with open(path, "w") as handle:
                json.dump(cfg, handle)
            env = {k: v for k, v in os.environ.items() if k != "FLUX_JOB_ID"}
            done = subprocess.run([sys.executable, os.path.join(ROOT, "campaign.py"), "run", path],
                                  env=env, stderr=subprocess.PIPE, encoding="utf-8")
            self.assertIn("set bank", done.stderr)
            self.assertEqual(done.returncode, 2)
            cfg["bank"] = "b"
            cfg["allocation"] = {"nodes": 1}
            cfg["ensemble"]["nodes"] = 2
            with open(path, "w") as handle:
                json.dump(cfg, handle)
            done = subprocess.run([sys.executable, os.path.join(ROOT, "campaign.py"), "run", path,
                                   "--dry-run"], stderr=subprocess.PIPE, encoding="utf-8")
            self.assertIn("needs more nodes", done.stderr)
            self.assertEqual(done.returncode, 2)

    def test_bad_exit_code_is_reported_once(self):
        errors = self.errors_for(lambda c: c["ensemble"]["failures"]["BAD"].update(exit_code=1))
        self.assertEqual(len(errors), 1, errors)

    def test_every_problem_is_reported_at_once(self):
        errors = self.errors_for(lambda c: (c.update(cycels=1), c["time"].pop("member"),
                                            c["ensemble"].update(size=0)))
        self.assertEqual(len(errors), 3, errors)

    def test_cli_prints_all_errors_and_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.json")
            cfg = self.base()
            cfg.update(cycels=1)
            cfg["time"]["member"] = "30"
            with open(path, "w") as handle:
                json.dump(cfg, handle)
            done = subprocess.run([sys.executable, os.path.join(ROOT, "campaign.py"), "status",
                                   path], stderr=subprocess.PIPE, encoding="utf-8")
            self.assertEqual(done.returncode, 2)
            self.assertEqual(done.stderr.count("config error:"), 2, done.stderr)
            with open(path, "w") as handle:
                handle.write("{not json")
            done = subprocess.run([sys.executable, os.path.join(ROOT, "campaign.py"), "status",
                                   path], stderr=subprocess.PIPE, encoding="utf-8")
            self.assertIn("config error: cannot read", done.stderr)
            self.assertEqual(done.returncode, 2)


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

    def go(self, cfg=None, mode="login"):
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
            code = campaign.run(cfg, mode)
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

    def test_allocation_pauses_when_job_cannot_fit_then_resumes(self):
        self.config(size=1)
        real_timeleft = campaign.timeleft
        self.addCleanup(setattr, campaign, "timeleft", real_timeleft)
        campaign.timeleft = lambda: 59
        code, state = self.go(mode="allocation")
        self.assertEqual(code, 3)
        self.assertEqual(state["cycles"][0]["members"][0]["attempt"], 0)
        campaign.timeleft = lambda: 1000
        self.assertEqual(self.go(mode="allocation")[0], 0)

    def test_login_mode_never_checks_time_left(self):
        self.config(size=1)
        real_timeleft = campaign.timeleft
        self.addCleanup(setattr, campaign, "timeleft", real_timeleft)
        campaign.timeleft = lambda: 1
        self.assertEqual(self.go(mode="login")[0], 0)

    def test_lifetime_rejection_uses_no_retry(self):
        self.config(size=1, ensemble={"retries": 0})
        real_flux = campaign.flux
        self.addCleanup(setattr, campaign, "flux", real_flux)
        rejections = {"left": 1}

        def rejects_once(*args, **kwargs):
            if args[0] == "submit" and rejections["left"]:
                rejections["left"] -= 1
                return 1, "", "[Errno 22] job duration (2m) exceeds remaining instance lifetime"
            return real_flux(*args, **kwargs)
        campaign.flux = rejects_once
        code, state = self.go(mode="allocation")
        self.assertEqual(code, 0)
        task = state["cycles"][0]["members"][0]
        self.assertEqual((task["unpredicted"], task["history"][0]["kind"]), (0, "success"))

    def test_allocation_refuses_to_take_over_login_node_jobs(self):
        self.config(size=1)
        self.assertEqual(self.go()[0], 0)
        path = os.path.join(self.cfg["run_dir"], "state.json")
        with open(path) as handle:
            state = json.load(handle)
        state["status"] = "paused"
        state["cycles"][0]["analysis"] = None
        state["cycles"][0]["members"][0].update(status="RUNNING", job="f111111", mode="login")
        with open(path, "w") as handle:
            json.dump(state, handle)
        with self.assertRaises(SystemExit):
            self.go(mode="allocation")

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
