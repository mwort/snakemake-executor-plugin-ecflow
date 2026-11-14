import re
import os
from pathlib import Path
import time
import shutil
from typing import Optional
from snakemake.common.tests import handle_testcase, TestWorkflowsLocalStorageBase
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
from snakemake.settings.types import ResourceSettings
from snakemake_executor_plugin_ecflow import ExecutorSettings
import uuid
import pytest

from conftest import CLEAN_SUITES


class TestWorkflowsBase(TestWorkflowsLocalStorageBase):
    __test__ = False
    latency_wait = 60
    ecflow_suite_name = None
    ecflow_host = os.environ.get("ECF_HOST", f"ecflow-gen-{os.environ['USER']}-001")

    # by default switch off snakemake common tests
    test_simple_workflow = None
    test_group_workflow = None

    def get_executor(self) -> str:
        return "ecflow"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instantiate ExecutorSettings of this plugin as appropriate
        self.ecflow_suite_name = f"sm_test_suite_{uuid.uuid4().hex}"
        return ExecutorSettings(
            host=self.ecflow_host,
            suite_name=self.ecflow_suite_name,
            deploy="suite",
            output_def_file="test_suite.def",
            execute=True,
            linked=True,
        )

    def cleanup_test(self):
        # A test that fails before the executor is instantiated has no suite to
        # clean, and handle_testcase runs this in a finally block -- raising here
        # would replace the real failure with a confusing one about /None.
        if not CLEAN_SUITES or self.ecflow_suite_name is None:
            return
        # since the task deletes the suite, the waiting check can fail
        self.run_task("clean/clean_suite", wait=False)
        try:
            self.wait("clean/clean_suite")
        except RuntimeError:
            pass

    @property
    def ecflow_client(self):
        import ecflow

        if not hasattr(self, "_ecflow_client"):
            self._ecflow_client = ecflow.Client(self.ecflow_host, 3141)
        return self._ecflow_client

    def get_state(self, node):
        return self.ecflow_client.query("state", f"{self.ecflow_suite_name}/{node}")

    def run_task(self, node, wait=False):
        self.ecflow_client.run(f"{self.ecflow_suite_name}/{node}", False)
        if wait:
            self.wait(node)

    def _copy_test_files(self, test_path, tmp_path):
        # snakemake's run_workflow only knows its own testcases, so a local
        # tests/<name>_testcase takes precedence. Copied straight into the
        # test's tmp dir, so parallel workers (pytest -n) share nothing.
        local = Path(__file__).parent / f"{test_path.name}_testcase"
        shutil.copytree(local if local.is_dir() else test_path, tmp_path)

    def wait_for_file(self, path, exists=True) -> bool:
        """Whether a file written by the suite (dis)appears within latency_wait.

        Files written on the compute nodes can take a while to show up here
        over NFS, in particular when snakemake looked them up as missing while
        building the DAG. Listing the directory refreshes that, a stat may not.
        """
        path = Path(path)
        start = time.time()
        while True:
            found = path.parent.is_dir() and path.name in os.listdir(path.parent)
            if found == exists or time.time() - start > self.latency_wait:
                return found == exists
            time.sleep(2)

    def wait(self, node, timeout=900):
        node_path = f"/{self.ecflow_suite_name}/{node}"
        state = None
        start = time.time()
        while state not in ["complete", "aborted"]:
            if time.time() - start > timeout:
                raise TimeoutError(f"ecFlow node {node_path} still {state} after {timeout}s.")
            time.sleep(5)
            state = self.get_state(node)
        if state == "aborted":
            raise RuntimeError(f"ecFlow task {node_path} was aborted.")


class TestSuiteExecution(TestWorkflowsBase):
    __test__ = True

    @handle_testcase
    def test_simple_workflow(self, tmp_path):
        self.run_workflow("simple", tmp_path)
        self.run_task("admin/evaluate", wait=True)

    @handle_testcase
    def test_group_workflow(self, tmp_path):
        self.run_workflow("groups", tmp_path)
        self.run_task("admin/evaluate", wait=True)

class TestDeployUnlinked(TestWorkflowsBase):
    __test__ = True

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instantiate ExecutorSettings of this plugin as appropriate
        self.ecflow_suite_name = f"sm_test_suite_{uuid.uuid4().hex}"
        return ExecutorSettings(
            host=self.ecflow_host,
            suite_name=self.ecflow_suite_name,
            execute=False,
            linked=False,
            deploy="suite",
            # The suite builds its own environment, so without this it would
            # install the published plugin and the tasks would run code that has
            # nothing to do with the tree under test.
            plugin_source=str(Path(__file__).parent.parent),
        )

    @handle_testcase
    def test_simple_workflow(self, tmp_path):
        with pytest.raises(SystemExit) as excinfo:
            self.run_workflow("simple", tmp_path)
        assert excinfo.value.code == 0
        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        self.run_task("admin/evaluate", wait=True)

    @handle_testcase
    def test_group_workflow(self, tmp_path):
        with pytest.raises(SystemExit) as excinfo:
            self.run_workflow("groups", tmp_path)
        assert excinfo.value.code == 0

        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        self.run_task("admin/evaluate", wait=True)

    def cleanup_test(self):
        # see TestWorkflowsBase.cleanup_test
        if not CLEAN_SUITES or self.ecflow_suite_name is None:
            return
        self.ecflow_client.delete(f"/{self.ecflow_suite_name}", True)


class TestCustomSnakefile(TestWorkflowsBase):
    """A realistic Snakefile: groups, wildcards, resources and ecflow params.

    Built and deployed but not executed. The workflow builds a conda
    environment and renders a report from downloaded data, none of which a test
    can run; what is being checked is that a complicated Snakefile still yields
    a valid suite definition.
    """

    __test__ = True

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        self.ecflow_suite_name = f"sm_test_suite_{uuid.uuid4().hex}"
        return ExecutorSettings(
            host=self.ecflow_host,
            suite_name=self.ecflow_suite_name,
            deploy="suite",
            output_def_file="test_suite.def",
            execute=False,
            linked=True,
        )

    @handle_testcase
    def test_custom_workflow(self, tmp_path):
        with pytest.raises(SystemExit) as excinfo:
            self.run_workflow("custom", tmp_path)
        assert excinfo.value.code == 0


class TestRunRules(TestWorkflowsBase):
    """Rules without a shell directive, which call snakemake back for the job."""

    __test__ = True

    @handle_testcase
    def test_run_workflow(self, tmp_path):
        self.run_workflow("run_rules", tmp_path)
        self.run_task("admin/evaluate", wait=True)


class TestDeployedSuite(TestWorkflowsBase):
    """Suites that are deployed by snakemake but run by ecFlow alone.

    Checkpoints and temp() cleanup only happen inside the suite (snakemake
    returns as soon as it is deployed), so these resume the suite and check
    the outputs once it is done.
    """

    __test__ = True
    def_file = "test_suite.def"
    addmeter = 10
    monitor = 0

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        self.ecflow_suite_name = f"sm_test_suite_{uuid.uuid4().hex}"
        return ExecutorSettings(
            host=self.ecflow_host,
            suite_name=self.ecflow_suite_name,
            deploy="suite",
            output_def_file=self.def_file,
            execute=False,
            linked=True,
            addmeter=self.addmeter,
            monitor=self.monitor,
        )

    def get_resource_settings(self) -> ResourceSettings:
        # only constrains the resources testcase, the others request no gpu
        return ResourceSettings(resources={"gpu": 2})

    def deploy_and_run(self, name, tmp_path) -> Path:
        """Deploy a local testcase, run the suite and return its workdir."""
        with pytest.raises(SystemExit) as excinfo:
            self.run_workflow(name, tmp_path)
        assert excinfo.value.code == 0
        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        # temp() cleanup runs alongside main; deleting the suite under a running
        # task leaves its job retrying --complete for ECF_TIMEOUT
        self.wait("clean")
        return Path(tmp_path) / name

    @handle_testcase
    def test_temp_workflow(self, tmp_path):
        workdir = Path(tmp_path) / "temp"
        with pytest.raises(SystemExit):
            self.run_workflow("temp", tmp_path)
        definition = (workdir / self.def_file).read_text()
        for sample in [1, 2]:
            assert f"task sample_{sample}_cleanup" in definition

        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        self.wait("clean")
        for sample in [1, 2]:
            assert self.wait_for_file(workdir / f"final.{sample}.out")
            assert (workdir / f"final.{sample}.out").read_text().strip() == str(sample)
            assert self.wait_for_file(workdir / f"temp.{sample}.out", exists=False)

    @handle_testcase
    def test_checkpoint_workflow(self, tmp_path):
        workdir = self.deploy_and_run("checkpoint", tmp_path)
        # the processed files only exist if the checkpoint re-expanded the suite
        assert self.wait_for_file(workdir / "aggregated.out")
        assert (workdir / "aggregated.out").read_text().split() == ["1", "2", "3"]
        for i in [1, 2, 3]:
            assert self.wait_for_file(workdir / "processed" / f"{i}.out")

    @handle_testcase
    def test_checkpoint_per_job(self, tmp_path):
        # with a checkpoint job per wildcard, each only replaces its own dependents
        workdir = Path(tmp_path) / "checkpoints"
        with pytest.raises(SystemExit):
            self.run_workflow("checkpoints", tmp_path)
        definition = (workdir / self.def_file).read_text()
        deploy = {
            block.split()[0]: block.split("CHECKPOINT_DEPLOY '")[1].split("'")[0]
            for block in definition.split("task ")[1:] if "CHECKPOINT_DEPLOY" in block
        }
        assert set(deploy) == {"g_a", "g_b"}
        for g, other in [("a", "b"), ("b", "a")]:
            assert f"g_{g}" in deploy[f"g_{g}"] and f"g_{other}" not in deploy[f"g_{g}"]

        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        self.wait("clean")
        for g in ["a", "b"]:
            assert self.wait_for_file(workdir / f"aggregated.{g}.out")
            assert (workdir / f"aggregated.{g}.out").read_text().split() == [f"{g}1", f"{g}2"]

    @handle_testcase
    def test_resource_limits(self, tmp_path):
        workdir = self.deploy_and_run("resources", tmp_path)
        definition = (workdir / self.def_file).read_text()
        limit = f"/{self.ecflow_suite_name}/admin:gpu"
        assert "limit gpu 2" in definition
        tasks = {
            block.split()[0]: block
            for block in definition.split("task ")[1:]
        }
        # one token for each small job, as many as requested for the big one
        assert definition.count(f"inlimit {limit}\n") == 2
        assert f"inlimit {limit} 2" in tasks["big"]
        assert limit not in tasks["free"]
        for f in ["small.1.out", "small.2.out", "big.out", "free.out"]:
            assert self.wait_for_file(workdir / f)

    @handle_testcase
    def test_meter(self, tmp_path):
        self.addmeter = 3
        workdir = Path(tmp_path) / "meter"
        with pytest.raises(SystemExit):
            self.run_workflow("meter", tmp_path)
        definition = (workdir / self.def_file).read_text()
        # family name -> its attributes, i.e. everything before its first task
        families = {
            block.split()[0]: block.split("task ")[0]
            for block in definition.split("family ")[1:]
        }
        assert "meter done 0 3" in families["counted"]
        assert "meter done" not in families["single"]

        path = f"/{self.ecflow_suite_name}/main/counted"
        # the task whose output already existed is counted from the start
        assert self.ecflow_client.query("meter", path, "done") == "1"
        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        assert self.ecflow_client.query("meter", path, "done") == "3"

    @handle_testcase
    def test_priority(self, tmp_path):
        workdir = self.deploy_and_run("priority", tmp_path)
        definition = (workdir / self.def_file).read_text()
        main = definition.split("family main")[1]  # main is the last family
        # the direct children of main, in definition order
        children = re.findall(r"^ {4}(?:family|task) (\w+)", main, re.M)
        assert children == ["prep", "urgent", "early", "late"]
        for f in ["late.1.out", "late.2.out", "urgent.out"]:
            assert self.wait_for_file(workdir / f)

    @handle_testcase
    def test_done_tasks(self, tmp_path):
        # A task that need not run is set complete after the upload, not given
        # defstatus complete, which requeue would reset it to.
        workdir = Path(tmp_path) / "meter"
        with pytest.raises(SystemExit):
            self.run_workflow("meter", tmp_path)
        definition = (workdir / self.def_file).read_text()
        main = definition.split("family main")[1]  # main is the last family
        assert "defstatus complete" not in main
        # counted.1.out ships with the testcase
        assert self.get_state("main/counted/sample_1") == "complete"
        assert self.get_state("main/counted/sample_2") == "queued"
        self.ecflow_client.requeue(f"/{self.ecflow_suite_name}/main/counted/sample_1")
        assert self.get_state("main/counted/sample_1") == "queued"
        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")

    @handle_testcase
    def test_monitor(self, tmp_path):
        # without --ecflow-monitor the monitor task is complete and run by hand; it
        # reports the progress in its labels, job output and progress.md
        self.addmeter = 3
        workdir = self.deploy_and_run("meter", tmp_path)
        # and corrects a rule meter that is off, e.g. after a lost count, and its
        # maximum, e.g. after a checkpoint added tasks
        rule = f"/{self.ecflow_suite_name}/main/counted"
        self.ecflow_client.alter(rule, "delete", "meter", "done")
        self.ecflow_client.alter(rule, "add", "meter", "done", "0,2,1")
        definition = (workdir / self.def_file).read_text()
        monitor = definition.split("task monitor")[1].split("task ")[0]
        assert "defstatus complete" in monitor and "cron" not in monitor
        path = f"/{self.ecflow_suite_name}/admin/monitor"
        self.run_task("admin/monitor", wait=True)
        label = self.ecflow_client.query("label", path, "progress")
        assert label.startswith("100.0% done"), label
        assert self.ecflow_client.query("label", path, "status").endswith("0 aborted")
        assert self.ecflow_client.query("label", path, "time").startswith("eta 0s")
        assert self.ecflow_client.query("meter", rule, "done") == "3"
        self.ecflow_client.sync_local()
        meter = self.ecflow_client.get_defs().find_abs_node(rule).meters
        assert [(m.min(), m.max()) for m in meter] == [(0, 3)]
        report = workdir / "ecflow_suites" / self.ecflow_suite_name / "progress.md"
        assert self.wait_for_file(report)
        rows = [l.split(" | ") for l in report.read_text().splitlines() if l.startswith("| counted")]
        assert rows and rows[0][3] == "100.0%"

    @handle_testcase
    def test_monitor_cron(self, tmp_path):
        # with --ecflow-monitor it runs on a cron while main is active or submitted
        self.monitor = 30
        with pytest.raises(SystemExit):
            self.run_workflow("meter", tmp_path)
        definition = (Path(tmp_path) / "meter" / self.def_file).read_text()
        monitor = definition.split("task monitor")[1].split("task ")[0]
        assert "trigger ../main eq active or ../main eq submitted" in monitor
        assert "cron 00:00 23:59 00:30" in monitor and "defstatus" not in monitor

    @handle_testcase
    def test_shared_scripts(self, tmp_path):
        # the tasks of a rule run one script named after it, in its family
        workdir = self.deploy_and_run("temp", tmp_path)
        files = workdir / "ecflow_suites" / self.ecflow_suite_name
        for rule in ["make_temp", "use_temp"]:
            scripts = [f.name for f in (files / "main" / rule).rglob("*.ecf")]
            assert scripts == [f"{rule}.ecf"]
        scripts = [f.name for f in (files / "clean" / "make_temp").rglob("*.ecf")]
        assert scripts == ["make_temp_cleanup.ecf"]
