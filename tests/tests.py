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

    def add_testcase(self, name, source=None):
        from snakemake.common.tests import testcases
        # this is a workaround to copy a local test case because the
        # snakemake.common.tests.__init__.py:TestWorkflowsBase.run_workflow doesnt
        # allow to pass custom test cases
        testcase = Path(__file__).parent / (source or f"{name}_testcase")
        destination = Path(testcases.__file__).parent / name
        shutil.rmtree(destination, ignore_errors=True)
        shutil.copytree(testcase, destination, dirs_exist_ok=True)

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
        self.add_testcase("custom", source="custom_testcase")
        with pytest.raises(SystemExit) as excinfo:
            self.run_workflow("custom", tmp_path)
        assert excinfo.value.code == 0


class TestRunRules(TestWorkflowsBase):
    """Rules without a shell directive, which call snakemake back for the job."""

    __test__ = True

    @handle_testcase
    def test_run_workflow(self, tmp_path):
        self.add_testcase("run_rules")
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

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        self.ecflow_suite_name = f"sm_test_suite_{uuid.uuid4().hex}"
        return ExecutorSettings(
            host=self.ecflow_host,
            suite_name=self.ecflow_suite_name,
            deploy="suite",
            output_def_file=self.def_file,
            execute=False,
            linked=True,
        )

    def get_resource_settings(self) -> ResourceSettings:
        # only constrains the resources testcase, the others request no gpu
        return ResourceSettings(resources={"gpu": 2})

    def deploy_and_run(self, name, tmp_path) -> Path:
        """Deploy a local testcase, run the suite and return its workdir."""
        self.add_testcase(name)
        with pytest.raises(SystemExit) as excinfo:
            self.run_workflow(name, tmp_path)
        assert excinfo.value.code == 0
        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        return Path(tmp_path) / name

    @handle_testcase
    def test_temp_workflow(self, tmp_path):
        workdir = Path(tmp_path) / "temp"
        self.add_testcase("temp")
        with pytest.raises(SystemExit):
            self.run_workflow("temp", tmp_path)
        definition = (workdir / self.def_file).read_text()
        for sample in [1, 2]:
            assert f"task make_temp_sample_{sample}_cleanup" in definition

        self.ecflow_client.resume(f"/{self.ecflow_suite_name}")
        self.wait("main")
        self.wait("clean")
        for sample in [1, 2]:
            assert (workdir / f"final.{sample}.out").read_text().strip() == str(sample)
            assert not (workdir / f"temp.{sample}.out").exists()

    @handle_testcase
    def test_checkpoint_workflow(self, tmp_path):
        workdir = self.deploy_and_run("checkpoint", tmp_path)
        # the processed files only exist if the checkpoint re-expanded the suite
        assert (workdir / "aggregated.out").read_text().split() == ["1", "2", "3"]
        for i in [1, 2, 3]:
            assert (workdir / "processed" / f"{i}.out").exists()

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
        # one token for the small jobs, as many as requested for the big one
        for sample in [1, 2]:
            assert f"inlimit {limit}\n" in tasks[f"small_sample_{sample}"]
        assert f"inlimit {limit} 2" in tasks["big"]
        assert limit not in tasks["free"]
        for f in ["small.1.out", "small.2.out", "big.out", "free.out"]:
            assert (workdir / f).exists()
