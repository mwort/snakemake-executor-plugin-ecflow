import os
from pathlib import Path
import time
import shutil
from typing import Optional
from snakemake.common.tests import handle_testcase, TestWorkflowsLocalStorageBase
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
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
        if CLEAN_SUITES:
            # since the task deletes the suite, the waiting check can fail
            self.run_task("clean/clean_suite", wait=False)
            try:
                self.wait("clean/clean_suite")
            except RuntimeError as e:
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

    def wait(self, node):
        node_path = f"/{self.ecflow_suite_name}/{node}"
        state = None
        while state not in ["complete", "aborted"]:
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
        if CLEAN_SUITES:
            self.ecflow_client.delete(f"/{self.ecflow_suite_name}", True)


class TestCustomSnakefile(TestWorkflowsBase):
    __test__ = True

    def add_custom_testcase(self):
        from snakemake.common.tests import testcases
        # this is a workaround to copy the custom test case because the
        # snakemake.common.tests.__init__.py:TestWorkflowsBase.run_workflow doesnt
        # allow to pass custom test cases
        testcase = Path(__file__).parent / "custom_testcase"
        destination = Path(testcases.__file__).parent / "custom"
        shutil.rmtree(destination, ignore_errors=True)
        shutil.copytree(testcase, destination, dirs_exist_ok=True)

    @handle_testcase
    def test_custom_workflow(self, tmp_path):
        self.add_custom_testcase()
        self.run_workflow("custom", tmp_path)
