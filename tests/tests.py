import os
from pathlib import Path
import time
from typing import Optional
from snakemake.common.tests import handle_testcase, TestWorkflowsLocalStorageBase
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
from snakemake_executor_plugin_ecflow import ExecutorSettings
import uuid
import pytest


class TestWorkflowsBase(TestWorkflowsLocalStorageBase):
    __test__ = True
    latency_wait = 60
    ecflow_suite_name = None
    ecflow_host = os.environ.get("ECF_HOST", f"ecflow-gen-{os.environ['USER']}-001")

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

    @property
    def ecflow_client(self):
        import ecflow

        if not hasattr(self, "_ecflow_client"):
            self._ecflow_client = ecflow.Client(self.ecflow_host, 3141)
        return self._ecflow_client

    def get_state(self, node):
        return self.ecflow_client.query("state", f"{self.ecflow_suite_name}/{node}")

    def cleanup_test(self):
        self.ecflow_client.run(f"{self.ecflow_suite_name}/clean/clean_suite", False)


class TestDeployOnly(TestWorkflowsBase):
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
        assert excinfo.value.code == 0  # Check exit code if needed

    @handle_testcase
    def test_group_workflow(self, tmp_path): ...

    def cleanup_test(self): ...