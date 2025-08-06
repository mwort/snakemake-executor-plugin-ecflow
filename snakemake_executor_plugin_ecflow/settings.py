from dataclasses import dataclass, field
from typing import List, Generator, Optional
from pathlib import Path

from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)


# Optional:
# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    # myparam: Optional[int] = field(
    #     default=None,
    #     metadata={
    #         "help": "Some help text",
    #         # Optionally request that setting is also available for specification
    #         # via an environment variable. The variable will be named automatically as
    #         # SNAKEMAKE_<executor-name>_<param-name>, all upper case.
    #         # This mechanism should only be used for passwords and usernames.
    #         # For other items, we rather recommend to let people use a profile
    #         # for setting defaults
    #         # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
    #         "env_var": False,
    #         # Optionally specify a function that parses the value given by the user.
    #         # This is useful to create complex types from the user input.
    #         "parse_func": ...,
    #         # If a parse_func is specified, you also have to specify an unparse_func
    #         # that converts the parsed value back to a string.
    #         "unparse_func": ...,
    #         # Optionally specify that setting is required when the executor is in use.
    #         "required": True,
    #     },
    # )
    deploy_dir: Optional[str] = field(
        default=None,
        metadata={
            "help": "Path where suite will be deployed. Defaults to <work_dir>/ecflow_suites/<suite_name> .",
            "required": False,
            "parse_func": Path,
            "unparse_func": str,
        },
    )
    output_dir: Optional[str] = field(
        default=None,
        metadata={
            "help": "Path where output of tasks will be written. Defaults to <deploy_dir>/logs .",
            "required": False,
            "parse_func": Path,
            "unparse_func": str,
        },
    )
    suite_name: Optional[str] = field(
        default=None,
        metadata={
            "help": "Name of deployed suite. Defaults to the name of the default target.",
        },
    )
    deploy: Optional[str] = field(
        default=None,
        metadata={
            "help": "Whether to deploy the suite (--deploy=suite) or parts of it (e.g. --deploy=rule/job).",
        },
    )
    host: Optional[str] = field(
        default=None,
        metadata={
            "help": "ECflow server to upload the suite to. Exisiting suites will be replaced.",
        },
    )
    output_def_file: Optional[str] = field(
        default=None,
        metadata={
            "help": "Path to write ecflow def file to. Optional.",
            "required": False,
        },
    )
    linked: Optional[bool] = field(
        default=True,
        metadata={
            "help": "Whether to link the ecflow suite to the project it is launched from, i.e. using the same "
            "Snakefile, python environment, etc. Useful for development.",
            "required": False,
        },
    )
    execute: Optional[bool] = field(
        default=False,
        metadata={
            "help": "Whether to execute the workflow after deploying it and monitoring it by snakemake. By default "
            "the suite is only build and deployed if the ecflow-deploy arg is given.",
            "required": False,
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Whether the executor implies to not have a shared file system
    implies_no_shared_fs=False,
    # whether to deploy workflow sources to default storage provider before execution
    job_deploy_sources=True,
    # whether arguments for setting the storage provider shall be passed to jobs
    pass_default_storage_provider_args=True,
    # whether arguments for setting default resources shall be passed to jobs
    pass_default_resources_args=True,
    # whether environment variables shall be passed to jobs (if False, use
    # self.envvars() to obtain a dict of environment variables and their values
    # and pass them e.g. as secrets to the execution backend)
    pass_envvar_declarations_to_cmd=True,
    # whether the default storage provider shall be deployed before the job is run on
    # the remote node. Usually set to True if the executor does not assume a shared fs
    auto_deploy_default_storage_provider=True,
    # specify initial amount of seconds to sleep before checking for job status
    init_seconds_before_status_checks=0,
)
