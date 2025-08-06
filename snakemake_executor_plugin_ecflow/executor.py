import sys
import os
import shutil
from pathlib import Path
from typing import List, Generator, Optional

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_common.exceptions import WorkflowError  # noqa
import pyflow as pf


class Executor(RemoteExecutor):

    load_python = "module load python3"
    load_ecflow = "module load ecflow"

    @property
    def settings(self):
        return self.workflow.executor_settings

    @property
    def ecflow_client(self):
        import ecflow

        if not hasattr(self, "_ecflow_client"):
            self._ecflow_client = ecflow.Client(self.settings.host, 3141)
        return self._ecflow_client

    def ecflow_state(self, path_or_pyflow_node: str | pf.nodes.Node) -> str:
        """Get the state of a ecflow node or path."""
        if isinstance(path_or_pyflow_node, str):
            path_or_pyflow_node = self.suite.find_node(path_or_pyflow_node)
        return self.ecflow_client.query("state", path_or_pyflow_node.fullname)

    def __post_init__(self):

        # IMPORTANT: in your plugin, only access methods and properties of
        # Snakemake objects (like Workflow, Persistence, etc.) that are
        # defined in the interfaces found in the
        # snakemake-interface-executor-plugins and the
        # snakemake-interface-common package.
        # Other parts of those objects are NOT guaranteed to remain
        # stable across new releases.

        # To ensure that the used interfaces are not changing, you should
        # depend on these packages as >=a.b.c,<d with d=a+1 (i.e. pin the
        # dependency on this package to be at least the version at time
        # of development and less than the next major version which would
        # introduce breaking changes).

        # In case of errors outside of jobs, please raise a WorkflowError

        suite = self.create_suite()
        families = self.create_families()
        env = self.create_env_task()
        evaluate = self.create_evaluate_task()
        clean = self.create_clean_task()
        tasks = self.create_tasks_from_dag()

        self.deploy()
        if not self.settings.execute:
            sys.exit(0)
        else:
            # begin suite if not already done
            if self.ecflow_state(suite) == "unknown":
                self.ecflow_client.begin_suite("/" + self.suite.name)
        return

    def create_suite(self):
        suite_name = self.settings.suite_name or self.workflow.default_target
        host = pf.TroikaHost(
            "%HOST%",
            user="$USER",
            extra_variables={"HOST": f"%SCHOST:hpc%"},
            server_ecfvars=False,
        )
        # sm changes into the set workdir but doesnt store it in the workflow object
        self.work_dir = restore_common_linked_dirs(self.workflow.workdir_init)
        self.deploy_dir = (
            self.settings.deploy_dir or f"{self.work_dir}/ecflow_suites/{suite_name}"
        )
        self.output_dir = self.settings.output_dir or f"{self.deploy_dir}/logs"
        self.suite = pf.Suite(
            name=suite_name,
            host=host,
            files=self.deploy_dir,
            out=self.output_dir,
            workdir=self.work_dir,
            defstatus=pf.state.suspended,
        )
        return self.suite

    def create_families(self):
        families = {}
        with self.suite:
            for f in ["make", "admin", "main", "clean"]:
                families[f] = pf.AnchorFamily(f)
            families["make"] >> families["main"] >> families["clean"]
        for rule in self.workflow.rules:
            if not rule.output:
                continue
            if rule.group:
                parent = families["main"]
                families[rule] = self.create_group_families(rule.group, parent)
            elif rule.has_wildcards():
                with families["main"]:
                    families[rule] = pf.AnchorFamily(pf.ecflow_name(rule.name))
            else:
                families[rule] = families["main"]
        self.families = families
        return families

    def create_group_families(
        self, group: str, parent: pf.AnchorFamily
    ) -> pf.AnchorFamily:
        for sf in pf.ecflow_name(group).split("/"):
            try:
                family = parent.find_node(sf)
            except KeyError:
                with parent:
                    family = pf.AnchorFamily(sf)
            parent = family
        return family

    def create_env_task(self):
        self.venv_dir = f"{self.deploy_dir}/venv"
        script = f"""
        {self.load_python}
        python3 -m virtualenv {self.venv_dir}
        source {self.venv_dir}/bin/activate
        pip install snakemake /home/dimw/src/snakemake-executor-plugin-ecflow
        """
        with self.families["make"]:
            env = pf.Task(
                name="setup_env",
                script=script,
                defstatus=pf.state.complete if self.settings.linked else pf.state.queued,
            )
        return env

    def create_evaluate_task(self):
        linked = self.settings.linked
        python = self.get_python_executable() if linked else self.venv_dir + "/bin/python"
        python = restore_common_linked_dirs(python)
        # make sure same settings are used as original setup
        ecfsets = {
            k: v for k, v in self.settings.get_items_by_category(None) if v is not None
        }
        ecfsets["deploy"] = "main"
        ecfsets.pop("execute", None)
        ecfsets.pop("linked", None)
        ecfsets_str = " \\\n    ".join(
            f"--ecflow-{k.replace('_', '-')} {v}" for k, v in ecfsets.items()
        )
        snakefile = self.snakefile if linked else self.deploy_dir + "/Snakefile"
        script = f"""
        {self.load_python}
        {self.load_ecflow}

        {python} -m snakemake -s {snakefile} -j 1 --executor ecflow \\
            {ecfsets_str} \\
            $SNAKEMAKE_TARGET
        """
        with self.families["admin"]:
            evaluate = pf.Task(
                name="evaluate",
                script=script,
                defstatus=pf.state.complete,
                SNAKEMAKE_TARGET=self.workflow.default_target,
            )
        return evaluate

    @property
    def snakefile_path(self) -> str:
        """This is needed because self.workflow.snakefile gives weird results."""
        sf = self.workflow.get_rule(self.workflow.default_target).snakefile
        return restore_common_linked_dirs(sf)

    def create_tasks_from_dag(self):
        tasks = {}
        for job in reversed(list(self.workflow.dag.get_jobs_or_groups())):
            if not job.output:
                continue
            # convert to group jobs if needed
            triggers = list(
                {
                    tasks[self.workflow.dag.get_job_group(j) if j.group else j]
                    for j in self.workflow.dag.dependencies[job]
                }
            )
            triggers = pf.all_complete(triggers) if len(triggers) > 1 else triggers
            with self.families[(next(iter(job.jobs)) if job.is_group() else job).rule]:
                tasks[job] = self.create_task(job, triggers=triggers)
        self.tasks = tasks
        return tasks

    def create_task(self, job: JobExecutorInterface, **kwargs) -> pf.Task:
        if job.is_group():
            dag_order = list(reversed(self.workflow.dag.jobs))
            jobs = sorted(job.jobs, key=lambda x: dag_order.index(x))
        else:
            jobs = [job]
        wildcards = {k: v for j in jobs for k, v in j.wildcards_dict.items()}
        wcname = "_".join([f"{k}_{v}" for k, v in wildcards.items()])
        name = job.name + ("_" + wcname if wcname else "")
        # rules without wildcards will be empty here, so we use the rule name
        name_cleaned = pf.ecflow_name(name)
        needsrun = any(j.dag.needrun(j) for j in jobs)
        import ipdb; ipdb.set_trace()  # noqa: E702
        task = pf.Task(
            name=name_cleaned,
            # submit_arguments=job.resources,
            script="\n\n".join([self.job_script(j) for j in jobs]),
            defstatus=pf.state.queued if needsrun else pf.state.complete,
            SNAKEMAKE_INPUT=" ".join([i for j in jobs for i in j.input]),
            SNAKEMAKE_OUTPUT=" ".join([o for j in jobs for o in j.output]),
            **kwargs,
        )
        return task

    def job_script(self, job) -> str:
        """Return the script to run for a job."""
        script = f"""
        mkdir -p $(dirname {" ".join(job.output)})

        {job.shellcmd}
        """
        return script

    def create_clean_task(self):
        """Create a task to clean the suite."""
        script = f"""
        ecflow_client --delete=force yes /{self.suite.name}
        rm -rf {self.deploy_dir} {self.output_dir}
        """
        with self.families["clean"]:
            clean = pf.Task(
                name="clean_suite",
                script=script,
                defstatus=pf.state.complete,
            )
        return clean

    def deploy(self):
        suite = self.suite
        print("Checking definition...")
        suite.check_definition()
        output_def_file = self.settings.output_def_file
        if output_def_file is not None:
            print("Writing definition file...")
            suite_def = suite.ecflow_definition()
            suite_def.check()
            suite_def.save_as_defs(output_def_file)
        deploy = self.settings.deploy
        if deploy:
            node = deploy if deploy != "suite" else None
            print(f"Deploying {deploy}...")
            suite.deploy_suite(node=node)
            if not self.settings.linked:
                print("Copying Snakefile to deploy directory...")
                shutil.copy(self.snakefile, self.deploy_dir + "/Snakefile")
        ecf_host = self.settings.host
        if ecf_host and deploy is not None:
            print("Uploading to server...")
            if deploy is not None and deploy != "suite":
                replace_family = suite.find_node(deploy)
                assert hasattr(
                    replace_family, "replace_on_server"
                ), f"Is {replace_family} really a family?"
            else:
                replace_family = suite
            print(f"Replace {replace_family.fullname} on {ecf_host}...")
            replace_family.replace_on_server(ecf_host, 3141)
        return

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        # If required, make sure to pass the job's id to the job_info object, as keyword
        # argument 'external_job_id'.
        if job in self.tasks:
            task = self.tasks[job]
            self.ecflow_client.run(task.fullname, False)

        job_info = SubmittedJobInfo(job)
        self.report_job_submission(job_info)
        return

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # If you provided it above, each will have its external_jobid set according
        # to the information you provided at submission time.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(active_job).
        # For jobs that have errored, you have to call
        # self.report_job_error(active_job).
        # This will also take care of providing a proper error message.
        # Usually there is no need to perform additional logging here.
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        #
        # To modify the time until the next call of this method,
        # you can set self.next_sleep_seconds here.
        for job in active_jobs:
            status = (
                self.ecflow_state(self.tasks[job.job])
                if job.job in self.tasks
                else "complete"
            )
            if status == "complete":
                self.report_job_success(job)
            elif status == "aborted":
                msg = f"ecFlow task '{job.job}' failed, status is: '{status}'. "
                self.report_job_error(job, msg=msg)
            else:
                yield job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        for job in active_jobs:
            task = self.tasks[job]
            self.ecflow_client.kill(task.fullname)
            self.report_job_cancel(job)


def restore_common_linked_dirs(path: str | Path) -> Path:
    """Make sure a path that is resolved through python is restored to common
    linked prefixes available as environment variables.
    If none of them work, return the original path.
    """
    path = Path(path)
    for cld in ["HOME", "PERM", "HPCPERM"]:
        cldp = Path(os.environ[cld])
        if path.is_relative_to(cldp.resolve()):
            return cldp / path.relative_to(cldp.resolve())
    return path
