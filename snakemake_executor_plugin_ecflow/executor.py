import sys
import os
import shutil
from pathlib import Path
from typing import List, Generator, Optional
from textwrap import dedent as dd

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

        self.suite = self.create_suite()
        self.families = self.create_standard_families()
        self.env_task = self.create_env_task()
        self.evaluate_task = self.create_evaluate_task()
        self.clean_task = self.create_clean_task()
        self.tasks = self.create_tasks_from_dag()

        self.deploy()

        if not self.settings.execute:
            sys.exit(0)
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
        suite = pf.Suite(
            name=suite_name,
            host=host,
            files=self.deploy_dir,
            out=self.output_dir,
            workdir=self.work_dir,
            defstatus=pf.state.suspended,
        )
        return suite

    def create_standard_families(self):
        families = {}
        with self.suite:
            for f in ["make", "admin", "main", "clean"]:
                families[f] = pf.AnchorFamily(f)
            families["make"] >> families["main"] >> families["clean"]
        return families

    def create_env_task(self):
        self.venv_dir = f"{self.deploy_dir}/venv"
        script = dd(f"""
        {self.load_python}
        virtualenv {self.venv_dir}
        source {self.venv_dir}/bin/activate
        pip install snakemake
        pip install git+https://github.com/mwort/snakemake-executor-plugin-ecflow.git
        """)
        with self.families["make"]:
            env = pf.Task(
                name="setup_snakemake_env",
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
        script = dd(f"""
        {self.load_python}
        {self.load_ecflow}

        {python} -m snakemake -s {snakefile} -j 1 --executor ecflow \\
            {ecfsets_str} \\
            $TARGET
        """)
        with self.families["admin"]:
            evaluate = pf.Task(
                name="evaluate",
                script=script,
                defstatus=pf.state.complete,
                TARGET=self.workflow.default_target,
            )
        return evaluate

    @property
    def snakefile_path(self) -> str:
        """This is needed because self.workflow.snakefile gives weird results."""
        sf = self.workflow.get_rule(self.workflow.default_target).snakefile
        return restore_common_linked_dirs(sf)

    def create_tasks_from_dag(self):
        tasks = {}
        dag = self.workflow.dag
        self.jobs_and_groups, self.jobs, self.groups = self.get_jobs_and_groups()
        # global task limit attached to the admin family
        tlimit = pf.Limit("task_limit", self.workflow.nodes)
        self.families["admin"].limits = tlimit
        # create tasks first
        for job in self.jobs_and_groups:
            self.families[job], tasks[job] = self.create_task(job, inlimits=tlimit)
        # assign triggers
        for job, task in tasks.items():
            deps = dag.dependencies[job]
            if deps:
                triggers = list({tasks[self.groups[j] if j.group else j] for j in deps})
                triggers = pf.all_complete(triggers) if len(triggers) > 1 else triggers
                task.triggers = triggers
        return tasks

    def get_jobs_and_groups(self):
        # groups in SM are based on jobs that need to run, here we want groups
        # irrgardless of whether they need to run or not so we need to hack the
        # update_groups method to create groups for all jobs (as if sm was called with -F)
        dag = self.workflow.dag
        needrun, dag_groups = dag._needrun, dag._group
        dag._needrun = set(dag.jobs)
        dag.update_groups()
        groups_dict = dag._group
        # restore original needrun and groups
        dag._needrun = needrun
        dag._group = dag_groups

        jobs = []
        groups_seen = set()
        sorted_jobs_or_groups = []

        for layer in self.workflow.dag.toposorted():
            for job in sorted(layer, key=lambda j: j.wildcards):
                if not job.output:
                    continue
                if job.group is None:
                    sorted_jobs_or_groups.append(job)
                else:
                    group = groups_dict[job]
                    if group not in groups_seen:
                        sorted_jobs_or_groups.append(group)
                        groups_seen.add(group)
                jobs.append(job)
        return sorted_jobs_or_groups, jobs, groups_dict

    def create_task(self, job: JobExecutorInterface, **kwargs) -> pf.Task:
        if job.is_group():
            jobs = sorted(job.jobs, key=lambda x: self.jobs.index(x))
        else:
            jobs = [job]
        wildcards = {k: v for j in jobs for k, v in j.wildcards_dict.items()}
        wcname = "_".join([f"{k}_{v}" for k, v in wildcards.items()])
        name = job.name + ("_" + wcname if wcname else "")
        # rules without wildcards will be empty here, so we use the rule name
        name_cleaned = pf.ecflow_name(name)
        needsrun = any(j.dag.needrun(j) for j in jobs)
        variables = {k.upper(): v for k, v in wildcards.items()}

        if job.is_group():
            variables.update({f"INPUT_{i}": str(j.input) for i, j in enumerate(jobs)})
            variables.update({f"OUTPUT_{i}": str(j.output) for i, j in enumerate(jobs)})
            script = "\n\n".join([self.job_script(j, order=i) for i, j in enumerate(jobs)])
            ecfp = (getattr(j.params, "ecflow", None) for j in jobs)
            ecflow_params = {k: v for d in ecfp if d is not None for k, v in d.items()}
        else:
            variables.update({f"INPUT": str(job.input)})
            variables.update({f"OUTPUT": str(job.output)})
            script = self.job_script(job)
            ecflow_params = getattr(job.params, "ecflow", {})

        kwargs.update(variables)
        kwargs.update(ecflow_params)

        # take the max of resources for all jobs
        sargs = [self.translate_resources(j) for j in jobs]
        resources = {k: max(d[k] for d in sargs if k in d) for k in {k for d in sargs for k in d}}

        family = self.create_job_family(job)
        with family:
            task = pf.Task(
                name=name_cleaned,
                submit_arguments=resources,
                script=script,
                defstatus=pf.state.queued if needsrun else pf.state.complete,
                **kwargs,
            )
        return family, task

    def create_job_family(self, job: JobExecutorInterface) -> pf.AnchorFamily:
        main = self.families["main"]
        if job.is_group():
            family = self.create_group_families(job.groupid, main)
        elif job.wildcards:
            family = self.create_wildcard_families(job, main)
        else:
            family = main
        return family

    def create_group_families(self, group: str, parent: pf.AnchorFamily) -> pf.AnchorFamily:
        parts = group.split("/")
        # first check if any of the standard families match
        try:
            parent = family = self.suite.find_node(pf.ecflow_name(parts[0]))
            parts = parts[1:]
        except KeyError:
            pass

        for sf in parts:
            sf = pf.ecflow_name(sf)
            try:
                family = parent.find_node(sf)
            except KeyError:
                with parent:
                    family = pf.AnchorFamily(sf)
            parent = family
        return family

    def create_wildcard_families(
            self, job: JobExecutorInterface, parent: pf.AnchorFamily) -> pf.AnchorFamily:
        fname = pf.ecflow_name(job.rule.name)
        try:
            family = parent.find_node(fname)
        except KeyError:
            with parent:
                family = pf.AnchorFamily(fname)
        return family

    def translate_resources(self, job: JobExecutorInterface) -> dict:
        """Translate job resources to ecFlow resources."""
        accepted_resources = {
            # sm job.resources keys to troika resources
            "time": "time",
            "mem": "memory_per_cpu",
            "total_tasks": "total_tasks",
            "_cores": "cpus_per_task",
            "runtime": "time",
            "cpus": "cpus_per_task",
            #"tmpdir": "tmpdir",
        }
        resources = {v: job.resources.get(k) for k, v in accepted_resources.items()}
        return {k: v for k, v in resources.items() if v is not None}

    def job_script(self, job, order=None) -> str:
        """Return the script to run for a job."""
        lines = []

        if job.env_modules:
            lines.append(job.env_modules.shellcmd(""))

        dirs = " ".join([d for d in map(os.path.dirname, job.output) if d])
        if dirs:
            lines.append(f"mkdir -p {dirs}")

        lines.append(job.shellcmd)
        lines.append(self.io_check_script(job, order=order))

        return "\n\n".join(lines)

    def io_check_script(self, job: JobExecutorInterface, order=None) -> str:
        ostr = f"_{order}" if order is not None else ""
        has_i = bool(job.input)
        script = dd(f"""
        n_missing=0
        n_outdated=0
        {f'last_input=$(stat -c %Y $INPUT{ostr} | sort -n | tail -n1)' if has_i else ''}
        for f in $OUTPUT{ostr}; do
            [ ! -e "$f" ] && echo "Missing: $f" && n_missing=$((n_missing+1))
            {'[ -e "$f" ] && [ $(stat -c %Y "$f") -lt $last_input ] && echo "Outdated: $f" && n_outdated=$((n_outdated+1))' if has_i else ''}
        done

        if [[ ! $n_missing -eq 0 || ! $n_outdated -eq 0 ]]; then
            [[ ! $n_missing -eq 0 ]] && echo $n_missing " files or directories are missing."
            {'[[ ! $n_outdated -eq 0 ]] && echo $n_outdated " files or directories are outdated."' if has_i else ''}
            exit 1
        fi
        """)
        return script

    def create_clean_task(self):
        """Create a task to clean the suite."""
        script = dd(f"""
        ecflow_client --delete=force yes /{self.suite.name}
        rm -rf {self.deploy_dir} {self.output_dir}
        """)
        with self.families["clean"]:
            clean = pf.Task(
                name="clean_suite",
                script=script,
                defstatus=pf.state.complete,
            )
        return clean

    def deploy(self):
        print("Checking suite definition...")
        self.suite.check_definition()
        if self.settings.output_def_file is not None:
            self.write_def_file()
        if self.settings.deploy:
            self.deploy_scripts()
        if self.settings.host and self.settings.deploy is not None:
            self.upload_suite()

    def write_def_file(self):
        print("Writing definition file...")
        suite_def = self.suite.ecflow_definition()
        suite_def.check()
        suite_def.save_as_defs(self.settings.output_def_file)

    def deploy_scripts(self):
        deploy = self.settings.deploy
        node = deploy if deploy != "suite" else None
        print(f"Deploying {deploy}...")
        self.suite.deploy_suite(node=node)
        if not self.settings.linked:
            print("Copying Snakefile to deploy directory...")
            shutil.copy(self.snakefile, self.deploy_dir + "/Snakefile")

    def upload_suite(self, deploy: Optional[str] = None):
        """Upload the suite to the ecFlow server."""
        suite, deploy, ecf_host = self.suite, self.settings.deploy, self.settings.host
        print("Uploading to server...")
        if deploy != "suite":
            replace_family = suite.find_node(deploy)
            assert hasattr(
                replace_family, "replace_on_server"
            ), f"Is {replace_family} really a family?"
        else:
            replace_family = suite
        print(f"Replace {replace_family.fullname} on {ecf_host}...")
        replace_family.replace_on_server(ecf_host, 3141)
        # begin suite if not already done
        if deploy == "suite" and self.ecflow_state(suite) == "unknown":
            self.ecflow_client.begin_suite("/" + self.suite.name)
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
