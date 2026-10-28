import sys
import os
import re
import shutil
from pathlib import Path
from typing import List, Generator
from textwrap import dedent as dd

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_common.exceptions import WorkflowError  # noqa
from snakemake.io import is_flagged
from pyflow.importer import ecflow  # finds the bindings for this python via $ECFLOW_DIR
import pyflow as pf


def ecflow_name(name: str) -> str:
    """Make a string safe to use as an ecFlow node name.

    pf.ecflow_name only replaces "-", " " and "/", but wildcard values may
    contain anything, while ecFlow only accepts alphanumerics, "_" and "."
    (the latter not as the first character).
    """
    cleaned = re.sub(r"[^A-Za-z0-9_.]", "_", str(name))
    return "_" + cleaned[1:] if cleaned.startswith(".") else cleaned


class TokenInLimit(pf.InLimit):
    """An inlimit taking several tokens, which pf.InLimit cannot express."""

    def __init__(self, limit: pf.Limit, tokens: int = 1):
        super().__init__(limit)
        self.tokens = tokens

    def _build(self, ecflow_parent):
        path, name = self.value.fullname.split(":")
        ecflow_parent.add_inlimit(ecflow.InLimit(name, path, self.tokens))


class Executor(RemoteExecutor):

    load_python = "module load python3"
    load_ecflow = "module load ecflow"

    @property
    def load_snakemake(self) -> str:
        """Preamble for tasks that call snakemake, which loads this plugin."""
        return f"{self.load_python}\n{self.load_ecflow}"

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
        # sm changes into the set workdir but doesnt store it in the workflow object
        self.work_dir = restore_common_linked_dirs(self.workflow.workdir_init)
        # str() because the settings parse these into Path, which pyflow rejects
        self.deploy_dir = str(
            self.settings.deploy_dir or f"{self.work_dir}/ecflow_suites/{suite_name}"
        )
        self.output_dir = str(self.settings.output_dir or f"{self.deploy_dir}/logs")
        host = pf.TroikaHost(
            "%HOST%",
            user="$USER",
            extra_variables={"HOST": f"%SCHOST:hpc%"},
            server_ecfvars=False,
            # the suite out= arg is deprecated and clashes with the ECF_OUT the
            # host defines itself (pyflow >= 3.5)
            log_directory=self.output_dir,
        )
        suite = pf.Suite(
            name=suite_name,
            host=host,
            files=self.deploy_dir,
            workdir=self.work_dir,
            defstatus=pf.state.suspended,
        )
        return suite

    def create_standard_families(self):
        families = {}
        with self.suite:
            for f in ["make", "admin", "main", "clean"]:
                families[f] = pf.AnchorFamily(f)
            # clean holds the temp() cleanup tasks, which run alongside main
            families["make"] >> families["main"]
            families["make"] >> families["clean"]
        return families

    @property
    def venv_dir(self) -> str:
        """Environment the suite uses when it is not linked to the project."""
        return f"{self.deploy_dir}/venv"

    def create_env_task(self):
        script = dd(f"""
        {self.load_python}
        virtualenv {self.venv_dir}
        source {self.venv_dir}/bin/activate
        pip install snakemake
        pip install {self.settings.plugin_source}
        """)
        with self.families["make"]:
            env = pf.Task(
                name="setup_snakemake_env",
                script=script,
                defstatus=pf.state.complete if self.settings.linked else pf.state.queued,
            )
        return env

    def get_python_executable(self) -> str:
        """Python the suite tasks call snakemake with.

        Either the one this process runs in (linked) or the one the env task
        builds in the deploy directory (unlinked).
        """
        if not self.settings.linked:
            return f"{self.venv_dir}/bin/python"
        return str(restore_common_linked_dirs(super().get_python_executable()))

    def get_snakefile(self) -> str:
        """Snakefile the suite tasks refer to, see get_python_executable."""
        if not self.settings.linked:
            return f"{self.deploy_dir}/Snakefile"
        return str(restore_common_linked_dirs(self.snakefile))

    def get_job_args(self, job: JobExecutorInterface, **kwargs) -> str:
        """Skip RemoteExecutor's --wait-for-files.

        Those point at this process' temporary directory, which is long gone
        by the time the deployed suite runs the task.
        """
        return super(RemoteExecutor, self).get_job_args(job, **kwargs)

    def additional_general_args(self) -> str:
        """Repeat args that general_args() derives from the building process.

        They are appended after general_args, so argparse lets them win.
        """
        if self.settings.linked:
            return ""
        # general_args points this at the venv snakemake is currently run from
        return f"--scheduler-solver-path {self.venv_dir}/bin"

    @property
    def suite_targets(self) -> str:
        """The targets the suite was built for, as snakemake was asked for them.

        Not workflow.default_target: a suite built for `monthly` must rebuild
        itself for `monthly`, not for whatever rule happens to come first in
        the Snakefile. Falls back to the default target if the settings object
        ever stops carrying them.
        """
        settings = getattr(self.workflow, "dag_settings", None)
        targets = sorted(getattr(settings, "targets", None) or [])
        return " ".join(targets) or self.workflow.default_target

    def rebuild_command(self, target: str, **overrides) -> str:
        """A snakemake call that rebuilds the suite with this run's settings.

        Used by the evaluate task and by every checkpoint, which has to
        re-expand its own part of the DAG once it knows its output. Settings
        given here override the ones this process was invoked with; pass None
        to drop one.
        """
        # make sure same settings are used as original setup
        ecfsets = {
            k: v for k, v in self.settings.get_items_by_category(None) if v is not None
        }
        ecfsets.pop("execute", None)
        ecfsets.pop("linked", None)
        ecfsets.update(overrides)
        parts = [
            f"{self.get_python_executable()} -m snakemake"
            f" -s {self.get_snakefile()} -j 1 --executor ecflow"
        ]
        parts += [
            f"--ecflow-{k.replace('_', '-')} {v}"
            for k, v in ecfsets.items()
            if v is not None
        ]
        parts.append(target)
        return " \\\n    ".join(parts) + "\n"

    def create_evaluate_task(self):
        script = dd(f"""
        {self.load_python}
        {self.load_ecflow}
        """) + self.rebuild_command("$TARGET", deploy="main", deploy_downstream=None)
        with self.families["admin"]:
            evaluate = pf.Task(
                name="evaluate",
                script=script,
                defstatus=pf.state.complete,
                TARGET=self.suite_targets,
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
        # global task limit and one limit per --resources entry (other than
        # snakemake's own _cores/_nodes), attached to the admin family
        with self.families["admin"]:
            self.task_limit = pf.Limit("task_limit", self.workflow.nodes)
            self.resource_limits = {
                name: pf.Limit(ecflow_name(name), int(value))
                for name, value in self.workflow.global_resources.items()
                if not name.startswith("_") and value is not None
            }
        # create tasks first
        for job in self.jobs_and_groups:
            self.families[job], tasks[job] = self.create_task(job)
        # assign triggers
        for job, task in tasks.items():
            deps = dag.dependencies[job]
            if deps:
                nodes = {tasks[self.groups[j] if j.group else j] for j in deps}
                task.triggers = self.build_trigger(nodes)
        self.cleanup_tasks = self.create_cleanup_tasks(tasks)
        return tasks

    def inlimits(self, jobs: list) -> list:
        """The task limit plus a limit per resource the jobs request, taking as
        many tokens as the most demanding job, as snakemake would."""
        limits = [self.task_limit]
        for name, limit in self.resource_limits.items():
            tokens = max((j.resources.get(name) or 0 for j in jobs), default=0)
            if tokens:
                limits.append(TokenInLimit(limit, int(tokens)))
        return limits

    @staticmethod
    def temp_outputs(job: JobExecutorInterface) -> list:
        return [f for f in job.output if is_flagged(f, "temp")]

    def create_cleanup_tasks(self, tasks: dict) -> dict:
        """job -> task removing the job's temp() outputs once every task
        consuming them is complete.

        The inlined shell scripts bypass snakemake, which would otherwise
        delete temp files after their last consumer. The cleanup task goes in
        the job's family path under clean/ rather than main/, so a trigger on a
        family in main does not wait for the cleanup (which waits for it).
        """
        dag = self.workflow.dag
        cleanup = {}
        for job, task in tasks.items():
            jobs = sorted(job.jobs, key=lambda x: self.jobs.index(x)) if job.is_group() else [job]
            temp = [f for j in jobs for f in self.temp_outputs(j)]
            # target rules without output (e.g. all) get no task
            keys = {self.groups[c] if c.group else c for j in jobs for c in dag.depending[j]}
            consumers = {tasks[k] for k in keys if k in tasks}
            consumers.discard(task)
            if not temp or not consumers:
                continue
            with self.create_job_family(job, self.families["clean"]):
                cleanup[job] = pf.Task(
                    name=task.name + "_cleanup",
                    script="rm -rf " + " ".join(temp),
                    triggers=self.build_trigger(consumers),
                    # also when the producer is done but its temp files are still there
                    defstatus=pf.state.queued
                    if any(dag.needrun(j) for j in jobs) or any(os.path.exists(f) for f in temp)
                    else pf.state.complete,
                )
        return cleanup

    @staticmethod
    def and_all(nodes: list):
        """AND nodes together as a balanced tree rather than a chain.

        pf.all_complete reduces left to right, so n dependencies nest n deep
        and pyflow's simplify() recurses once per level. A fan-in of a few
        thousand jobs then dies on Python's recursion limit. Pairing them up
        makes the nesting log2(n) instead.
        """
        if not nodes:
            raise ValueError("cannot wait on an empty list of nodes")
        while len(nodes) > 1:
            nodes = [
                nodes[i] & nodes[i + 1] if i + 1 < len(nodes) else nodes[i]
                for i in range(0, len(nodes), 2)
            ]
        return nodes[0]

    @property
    def family_sizes(self) -> dict:
        """id(family) -> number of tasks under it, for the whole suite.

        Keyed on id() rather than on the node: pyflow overloads == to build
        trigger expressions, so a dict keyed on nodes would match on any hash
        collision.
        """
        if not hasattr(self, "_family_sizes"):
            sizes = {}

            def walk(node) -> int:
                if isinstance(node, pf.Task):
                    return 1
                if not hasattr(node, "executable_children"):
                    return 0  # a label, variable or limit
                n = sum(walk(c) for c in node.executable_children)
                sizes[id(node)] = n
                return n

            for root in self.suite.executable_children:
                walk(root)
            self._family_sizes = sizes
        return self._family_sizes

    def cover_nodes(self, nodes: set) -> list:
        """The smallest set of nodes covering exactly the given tasks.

        A family whose every task is in the set stands in for those tasks, so a
        dependency on all of a rule's jobs, or on all of one wildcard's, becomes
        a single family. Triggers and partial deployment both want this: waiting
        on, or replacing, one family beats naming its several thousand tasks.

        Counted upwards from the given tasks rather than by walking the suite,
        which would be quadratic over the whole DAG.
        """
        sizes = self.family_sizes
        covered = {}
        for task in nodes:
            parent = getattr(task, "parent", None)
            while parent is not None and id(parent) in sizes:
                covered[id(parent)] = covered.get(id(parent), 0) + 1
                parent = getattr(parent, "parent", None)

        keep, seen = [], set()
        for task in nodes:
            best, parent = task, getattr(task, "parent", None)
            # climb while the whole family is wanted; the suite is not in sizes,
            # so this stops below it
            while (parent is not None and id(parent) in sizes
                   and covered.get(id(parent)) == sizes[id(parent)]):
                best = parent
                parent = getattr(parent, "parent", None)
            if id(best) not in seen:
                seen.add(id(best))
                keep.append(best)
        keep.sort(key=lambda n: n.fullname)
        return keep

    def build_trigger(self, nodes: set):
        """Trigger expression for a set of dependency tasks."""
        keep = self.cover_nodes(nodes)
        return keep[0] if len(keep) == 1 else self.and_all(keep)

    def downstream_jobs(self, rule: str) -> set:
        """Every job that depends, directly or not, on a job of this rule.

        The rule's own jobs are excluded: a checkpoint must not replace the
        family holding the task that is doing the replacing.
        """
        dag = self.workflow.dag
        seen, frontier = set(), [j for j in dag.jobs if j.rule.name == rule]
        if not frontier:
            raise WorkflowError(f"no job of rule {rule} is in the DAG")
        own = set(frontier)
        while frontier:
            job = frontier.pop()
            for dep in dag.depending[job]:
                if dep not in seen:
                    seen.add(dep)
                    frontier.append(dep)
        return seen - own

    def downstream_nodes(self, rule: str) -> list:
        """The nodes to replace so that one rule's dependents are rebuilt."""
        jobs = self.downstream_jobs(rule)
        tasks = set()
        for job in jobs:
            key = self.groups[job] if job.group else job
            if key in self.tasks:
                tasks.add(self.tasks[key])
            if key in self.cleanup_tasks:
                tasks.add(self.cleanup_tasks[key])
        if not tasks:
            return []
        return self.cover_nodes(tasks)

    def checkpoint_rules(self) -> list:
        """Names of the checkpoint rules with a job in the DAG."""
        seen = []
        for job in self.workflow.dag.jobs:
            if getattr(job.rule, "is_checkpoint", False) and job.rule.name not in seen:
                seen.append(job.rule.name)
        return seen

    def checkpoint_rebuild_script(self, rule: str) -> str:
        """Tail appended to a checkpoint task, re-expanding its own subtree.

        A checkpoint's dependents cannot be enumerated until it has run, so the
        suite is built without them and the task calls snakemake back once its
        output exists. Only the nodes downstream of this rule are replaced, so
        the rest of the suite -- including the family this task is running in --
        is left alone.
        """
        return dd(f"""
        # --- re-expand the part of the suite that depends on this checkpoint
        {self.load_python}
        {self.load_ecflow}
        """) + self.rebuild_command(
            self.suite_targets, deploy=None, deploy_downstream=rule
        )

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
        name_cleaned = ecflow_name(name)
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

        # A checkpoint only learns its dependents once it has run, so it
        # re-expands its own part of the suite before reporting complete.
        checkpoints = [j.rule.name for j in jobs
                       if getattr(j.rule, "is_checkpoint", False)]
        for rule in dict.fromkeys(checkpoints):
            script += "\n\n" + self.checkpoint_rebuild_script(rule)

        kwargs.update(variables)
        kwargs.update(ecflow_params)
        kwargs["inlimits"] = self.inlimits(jobs)

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

    def create_job_family(
            self, job: JobExecutorInterface, parent: pf.AnchorFamily = None) -> pf.AnchorFamily:
        """The job's family under main, or the same family path under another
        parent (where rules without wildcards get a family of their own)."""
        main = self.families["main"]
        parent = main if parent is None else parent
        if job.is_group():
            family = self.create_group_families(job.groupid, parent, standard=parent is main)
        elif job.wildcards or parent is not main:
            family = self.create_wildcard_families(job, parent)
        else:
            family = main
        return family

    def create_group_families(
            self, group: str, parent: pf.AnchorFamily, standard: bool = True) -> pf.AnchorFamily:
        parts = group.split("/")
        family = parent
        # first check if any of the standard families match
        if standard:
            try:
                parent = family = self.suite.find_node(ecflow_name(parts[0]))
                parts = parts[1:]
            except KeyError:
                pass

        for sf in parts:
            sf = ecflow_name(sf)
            try:
                family = parent.find_node(sf)
            except KeyError:
                with parent:
                    family = pf.AnchorFamily(sf)
            parent = family
        return family

    def create_wildcard_families(
            self, job: JobExecutorInterface, parent: pf.AnchorFamily) -> pf.AnchorFamily:
        # take all wildcards except the last one
        wc = [k+"_"+v for k, v in list(job.wildcards_dict.items())][:-1]
        for n in [job.rule.name] + wc:
            fname = ecflow_name(n)
            try:
                family = parent.find_node(fname)
            except KeyError:
                with parent:
                    family = pf.AnchorFamily(fname)
            parent = family
        return family

    def translate_resources(self, job: JobExecutorInterface) -> dict:
        """Translate job resources to ecFlow resources."""
        accepted_resources = {
            # sm job.resources keys to troika resources
            "time": "time",
            "mem": "memory_per_cpu",
            "mem_mb": "memory_per_cpu",
            "total_tasks": "total_tasks",
            "_cores": "cpus_per_task",
            "runtime": "time",
            "cpus": "cpus_per_task",
            "_nodes": "total_nodes",
            "nodes": "total_nodes",
            "ntasks": "ntasks",
            "tasks": "ntasks",
            #"tmpdir": "tmpdir",
        }
        resources = {}
        for k, v in job.resources.items():
            if k in accepted_resources and job.resources.get(k) is not None:
                resources[accepted_resources[k]] = v
        return resources

    def job_script(self, job: JobExecutorInterface, order=None) -> str:
        """Return the script to run for a job.

        Rules with a shell directive are inlined into the task script. Anything
        else (run:, script:, notebook:, wrapper: or a rule without any code,
        e.g. touch() output) needs the snakemake runtime, so the task calls
        snakemake back for exactly this job instead.
        """
        if job.shellcmd is None:
            return f"{self.load_snakemake}\n\n{self.format_job_exec(job)}"

        lines = []

        if job.env_modules:
            lines.append(job.env_modules.shellcmd(""))

        if len(job.output) > 0:
            lines.append(f"rm -rf $OUTPUT")

        # snakemake creates the directories of outputs and logs before a job
        dirs = " ".join(sorted({d for d in map(os.path.dirname, [*job.output, *job.log]) if d}))
        if dirs:
            lines.append(f"mkdir -p {dirs}")

        # like snakemake, remove the outputs of a failed job, so a partial
        # output (e.g. a directory created before the command failed) is not
        # taken as done when the suite is rebuilt
        ostr = f"_{order}" if order is not None else ""
        if len(job.output) > 0:
            lines.append(f"trap 'rm -rf $OUTPUT{ostr}' ERR")
        lines.append(job.shellcmd)
        if len(job.output) > 0:
            lines.append("trap - ERR")

        # and touches touch() outputs after it
        touch = [f for f in job.output if is_flagged(f, "touch")]
        if touch:
            lines.append("touch " + " ".join(touch))
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
            rm -rf $OUTPUT{ostr}
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

    def node_path(self, node) -> str:
        """Suite-relative path of a node, the form find_node expects."""
        prefix = f"/{self.suite.name}/"
        name = node.fullname
        return name[len(prefix):] if name.startswith(prefix) else name

    @property
    def deploy_nodes(self) -> list:
        """The nodes to deploy scripts for and replace on the server.

        Either named with --ecflow-deploy or, for a checkpoint re-expanding
        itself, derived from --ecflow-deploy-downstream.
        """
        if self.settings.deploy_downstream:
            return self.downstream_nodes(self.settings.deploy_downstream)
        deploy = self.settings.deploy
        if not deploy:
            return []
        if deploy == "suite":
            return [self.suite]
        return [self.suite.find_node(n.strip()) for n in deploy.split(",")]

    def deploy(self):
        print("Checking suite definition...")
        self.suite.check_definition()
        if self.settings.output_def_file is not None:
            self.write_def_file()
        nodes = self.deploy_nodes
        if not nodes:
            if self.settings.deploy_downstream:
                print(f"Nothing downstream of {self.settings.deploy_downstream}.")
            return
        self.deploy_scripts(nodes)
        if self.settings.host:
            self.upload_suite(nodes)

    def write_def_file(self):
        print("Writing definition file...")
        suite_def = self.suite.ecflow_definition()
        suite_def.check()
        suite_def.save_as_defs(self.settings.output_def_file)

    def deploy_scripts(self, nodes: list):
        """Write out the scripts of exactly the tasks under the given nodes.

        Suite.deploy_suite only takes one node and walks its all_tasks, which a
        Task does not have, so deploying a single task would mean falling back
        to its whole family -- for a task sitting directly in main that is the
        entire suite, redeployed on every checkpoint. This does the same work
        per node instead.
        """
        from pyflow.deployment import FileSystem

        target = FileSystem(self.suite)
        tasks, families = {}, {}
        for node in nodes:
            if isinstance(node, pf.Task):
                tasks[id(node)] = node
            else:
                tasks.update({id(t): t for t in node.all_tasks})
                families.update({id(f): f for f in node.all_families})
        for task in tasks.values():
            script, includes = task.generate_script()
            target.deploy_task(task.deploy_path, script, includes)
        for family in families.values():
            manual = self.suite.generate_stub(family.manual)
            if manual:
                target.deploy_manual(family.manual_path, manual)
        target.deploy_headers()
        print(f"Deployed {len(tasks)} task scripts.")
        if not self.settings.linked:
            print("Copying Snakefile to deploy directory...")
            shutil.copy(self.snakefile, self.deploy_dir + "/Snakefile")

    def upload_suite(self, nodes: list):
        """Replace the given nodes on the ecFlow server."""
        ecf_host = self.settings.host
        print("Uploading to server...")
        for node in nodes:
            print(f"Replace {node.fullname} on {ecf_host}...")
            node.replace_on_server(ecf_host, 3141)
        # begin suite if not already done. Identity, not `in`: pyflow nodes
        # overload == to build trigger expressions, so `node in list` is always
        # truthy.
        if any(n is self.suite for n in nodes) and self.ecflow_state(self.suite) == "unknown":
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
