import sys
import os
import re
import shutil
from pathlib import Path
from typing import List, Generator
from textwrap import dedent as dd

from tqdm import tqdm

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_common.exceptions import WorkflowError  # noqa
from snakemake.io import is_flagged
from snakemake.common.tbdstring import TBDString
from snakemake.utils import format as sm_format
from pyflow.importer import ecflow  # finds the bindings for this python via $ECFLOW_DIR
import pyflow as pf

# fewer items than this are quick, so get no progress bar
PROGRESS_MIN = 1000


def progress(items, desc: str):
    """Iterate over items with a progress bar when there are many of them,
    and only in a terminal (disable=None)."""
    return tqdm(items, desc=desc, unit="task", disable=None if len(items) >= PROGRESS_MIN else True)


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


class EcfVar:
    """Stand-in for a job's input, output, params, ... when formatting a shell
    command, so that it reads ${INPUT_REF} instead of the value.

    Attribute and index access derive a sub-variable, {input.ref} -> INPUT_REF
    and {input[1]} -> INPUT_1, and every variable formatted is recorded with
    the path to its value on the job.
    """

    # stands in for ${NAME} until quoting is sorted out, see Executor.shell_command
    marker = "\x00{}\x00"

    def __init__(self, name: str, path: tuple, used: dict):
        self._name, self._path, self._used = name, path, used

    def _child(self, key, step):
        name = f"{self._name}_{key}" if self._name else str(key)
        return EcfVar(name.upper(), self._path + (step,), self._used)

    def __getattr__(self, attr):
        if attr.startswith("__"):
            raise AttributeError(attr)
        return self._child(attr, ("attr", attr))

    def __getitem__(self, key):
        return self._child(key, ("item", key))

    def __format__(self, spec):
        # anything else (e.g. {threads:02d}) needs the value itself
        if spec not in ("", "q", "u"):
            raise ValueError(f"format spec {spec!r}")
        name = self._name or "WILDCARDS"
        self._used[name] = self._path
        return self.marker.format(name)


class Executor(RemoteExecutor):

    load_python = "module load python3"
    load_ecflow = "module load ecflow"
    meter_name = "done"

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
        self.monitor_task = self.create_monitor_task()
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
        # --nolock: a rebuild only deploys, it writes no outputs, and several
        # checkpoint tasks of one suite can rebuild at the same time
        parts = [
            f"{self.get_python_executable()} -m snakemake"
            f" -s {self.get_snakefile()} -j 1 --nolock --executor ecflow"
        ]
        parts += self.profile_args
        parts += [
            f"--ecflow-{k.replace('_', '-')} {v}"
            for k, v in ecfsets.items()
            if v is not None
        ]
        parts.append(target)
        return " \\\n    ".join(parts) + "\n"

    @property
    def profile_args(self) -> list:
        """The profiles the suite was built with, for the snakemake calls of its
        tasks (rebuild_command): --profile/--workflow-profile as given, made
        absolute, and $SNAKEMAKE_PROFILE, which the batch jobs don't inherit.

        Only a workflow profile in profiles/default is found without them. The
        rest of the command line is not repeated: the targets, -n or --forcerun
        must not apply to every rebuild, and the profile is where settings belong.
        """
        def absolute(path):
            return str(restore_common_linked_dirs(Path(self.work_dir, path)))

        args, argv = [], sys.argv[1:]
        for i, arg in enumerate(argv):
            for opt in ("--profile", "--workflow-profile"):
                if arg == opt and i + 1 < len(argv):
                    args.append(f"{opt} {absolute(argv[i + 1])}")
                elif arg.startswith(opt + "="):
                    args.append(f"{opt} {absolute(arg.split('=', 1)[1])}")
        if os.environ.get("SNAKEMAKE_PROFILE") and not any(a.startswith("--profile ") for a in args):
            args.append(f"--profile {absolute(os.environ['SNAKEMAKE_PROFILE'])}")
        return args

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
        self.done_tasks = []  # tasks that need not run, see set_done
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
        # rule -> [family, n tasks, n complete] for the rules getting a meter
        counts = {}
        for job in self.jobs_and_groups:
            if not job.is_group() and job.wildcards:
                counts[job.rule.name] = counts.get(job.rule.name, 0) + 1
        addmeter = self.settings.addmeter
        self.meters = {
            rule: [None, n, 0] for rule, n in counts.items() if addmeter and n >= addmeter
        }
        # rule -> (script, resources) of its first job, see create_task
        self.rule_scripts = {}
        # id(task) -> path of the script it shares, see share_script
        self.shared_scripts = {}
        # create tasks first
        for job in progress(self.jobs_and_groups, "Creating tasks"):
            self.families[job], tasks[job] = self.create_task(job)
        for rule, meter in self.meters.items():
            meter[0] = self.families["main"].find_node(ecflow_name(rule))
            with meter[0]:
                pf.Meter(self.meter_name, 0, meter[1])
        # assign triggers
        for job, task in tasks.items():
            deps = dag.dependencies[job]
            if deps:
                nodes = {tasks[self.groups[j] if j.group else j] for j in deps}
                task.triggers = self.build_trigger(nodes)
        self.cleanup_tasks = self.create_cleanup_tasks(tasks)
        # family_sizes was cached before the clean families existed, without
        # them cover_nodes lists every cleanup task of a checkpoint's dependents
        self.__dict__.pop("_family_sizes", None)
        self.add_checkpoint_deploy_nodes(tasks)
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
                    script="rm -rf $TEMP",
                    TEMP=" ".join(temp),
                    triggers=self.build_trigger(consumers),
                    # a batch job like any other: without the limit, a backlog of
                    # them is submitted at once, past the scheduler's submit limit
                    inlimits=[self.task_limit],
                )
            # also to run when the producer is done but its temp files are still there
            if not any(dag.needrun(j) for j in jobs) and not any(os.path.exists(f) for f in temp):
                self.done_tasks.append(cleanup[job])
            if not job.is_group() and job.wildcards:
                self.share_script(cleanup[job], self.families["clean"], job.rule.name, "_cleanup")
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

    def downstream_jobs(self, jobs: list) -> set:
        """Every job that depends, directly or not, on one of these jobs.

        Not on every job of their rule: a checkpoint rule with a job per
        wildcard (e.g. per dataset) must only replace what its own job feeds.
        The jobs themselves are excluded: a checkpoint must not replace the
        family holding the task that is doing the replacing.
        """
        dag = self.workflow.dag
        frontier = list(jobs)
        seen, own = set(), set(frontier)
        while frontier:
            job = frontier.pop()
            for dep in dag.depending[job]:
                if dep not in seen:
                    seen.add(dep)
                    frontier.append(dep)
        return seen - own

    def downstream_nodes(self, jobs: list, tasks: dict) -> list:
        """The nodes to replace so that the dependents of these jobs are rebuilt."""
        jobs = self.downstream_jobs(jobs)
        nodes = set()
        for job in jobs:
            key = self.groups[job] if job.group else job
            if key in tasks:
                nodes.add(tasks[key])
            if key in self.cleanup_tasks:
                nodes.add(self.cleanup_tasks[key])
        tasks = nodes
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

    def add_checkpoint_deploy_nodes(self, tasks: dict):
        """Give each checkpoint task the nodes its rebuild has to replace.

        Worked out here rather than in the rebuild: once a checkpoint's output
        exists, snakemake resolves its dependents' input functions without it
        and leaves the checkpoint out of the DAG, so only this build still
        knows what depends on it.
        """
        for job, task in tasks.items():
            jobs = job.jobs if job.is_group() else [job]
            checkpoints = [j for j in jobs if getattr(j.rule, "is_checkpoint", False)]
            if not checkpoints:
                continue
            nodes = self.downstream_nodes(checkpoints, tasks)
            paths = ",".join(dict.fromkeys(self.node_path(n) for n in nodes))
            # exported into the job, where one variable may not exceed 128 KiB
            if len(paths) > 100_000:
                print(f"Warning: {task.fullname} has to redeploy {len(nodes)} nodes, too many for its "
                      "job's environment, every command in it will fail with 'Argument list too long'.")
            with task:
                pf.Variable("CHECKPOINT_DEPLOY", paths)

    def checkpoint_rebuild_script(self, rule: str) -> str:
        """Tail appended to a checkpoint task, re-expanding its own subtree.

        A checkpoint's dependents cannot be enumerated until it has run, so the
        suite is built without them and the task calls snakemake back once its
        output exists. Only the nodes that depended on this rule when the suite
        was built ($CHECKPOINT_DEPLOY) and the tasks new to the server are
        replaced, so the rest of the suite -- including the family this task is
        running in -- is left alone.
        """
        return dd(f"""
        # --- re-expand the part of the suite that depends on this checkpoint
        {self.load_python}
        {self.load_ecflow}
        """) + self.rebuild_command(
            self.suite_targets, deploy='"$CHECKPOINT_DEPLOY"', deploy_downstream=rule
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
        if job.is_group() or not wildcards:
            # a group family holds all its tasks without wildcard subfamilies,
            # so they need every wildcard; rules without any use the rule name
            wcname = "_".join([f"{k}_{v}" for k, v in wildcards.items()])
            name = job.name + ("_" + wcname if wcname else "")
        else:
            # the rule and all other wildcards are in the (anchor) family path,
            # see create_wildcard_families, so the last one is unique there
            name = "_".join(list(wildcards.items())[-1])
        name_cleaned = ecflow_name(name)
        needsrun = any(j.dag.needrun(j) for j in jobs)
        variables = {k.upper(): v for k, v in wildcards.items()}

        if job.is_group():
            scripts = []
            for i, j in enumerate(jobs):
                script, jvars = self.job_script(j, order=i)
                scripts.append(script)
                variables.update(jvars)
            script = "\n\n".join(scripts)
            ecfp = (getattr(j.params, "ecflow", None) for j in jobs)
            ecflow_params = {k: v for d in ecfp if d is not None for k, v in d.items()}
        else:
            script, jvars = self.job_script(job)
            variables.update(jvars)
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

        if not job.is_group() and job.rule.name in self.meters:
            script += "\n\n" + self.meter_script(job.rule.name, done=not needsrun)
        family = self.create_job_family(job)
        with family:
            task = pf.Task(
                name=name_cleaned,
                submit_arguments=resources,
                script=script,
                **kwargs,
            )
        if not needsrun:
            self.done_tasks.append(task)
        if not job.is_group() and wildcards:
            # a job whose script (or submit directives, which go into its
            # header) differs from the rule's first one keeps its own
            key = (script, tuple(sorted(resources.items())))
            if self.rule_scripts.setdefault(job.rule.name, key) == key:
                self.share_script(task, self.families["main"], job.rule.name)
        return family, task

    def share_script(self, task: pf.Task, parent: pf.Family, rule: str, suffix: str = ""):
        """Have the task run <rule><suffix>.ecf in its rule's family instead of
        a script named after itself.

        ecFlow looks a script up by the task's name, so to share one between
        tasks named after their wildcards it is fetched with ECF_SCRIPT_CMD,
        which the server runs ahead of that lookup.
        """
        family = parent.find_node(ecflow_name(rule))
        path = os.path.join(family.files_path, ecflow_name(rule) + suffix + task.deploy_extension)
        with task:
            pf.Variable("ECF_SCRIPT_CMD", f"cat {path}")
        self.shared_scripts[id(task)] = path

    def meter_script(self, rule: str, done: bool) -> str:
        """The lines that count a task of the rule as done on the meter of
        the rule's family, which create_tasks_from_dag adds once it exists.

        ecFlow cannot increment a meter, nor can a task set one on its family
        with --meter, so this reads the value and alters it. Two tasks finishing
        at the same moment can therefore lose a count. A failed update is only
        reported, a meter is not worth aborting the task for.
        """
        meter = self.meters[rule]
        meter[2] += done
        family = f"{self.families['main'].fullname}/{ecflow_name(rule)}"
        n, name = meter[1], self.meter_name
        return dd(f"""
        # count this task as done on the meter of its rule family
        v=$(ecflow_client --query meter {family}:{name}) && \\
            ecflow_client --alter change meter {name} $(( v < {n} ? v + 1 : {n} )) {family} || \\
            echo "Could not update the meter of {family}."
        """)

    def set_meters(self, nodes: list):
        """Start the meters of replaced families at their completed tasks,
        which a definition cannot carry."""
        replaced = [n.fullname for n in nodes]
        for family, _, done in self.meters.values():
            path = family.fullname
            if done and any(path == r or path.startswith(r + "/") for r in replaced):
                self.ecflow_client.alter(path, "change", "meter", self.meter_name, str(done))

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
        # take all wildcards except the last one, which names the task
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
            "memory_per_cpu": "memory_per_cpu",
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
            # resources depending on input sizes (e.g. snakemake's default
            # mem_mb=max(2*input.size_mb, 1000)) are <TBD> while the inputs don't
            # exist yet, i.e. for most of a freshly built suite; sbatch rejects them
            if k in accepted_resources and v is not None and not isinstance(v, TBDString):
                # several snakemake resources map to one directive, e.g. threads
                # (_cores) and a default cpus=1: the larger one wins, not the last;
                # values of different types (e.g. time "01:00:00" and runtime in
                # minutes) can't be compared, there the last one wins
                t = accepted_resources[k]
                numbers = all(isinstance(x, (int, float)) for x in (resources.get(t, 0), v))
                resources[t] = max(resources[t], v) if t in resources and numbers else v
        return resources

    def job_script(self, job: JobExecutorInterface, order=None) -> tuple:
        """Return the script to run for a job and the ecFlow variables it uses.

        Rules with a shell directive are inlined into the task script, with
        the job's files, params and wildcards as variables (see shell_command),
        so all tasks of a rule usually share one script. Anything else (run:,
        script:, notebook:, wrapper: or a rule without any code, e.g. touch()
        output) needs the snakemake runtime, so the task calls snakemake back
        for exactly this job instead.
        """
        # group tasks number their variables, OUTPUT_0, OUTPUT_1, ...
        ostr = f"_{order}" if order is not None else ""
        variables = {
            f"INPUT{ostr}": sm_format("{v}", v=job.input),
            f"OUTPUT{ostr}": sm_format("{v}", v=job.output),
        }
        if job.shellcmd is None:
            return f"{self.load_snakemake}\n\n{self.format_job_exec(job)}", variables

        lines = []

        if job.env_modules:
            lines.append(job.env_modules.shellcmd(""))

        if len(job.output) > 0:
            lines.append(f"rm -rf $OUTPUT{ostr}")

        # snakemake creates the directories of outputs and logs before a job
        if job.log:
            variables[f"LOG{ostr}"] = sm_format("{v}", v=job.log)
            lines.append(f"mkdir -p $(dirname $OUTPUT{ostr} $LOG{ostr})")
        elif len(job.output) > 0:
            lines.append(f"mkdir -p $(dirname $OUTPUT{ostr})")

        # like snakemake, remove the outputs of a failed job, so a partial
        # output (e.g. a directory created before the command failed) is not
        # taken as done when the suite is rebuilt
        if len(job.output) > 0:
            lines.append(f"trap 'rm -rf $OUTPUT{ostr}' ERR")
        command, shell_vars = self.shell_command(job, ostr, variables)
        variables.update(shell_vars)
        lines.append(command)
        if len(job.output) > 0:
            lines.append("trap - ERR")

        # and touches touch() outputs after it
        touch = [f for f in job.output if is_flagged(f, "touch")]
        if touch:
            variables[f"TOUCH{ostr}"] = " ".join(touch)
            lines.append(f"touch $TOUCH{ostr}")
        lines.append(self.io_check_script(job, order=order))

        return "\n\n".join(lines), variables

    # characters a value cannot have inside export NAME="%NAME%"
    unsafe_value = re.compile(r'["$`\\%\n]')

    def shell_command(self, job: JobExecutorInterface, ostr: str = "", taken: dict = None) -> tuple:
        """The job's shell command with ${VARIABLES} in place of the values of
        its input, output, params, wildcards, threads, resources and log, and
        those variables.

        Formatted by snakemake itself, with stand-ins (EcfVar) for those
        fields, so the command only differs between jobs of a rule in the
        variables. Falls back to the plain command with the values inlined when
        that is not possible: a format spec needing the value, a value that
        would break the export line, or a variable name already taken by
        another value.
        """
        used = {}
        stand_ins = {
            f: EcfVar(f.upper(), (f,), used)
            for f in ["input", "output", "params", "threads", "resources", "log"]
        }
        # wildcards are already variables by their own names, e.g. $SAMPLE
        stand_ins["wildcards"] = EcfVar("", ("wildcards",), used)
        try:
            command = job.format_wildcards(job.rule.shellcmd, **stand_ins)
        except Exception:
            return job.shellcmd, {}

        roots = {f: getattr(job, f) for f in stand_ins}
        roots["wildcards"] = job.wildcards
        variables = {}
        for name, path in used.items():
            value = roots[path[0]]
            for kind, key in path[1:]:
                value = getattr(value, key) if kind == "attr" else value[key]
            value = sm_format("{v}", v=value)
            name += ostr
            if self.unsafe_value.search(value) or (taken or {}).get(name, value) != value:
                return job.shellcmd, {}
            variables[name] = value

        # snakemake quotes {x:q} with shlex.quote, i.e. in single quotes, in
        # which bash would not expand the variable
        def name(m):
            return m.group(1) + ostr
        command = re.sub("'\x00(\\w+)\x00'", lambda m: '"${%s}"' % name(m), command)
        command = re.sub("\x00(\\w+)\x00", lambda m: "${%s}" % name(m), command)
        return command, variables


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
        """Create a task to clean the suite.

        The task deletes the suite it is part of, so it reports complete first
        and exits without the job footer: a --complete or --abort for a node
        that no longer exists is retried for ECF_TIMEOUT (a day by default),
        keeping the job alive all that time.
        """
        script = dd(f"""
        ecflow_client --complete
        trap - 0 $SIGNAL_LIST
        ecflow_client --delete=force yes /{self.suite.name}
        rm -rf {self.deploy_dir} {self.output_dir}
        exit 0
        """)
        with self.families["clean"]:
            clean = pf.Task(
                name="clean_suite",
                script=script,
                defstatus=pf.state.complete,
            )
        return clean

    def create_monitor_task(self):
        """A task reporting the suite's progress, see monitor.py: the overall numbers
        in its labels, the table per rule in its job output and <deploy_dir>/progress.md.
        It also corrects the rule meters (--ecflow-addmeter) to the complete tasks.
        With --ecflow-monitor minutes it runs that often while main is active or
        submitted, i.e. not before it starts, nor once it is complete or a task has
        aborted; otherwise it is complete and only run by hand. It takes no slot of
        the task limit; a failed report only shows in its status label."""
        minutes = self.settings.monitor
        files = f"{self.deploy_dir}/progress"
        script = dd(f"""
        {self.load_snakemake}
        {self.get_python_executable()} -m snakemake_executor_plugin_ecflow.monitor --labels --meters {self.meter_name} \\
            --suite {self.suite.name} --report {files}.md --json {files}.json --cache {files}_runs.json \\
            || ecflow_client --label=status "Report failed, see the job output."
        """)
        main = self.families["main"]
        schedule = {"triggers": main.active | main.submitted} if minutes else {"defstatus": pf.state.complete}
        with self.families["admin"]:
            monitor = pf.Task(
                name="monitor",
                script=script,
                labels={"progress": "", "status": "", "time": ""},
                **schedule,
            )
            if minutes:
                with monitor:
                    pf.Cron(f"00:00 23:59 {minutes // 60:02d}:{minutes % 60:02d}")
        return monitor

    def node_path(self, node) -> str:
        """Suite-relative path of a node, the form find_node expects."""
        prefix = f"/{self.suite.name}/"
        name = node.fullname
        return name[len(prefix):] if name.startswith(prefix) else name

    @property
    def deploy_nodes(self) -> list:
        """The nodes to deploy scripts for and replace on the server.

        Named with --ecflow-deploy. A checkpoint re-expanding itself
        (--ecflow-deploy-downstream) also adds the tasks the server does not
        have yet, i.e. the jobs its output has just expanded to.
        """
        deploy = self.settings.deploy or ""
        if deploy == "suite":
            return [self.suite]
        paths = [n.strip() for n in deploy.split(",") if n.strip()]
        if not self.settings.deploy_downstream:
            return [self.suite.find_node(p) for p in paths]
        named = []
        for path in paths:
            try:
                named.append(self.suite.find_node(path))
            except KeyError:
                print(f"{path} is no longer part of the suite, skipping it.")
        # new nodes first, so the triggers of the replaced ones resolve
        return self.new_nodes(named) + named

    def new_nodes(self, named: list) -> list:
        """Nodes covering the tasks that are not on the server yet and feed the
        named nodes, i.e. the jobs this checkpoint's output expanded to.

        Not every task new to the server: another checkpoint rebuilding at the
        same time may have just added its own, and replacing those would reset
        tasks already submitted, orphaning their jobs. Without named nodes (a
        checkpoint whose dependents have no task) all new tasks are taken.
        """
        self.ecflow_client.sync_local()
        defs = self.ecflow_client.get_defs()
        dag = self.workflow.dag
        prefixes = [n.fullname for n in named]

        def under(task):
            return any(task.fullname == p or task.fullname.startswith(p + "/") for p in prefixes)

        # the jobs feeding the named nodes: walk upstream from them once
        feeding = {j for k, t in self.tasks.items() if under(t) for j in (k.jobs if k.is_group() else [k])}
        frontier = list(feeding)
        while frontier:
            for dep in dag.dependencies[frontier.pop()]:
                if dep not in feeding:
                    feeding.add(dep)
                    frontier.append(dep)
        new = []
        for key, task in [*self.tasks.items(), *self.cleanup_tasks.items()]:
            jobs = key.jobs if key.is_group() else [key]
            if defs.find_abs_node(task.fullname) is None and (not named or feeding & set(jobs)):
                new.append(task)
        # A family that is already on the server may hold tasks this build
        # doesn't know of (another checkpoint's), so only families new to the
        # server are replaced whole; for the others, their new children.
        nodes = []

        def add(node):
            if isinstance(node, pf.Task) or defs.find_abs_node(node.fullname) is None:
                nodes.append(node)
            else:
                for child in node.executable_children:
                    add(child)

        for node in self.cover_nodes(new) if new else []:
            add(node)
        return nodes

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
        # the tasks of a rule share one script, see share_script
        written = set()
        for task in progress(tasks.values(), "Writing scripts"):
            script, includes = task.generate_script()
            path = self.shared_scripts.get(id(task), task.deploy_path)
            if path in written:
                # still checked: pyflow raises if a shared script differs
                target.deploy_uniqueness_check("\n".join(script), path)
                continue
            target.deploy_task(path, script, includes)
            written.add(path)
        for family in families.values():
            manual = self.suite.generate_stub(family.manual)
            if manual:
                target.deploy_manual(family.manual_path, manual)
        target.deploy_headers()
        print(f"Deployed {len(written)} scripts for {len(tasks)} tasks.")
        if not self.settings.linked:
            print("Copying Snakefile to deploy directory...")
            shutil.copy(self.snakefile, self.deploy_dir + "/Snakefile")

    def upload_suite(self, nodes: list):
        """Replace the given nodes on the ecFlow server."""
        ecf_host = self.settings.host
        suite = "/" + self.suite.name
        # Identity, not `in`: pyflow nodes overload == to build trigger
        # expressions, so `node in list` is always truthy.
        full = any(n is self.suite for n in nodes)
        done = self.done_paths(nodes)
        # Tasks that need not run are set complete after the upload (set_done),
        # so nothing may be submitted until then: a whole suite is created
        # suspended, a running one is suspended while parts of it are replaced
        # (unless it is suspended already, then it stays so). A checkpoint
        # usually replaces only tasks that are new or need to run, and then
        # leaves the suite alone, also when several rebuild at the same time.
        pause = bool(done) and not full and self.ecflow_client.query("dstate", suite) != "suspended"
        if pause:
            self.ecflow_client.suspend(suite)
        try:
            print("Uploading to server...")
            defs = self.suite.ecflow_definition()
            if not full:
                self.prune_new_parents(defs, nodes)
            for node in nodes:
                print(f"Replace {node.fullname} on {ecf_host}...")
                self.ecflow_client.replace(node.fullname, defs, True, True)
            # begin suite if not already done
            if full and self.ecflow_state(self.suite) == "unknown":
                self.ecflow_client.begin_suite(suite)
            # after begin, which resets states and meters
            self.set_done(done)
            self.set_meters(nodes)
        finally:
            if pause:
                self.ecflow_client.resume(suite)
        return

    def prune_new_parents(self, defs, nodes: list):
        """Remove from the client definition the nodes that a replace would add
        on the side.

        A replace creates missing parent families from the client definition,
        with all their children, not only the replaced node. A checkpoint
        rebuild would thus add tasks another checkpoint is about to deploy
        itself; ecFlow submits them, the other rebuild replaces them again and
        their first jobs are left as zombies. So in every family the server
        does not have yet, only the replaced nodes and their ancestors are kept.
        ecFlow checks the triggers of the whole client definition, so those of
        the nodes not being replaced, which may refer to the removed ones, are
        dropped from it too; only the replaced nodes are sent to the server.
        """
        self.ecflow_client.sync_local()
        server = self.ecflow_client.get_defs()
        replaced = [n.fullname for n in nodes]
        keep = set(replaced)
        for path in replaced:
            parts = path.split("/")
            keep.update("/".join(parts[:i]) for i in range(2, len(parts)))
        for path in sorted(keep - set(replaced)):
            if server is not None and server.find_abs_node(path) is not None:
                continue
            parent = defs.find_abs_node(path)
            for child in list(parent.nodes) if parent is not None else []:
                child_path = child.get_abs_node_path()
                if child_path not in keep and not any(child_path.startswith(r + "/") for r in replaced):
                    child.remove()
        for node in defs.get_all_nodes():
            path = node.get_abs_node_path()
            if not any(path == r or path.startswith(r + "/") for r in replaced):
                node.delete_trigger()
                node.delete_complete()

    def done_paths(self, nodes: list) -> list:
        """Paths of the tasks that need not run, in the replaced nodes."""
        replaced = [n.fullname for n in nodes]
        return [t.fullname for t in self.done_tasks
                if any(t.fullname == r or t.fullname.startswith(r + "/") for r in replaced)]

    def set_done(self, paths: list, batch: int = 5000):
        """Set the tasks that need not run to complete.

        A definition can only carry a defstatus, which is also what requeue
        resets a node to, so tasks loaded with defstatus complete could not be
        requeued by hand. They are loaded queued and set complete here instead.
        """
        for i in range(0, len(paths), batch):
            self.ecflow_client.force_state(paths[i:i + batch], ecflow.State.complete)
        print(f"Set {len(paths)} tasks complete that need not run.")

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
