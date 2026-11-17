# ecFlow interface for snakemake workflows

This allows building minimal-code, IO-based ecFlow suites from Snakemake workflows.
See [introduction and examples here](https://confluence.ecmwf.int/x/z9YHIQ).

## Rules without a shell directive

A `shell:` command is inlined into the ecFlow task script, so the task is
independent of snakemake. Rules that cannot be inlined (`run:`, `script:`,
`notebook:`, `wrapper:`, or a rule with no code at all such as a `touch()`
output) instead get a task that calls `snakemake` back for exactly that job,
with `--target-jobs` and `--allowed-rules` so only this job runs and its
already-produced inputs are taken as given. Such tasks need snakemake and this
plugin on the ecFlow execution host; the task script loads the `python3` and
`ecflow` modules for that.

## Task order, priorities and rule limits

ecFlow has no priorities, but while limits (e.g. `task_limit`) are full it
submits the eligible tasks in the order of the suite definition. The triggers
keep the dependencies, so tasks are ordered by what should run first instead:

1. their snakemake priority (`priority:` in a rule, `--prioritize`)
2. their distance to the end of the workflow, the nearest first, so what is
   closest to done is finished first and `temp()` files are consumed, and
   removed, before more are made
3. their topological order, and by wildcards within a rule

A rule's family takes the place of its first task, so without priorities the
rule families of `main` come in reverse topological order, e.g. `archive`
before `convert` before `retrieve`.

A rule with more tasks than `task_limit` (`--jobs`) gets a `rule_limit` of the
same size on its family. It changes nothing until lowered in the running suite,
e.g. to keep a rule that makes temp files faster than they are consumed from
taking all the task slots:

    ecflow_client --alter change limit_max rule_limit 3 /SUITE/main/RULE

It also shows on the family how many of its tasks are submitted or active.

## Monitoring progress

Snakemake's monitoring plugins see nothing of a deployed suite, as ecFlow runs
its tasks. The suite has an `admin/monitor` task instead that reports from the
ecFlow server alone: tasks done, active, queued and aborted, mean runtime and
queue time, and the estimated time to completion, per rule and overall. It is
complete and run by hand, or with `--ecflow-monitor MINUTES` every that many
minutes while `main` is active or submitted. The overall numbers go into its
`progress`, `status` and `time` labels, the table into its job output and
`<deploy_dir>/progress.md` (and `progress.json`). Runtimes come from the server
log, which the task caches in `progress_runs.json`. It also sets the rule
meters (`--ecflow-addmeter`) to the tasks actually complete, as the tasks'
own counting can miss some (simultaneous finishes, requeues). It can be run
outside the suite too:

    python -m snakemake_executor_plugin_ecflow.monitor --host HOST --suite SUITE

To add or change the task in a suite that is already running, deploy it alone,
e.g. `--ecflow-monitor 30 --ecflow-deploy admin/monitor`.


## Running tests
All tests:
```
pytest -s tests/tests.py
```
In parallel (each test deploys its own suite, so they only wait on ecFlow):
```
pytest -n 8 tests/tests.py
```
Individual tests:
```
pytest -s tests/tests.py::TestCustomSnakefile
```

## Running commands of docs
cd tests/custom_testcase
snakemake -s Snakefile_docs --use-envmodules -n

snakemake -s ./Snakefile_docs --use-envmodules --dag | dot -Tpng -o dag.png

snakemake -s Snakefile_docs --jobs 20 --executor ecflow --ecflow-host ecflow-gen-$USER-001 --ecflow-deploy suite
