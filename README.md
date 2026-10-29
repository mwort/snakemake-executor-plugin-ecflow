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
