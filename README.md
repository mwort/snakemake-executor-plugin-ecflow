# ecFlow interface for snakemake workflows

This allows building minimal-code, IO-based ecFlow suites from Snakemake workflows.
See [introduction and examples here](https://confluence.ecmwf.int/x/z9YHIQ).


## Running tests
All tests:
```
pytest -s tests/tests.py
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
