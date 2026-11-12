import pytest
from pathlib import Path
import shutil

CLEAN_SUITES = True

@pytest.fixture
def tmp_path(tmp_path_factory, request):
    test_dir = Path(__file__).parent
    class_name = request.node.parent.name if request.node.parent else ""
    prefix = f"tmp_{class_name}_{request.node.name}"
    path = test_dir / (prefix)
    # left behind by an interrupted run, or kept with CLEAN_SUITES = False
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    yield path
    # Cleanup after the test
    if CLEAN_SUITES:
        shutil.rmtree(path)
