import pytest

pytestmark = pytest.mark.unit


def test_deliberate_failure_to_prove_the_gate_fails():
    assert False, "deliberate failure: the next commit reverts this test"
