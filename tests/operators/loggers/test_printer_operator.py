import pytest
from datawaves_pipeline_runner.data import Dataset
from datawaves_pipeline_runner.operators.loggers import PrinterOperator

@pytest.mark.parametrize('msg', ["hello " + str(i) for i in range(10)])
def test_printer(msg: str, capfd):
    """
    Test to see if PrinterOperator truly prints data to stdout.
    """
    op = PrinterOperator('test', msg)
    op._operate(Dataset())
    captured = capfd.readouterr()
    assert captured.out == msg + '\n'
