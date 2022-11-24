import pytest
from datawaves_pipeline_runner.data import Dataset
from datawaves_pipeline_runner.operators.loggers import PrinterOperator
from omegaconf import OmegaConf

@pytest.mark.parametrize('msg', ["hello " + str(i) for i in range(10)])
def test_printer(msg: str, capfd):
    """
    Test to see if PrinterOperator truly prints data to stdout.
    """
    op = PrinterOperator('test', msg)
    op._operate(Dataset())
    captured = capfd.readouterr()
    assert captured.out == msg + '\n'

@pytest.mark.parametrize('msg', ["hello " + str(i) for i in range(10)])
def test_dictionary(msg: str):
    """
    Tests to see if the PrinterOperator generates a valid OmegaConf dictionary.
    """
    op = PrinterOperator('test', msg)
    dict = op.to_dictionary()
    target = OmegaConf.create({
        'name': 'test',
        '_target_': 'datawaves_pipeline_runner.operators.loggers.printer_operator.PrinterOperator',
        'msg': msg
    })
    assert dict == target
