import pytest
from datawaves_pipeline_runner.operators.loggers import PrinterOperator
from datawaves_pipeline_runner.operators.core import PipelineOperator

@pytest.mark.parametrize('count', range(10))
def test_pipeline_operator(count, capfd):
    """
    Tests to see if pipeline invokes on a flat list.
    """
    printer_list = [PrinterOperator('test' + str(i), 'hello' + str(i)) for i in range(count)]
    pipeline = PipelineOperator('test', printer_list)
    pipeline.run()
    captured = capfd.readouterr()
    assert captured.out == ''.join(['hello' + str(i) + '\n' for i in range(len(printer_list))])