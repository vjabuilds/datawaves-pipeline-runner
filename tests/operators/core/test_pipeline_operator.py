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

def test_nested_pipelines(capfd):
    """
    Tests to see if pipeline invokes on a nested list.
    """
    printer_list = [PrinterOperator('test' + str(i), 'hello') for i in range(2)]
    pipelines = [PipelineOperator('test', printer_list) for i in range(2)]

    final_pipeline = PipelineOperator('test', pipelines)
    final_pipeline.run()
    captured = capfd.readouterr()
    assert captured.out == ''.join(['hello\n' for i in range(4)])