import pytest
from omegaconf import OmegaConf

from datawaves_pipeline_runner.operators.core import PipelineOperator
from datawaves_pipeline_runner.operators.loggers import PrinterOperator


@pytest.mark.parametrize("count", range(10))
def test_pipeline_operator(count, capfd):
    """
    Tests to see if pipeline invokes on a flat list.
    """
    printer_list = [
        PrinterOperator("test" + str(i), "hello" + str(i)) for i in range(count)
    ]
    pipeline = PipelineOperator("test", printer_list)
    pipeline.run()
    captured = capfd.readouterr()
    assert captured.out == "".join(
        ["hello" + str(i) + "\n" for i in range(len(printer_list))]
    )


def test_nested_pipelines(capfd):
    """
    Tests to see if pipeline invokes on a nested list.
    """
    printer_list = [PrinterOperator("test" + str(i), "hello") for i in range(2)]
    pipelines = [PipelineOperator("test", printer_list) for i in range(2)]

    final_pipeline = PipelineOperator("test", pipelines)
    final_pipeline.run()
    captured = capfd.readouterr()
    assert captured.out == "".join(["hello\n" for i in range(4)])


@pytest.mark.parametrize("count", range(10))
def test_dictionary(count: int):
    """
    Tests to see if the PipelineOperator generates a valid OmegaConf dictionary.
    """
    printer_list = [
        PrinterOperator("test" + str(i), "hello" + str(i)) for i in range(count)
    ]
    pipeline = PipelineOperator("test", printer_list)
    conf = pipeline.to_dictionary()
    target = OmegaConf.create(
        {
            "name": "test",
            "_target_": "datawaves_pipeline_runner.operators.core.pipeline_operator.PipelineOperator",
        }
    )
    configs = [p.to_dictionary() for p in printer_list]
    target.operators = configs
    assert conf == target
