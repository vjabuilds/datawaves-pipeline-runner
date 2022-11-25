from enum import Enum


class FieldAggregation(Enum):
    """
    An enumeration which contains all the supported aggregation functions
    """

    Mean = "mean"
    Sum = "sum"
    Count = "count"
    Std = "std"
    Min = "min"
    Max = "max"
