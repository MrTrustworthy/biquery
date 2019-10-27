from enum import Enum

from google.cloud.bigquery import Table


class Aggregation(Enum):
    SUM = "SUM({column})"
    AVG = "AVG({column})"
    COUNT = "COUNT({column})"
    COUNTD = "COUNT(distinct {column})"


class FieldType(Enum):
    METRIC = "Metric"
    DIMENSION = "Dimension"


class Field:
    def __init__(self, table: Table, column_name: str, /, *, field_type: FieldType = FieldType.METRIC):
        self.table = table
        self.column_name = column_name
        self.field_type = field_type


class MetricModel:
    def __init__(self, name: str, field: Field, agg: Aggregation):
        self.name = name
        self.field = field
        self.agg = agg

