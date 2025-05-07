import math

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F


class SinCosTransformer(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    period = Param(
        Params._dummy(),
        "period",
        "Period of event for sin/cos function",
        typeConverter=TypeConverters.toFloat,
    )

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, period=1.0):
        super(SinCosTransformer, self).__init__()
        self._setDefault(period=1.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, period=1.0):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value):
        return self.setParams(inputCol=value)

    def setOutputCol(self, value):
        return self.setParams(outputCol=value)

    def setPeriod(self, value):
        return self._set(period=value)

    def getPeriod(self):
        return self.getOrDefault(self.period)

    def _transform(self, dataset):
        period = self.getPeriod()
        input_col = self.getInputCol()
        output_col = self.getOutputCol()

        angle = F.lit(2 * math.pi) * F.col(input_col) / F.lit(period)
        df = dataset.withColumn(f"{output_col}_sin", F.sin(angle))
        df = df.withColumn(f"{output_col}_cos", F.cos(angle))

        return df
