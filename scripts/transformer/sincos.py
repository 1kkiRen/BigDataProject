"""
Sine and Cosine Transformer Module

This module provides a transformer class `SinCosTransformer` for converting input values
into their sine and cosine components based on a specified period. The transformer is designed
to be used within a PySpark ML pipeline.

Classes:
    SinCosTransformer: A transformer class to convert input values into sine and cosine components.
"""

import math

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F

# pylint: disable=C0103, E1101, W0613, R0901, R1725, W0212, R0801
class SinCosTransformer(Transformer, HasInputCol, HasOutputCol,
                        DefaultParamsReadable, DefaultParamsWritable):
    """
    A transformer class to convert input values into sine and cosine components based on a period.

    This transformer can be used in a PySpark ML pipeline to transform input data into
    sine and cosine features, which are useful for various analyses and modeling tasks.
    """

    period = Param(
        Params._dummy(),
        "period",
        "Period of event for sin/cos function",
        typeConverter=TypeConverters.toFloat,
    )

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, period=1.0):
        """
        Initializes the SinCosTransformer with specified input, output column names, and period.

        Parameters:
            inputCol (str, optional): The name of the input column containing values to transform.
            outputCol (str, optional): The name of the output column
            prefix for sin cos components.
            period (float, optional): The period for the sine and cosine functions. Defaults to 1.0.
        """
        super(SinCosTransformer, self).__init__()
        self._setDefault(period=1.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, period=1.0):
        """
        Sets the parameters for the SinCosTransformer.

        Parameters:
            inputCol (str, optional): The name of the input column containing values to transform.
            outputCol (str, optional): The name of the output column prefix for sin cos components.
            period (float, optional): The period for the sine and cosine functions. Defaults to 1.0.

        Returns:
            SinCosTransformer: The transformer instance with updated parameters.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value):
        """
        Sets the input column name for values to transform.

        Parameters:
            value (str): The name of the input column containing values to transform.

        Returns:
            SinCosTransformer: The transformer instance with updated input column.
        """
        return self.setParams(inputCol=value)

    def setOutputCol(self, value):
        """
        Sets the output column name prefix for sine and cosine components.

        Parameters:
            value (str): The name of the output column prefix for sine and cosine components.

        Returns:
            SinCosTransformer: The transformer instance with updated output column.
        """
        return self.setParams(outputCol=value)

    def setPeriod(self, value):
        """
        Sets the period for the sine and cosine functions.

        Parameters:
            value (float): The period for the sine and cosine functions.

        Returns:
            SinCosTransformer: The transformer instance with updated period.
        """
        return self._set(period=value)

    def getPeriod(self):
        """
        Gets the period for the sine and cosine functions.

        Returns:
            float: The period for the sine and cosine functions.
        """
        return self.getOrDefault(self.period)

    def _transform(self, dataset):
        """
        Transforms the dataset by converting input values into sine and cosine components.

        Parameters:
            dataset (DataFrame): The input dataset containing values to transform.

        Returns:
            DataFrame: The dataset with added sine and cosine component columns.
        """
        period = self.getPeriod()
        input_col = self.getInputCol()
        output_col = self.getOutputCol()

        angle = F.lit(2 * math.pi) * F.col(input_col) / F.lit(period)
        df = dataset.withColumn(f"{output_col}_sin", F.sin(angle))
        df = df.withColumn(f"{output_col}_cos", F.cos(angle))

        return df
