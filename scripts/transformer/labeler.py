"""
Radiation Labeler Transformer Module

This module provides a transformer class
`RadiationLabeler` for labeling radiation data
based on predefined thresholds. The transformer
 is designed to be used within a PySpark ML pipeline.

Classes:
    RadiationLabeler: A transformer class to
     label radiation data based on specified thresholds.
"""

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F

# pylint: disable=C0103, E1101, W0613, R0901, R1725
class RadiationLabeler(Transformer, HasInputCol, HasOutputCol,
                       DefaultParamsReadable, DefaultParamsWritable):
    """
    A transformer class to label radiation
     data based on predefined thresholds.

    This transformer can be used in a PySpark
     ML pipeline to transform radiation data into
    categorical labels, which are useful
    for various analyses and modeling tasks.
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        Initializes the RadiationLabeler with
        specified input and output column names.

        Parameters:
            inputCol (str, optional): The name of the input column containing radiation data.
            outputCol (str, optional): The name of the output column for labeled data.
        """
        super(RadiationLabeler, self).__init__()
        self._setDefault()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        Sets the parameters for the RadiationLabeler.

        Parameters:
            inputCol (str, optional): The name of the input column containing radiation data.
            outputCol (str, optional): The name of the output column for labeled data.

        Returns:
            RadiationLabeler: The transformer instance with updated parameters.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value):
        """
        Sets the input column name for radiation data.

        Parameters:
            value (str): The name of the input
             column containing radiation data.

        Returns:
            RadiationLabeler: The transformer instance with updated input column.
        """
        return self.setParams(inputCol=value)

    def setOutputCol(self, value):
        """
        Sets the output column name for labeled data.

        Parameters:
            value (str): The name of the output
             column for labeled data.

        Returns:
            RadiationLabeler: The transformer instance with updated output column.
        """
        return self.setParams(outputCol=value)

    def _transform(self, dataset):
        """
        Transforms the dataset by labeling radiation
         data based on predefined thresholds.

        Parameters:
            dataset (DataFrame): The input dataset
             containing radiation data.

        Returns:
            DataFrame: The dataset with added label
             column based on radiation thresholds.
        """
        input_col = self.getInputCol()
        output_col = self.getOutputCol()

        label = (
            F.when(F.col(input_col) < F.lit(100), 0)
            .when((F.col(input_col) >= F.lit(100)) & (F.col(input_col) < F.lit(500)), 1)
            .otherwise(2)
        )
        dataframe = dataset.withColumn(output_col, label)

        return dataframe
