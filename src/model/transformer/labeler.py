from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F


class RadiationLabeler(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
	@keyword_only
	def __init__(self, inputCol=None, outputCol=None):
		super(RadiationLabeler, self).__init__()
		self._setDefault()
		kwargs = self._input_kwargs
		self.setParams(**kwargs)

	@keyword_only
	def setParams(self, inputCol=None, outputCol=None):
		kwargs = self._input_kwargs
		return self._set(**kwargs)

	def setInputCol(self, value):
		return self.setParams(inputCol=value)

	def setOutputCol(self, value):
		return self.setParams(outputCol=value)

	def _transform(self, dataset):
		input_col = self.getInputCol()
		output_col = self.getOutputCol()

		label = (
			F.when(F.col(input_col) < F.lit(100), 0)
			.when((F.col(input_col) >= F.lit(100)) & (F.col(input_col) < F.lit(500)), 1)
			.otherwise(2)
		)
		df = dataset.withColumn(output_col, label)

		return df
