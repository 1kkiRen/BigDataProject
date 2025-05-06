from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F

class ECEFTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
	outputCol = Param(
		Params._dummy(),
		"outputCol",
		"Output column name prefix",
		typeConverter=TypeConverters.toString,
	)

	latitudeCol = Param(
		Params._dummy(),
		"latitudeCol",
		"Column name for latitude in degrees",
		typeConverter=TypeConverters.toString
	)

	longitudeCol = Param(
		Params._dummy(),
		"longitudeCol",
		"Column name for longitude in degrees",
		typeConverter=TypeConverters.toString
	)

	heightCol = Param(
		Params._dummy(),
		"heightCol",
		"Optional column name for height in meters",
		typeConverter=TypeConverters.toString
	)

	@keyword_only
	def __init__(self, outpuCol="ecef", latitudeCol="latitude", longitudeCol="longitude", heightCol=None):
		super().__init__()
		self._setDefault(outpuCol="ecef", latitudeCol="latitude", longitudeCol="longitude", heightCol=None)
		kwargs = self._input_kwargs
		self.setParams(**kwargs)

	@keyword_only
	def setParams(self, latitudeCol="latitude", longitudeCol="longitude", heightCol=None):
		kwargs = self._input_kwargs
		return self._set(**kwargs)

	def setLatitudeCol(self, value):
		return self.setParams(latitudeCol=value)

	def setLongitudeCol(self, value):
		return self.setParams(longitudeCol=value)

	def setHeightCol(self, value):
		return self.setParams(heightCol=value)

	def setOutputCol(self, value):
		return self.setParams(outputCol=value)

	def getLatitudeCol(self):
		return self.getOrDefault(self.latitudeCol)

	def getLongitudeCol(self):
		return self.getOrDefault(self.longitudeCol)

	def getHeightCol(self):
		return self.getOrDefault(self.heightCol)

	def getOutputCol(self):
		return self.getOrDefault(self.outputCol)

	def _transform(self, dataset):
		# WGS-84 ellipsoid constants
		a = 6378137.0  # Semi-major axis (meters)
		e2 = 6.6943799901377997e-3  # First eccentricity squared

		# Fetch column names
		lat_col = self.getLatitudeCol()
		lon_col = self.getLongitudeCol()
		height_col = self.getHeightCol()
		out_col = self.getOutputCol()

		# Convert latitude and longitude to radians
		df = dataset.withColumn(f"{out_col}_lat_rad", F.radians(F.col(lat_col)))
		df = df.withColumn(f"{out_col}_lon_rad", F.radians(F.col(lon_col)))

		# Use height if provided; otherwise, use 0
		if height_col:
			df = df.withColumn(f"{out_col}_height", F.col(height_col))
		else:
			df = df.withColumn(f"{out_col}_height", F.lit(0.0))

		# Radius of curvature in the prime vertical
		lat_rad_col = F.col(f"{out_col}_lat_rad")
		df = df.withColumn(
			f"{out_col}_N",
			F.lit(a) / F.sqrt(1 - F.lit(e2) * F.sin(lat_rad_col) ** 2)
		)

		# Compute ECEF coordinates
		N = F.col(f"{out_col}_N")
		h = F.col(f"{out_col}_height")
		lon_rad_col = F.col(f"{out_col}_lon_rad")

		df = df.withColumn(
			f"{out_col}_x",
			(N + h) * F.cos(lat_rad_col) * F.cos(lon_rad_col)
		).withColumn(
			f"{out_col}_y",
			(N + h) * F.cos(lat_rad_col) * F.sin(lon_rad_col)
		).withColumn(
			f"{out_col}_z",
			(N * F.lit(1 - e2) + h) * F.sin(lat_rad_col)
		)

		return df
