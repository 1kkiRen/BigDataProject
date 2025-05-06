from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler

from src.model.transformer.ecef import ECEFTransformer
from src.model.transformer.labeler import RadiationLabeler
from src.model.transformer.sincos import SinCosTransformer


class Feature:
	Ozone = "cmaq_ozone"
	NO2 = "cmaq_no2"
	CO = "cmaq_co"
	OC = "cmaq_oc"

	Pressure = "pressure"
	PBL = "pbl"
	Temperature = "temperature"
	WindSpeed = "wind_speed"
	CloudFraction = "cloud_fraction"

	Month = "month"
	Day = "day"
	Hour = "hour"

	Latitude = "latitude"
	Longitude = "longitude"

	Radiation = "radiation"

	Linear = [Ozone, NO2, CO, OC, Pressure, PBL, Temperature, WindSpeed, CloudFraction]
	SinCos = [Month, Day, Hour]
	Geo = [(Latitude, Longitude, "ecef")]
	Label = Radiation

	@staticmethod
	def pipeline() -> Pipeline:
		linear_assembler = VectorAssembler(inputCols=Feature.Linear, outputCol="linear_features")
		linear_scaler = StandardScaler(
			inputCol="linear_features",
			outputCol="scaled_features",
			withMean=True,
			withStd=True
		)

		sincos_transformers = [
			SinCosTransformer(inputCol=feature, outputCol=feature)
			for feature in Feature.SinCos
		]
		sincos_features = [
			f"{feature}_{postfix}"
			for feature in Feature.SinCos
			for postfix in ["sin", "cos"]
		]

		geo_transformers = [
			ECEFTransformer(latitudeCol=lon, longitudeCol=lat, outputCol=out)
			for (lat, lon, out) in Feature.Geo
		]
		geo_features = [
			f"{feature}_{postfix}"
			for _, _, feature in Feature.Geo
			for postfix in ["x", "y", "z"]
		]

		features_assembler = VectorAssembler(
			inputCols=Feature.Linear + sincos_features + geo_features,
			outputCol="features"
		)
		labeler = RadiationLabeler(inputCol=Feature.Label, outputCol="label")

		pipeline = Pipeline(
			stages=[
				linear_assembler, linear_scaler,
				*sincos_transformers,
				*geo_transformers,
				features_assembler,
				labeler,
			],
		)
		return pipeline
