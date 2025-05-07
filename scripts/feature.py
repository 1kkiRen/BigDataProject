"""
Feature Processing Module

This module provides functionality to define and process features for a machine learning pipeline.
It includes a class `Feature` that encapsulates various feature names and methods to assemble and
transform these features using PySpark ML libraries. The module supports linear, sinusoidal, and
geographic transformations, and provides a method to create a complete machine learning pipeline.

Classes:
    Feature: A class to define and process features for a machine learning pipeline.

Functions:
    assemble_features: Assembles and scales linear features.
    create_pipeline: Creates and returns a machine learning pipeline with feature transformations.
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler

from scripts.transformer.ecef import ECEFTransformer
from scripts.transformer.labeler import RadiationLabeler
from scripts.transformer.sincos import SinCosTransformer


class Feature:
    """
     A class to define and process features for a machine learning pipeline.
     This class includes methods to assemble and transform features for modeling.
    """
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
    def assemble_features() -> (VectorAssembler, StandardScaler, list, list):
        """
        Assembles and scales linear features.
        :return: A tuple containing the linear assembler, linear scaler,
         and lists of sincos and geo features.
        """
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

        return (linear_assembler, linear_scaler, sincos_transformers,
                sincos_features, geo_transformers, geo_features)

    @staticmethod
    def pipeline() -> Pipeline:
        """
        Creates and returns a machine learning pipeline with feature transformations.
        :return: A configured PySpark ML pipeline.
        """
        (linear_assembler, linear_scaler, sincos_transformers,
         sincos_features, geo_transformers, geo_features) = Feature.assemble_features()

        features_assembler = VectorAssembler(
            inputCols=["scaled_features"] + sincos_features + geo_features,
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
