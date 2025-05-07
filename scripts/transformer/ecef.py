"""
Earth-Centered, Earth-Fixed (ECEF) Transformer Module

This module provides a transformer class `ECEFTransformer` for converting geographic coordinates
(latitude, longitude, and optionally height) into Earth-Centered, Earth-Fixed (ECEF) coordinates.
The transformer is designed to be used within a PySpark ML pipeline.

Classes:
    ECEFTransformer: A transformer class to convert geographic coordinates to ECEF coordinates.
"""

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F

# pylint: disable=C0103, E1101, W0212, W0613
class ECEFTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """
    A transformer class to convert geographic coordinates (latitude, longitude, height) to
    Earth-Centered, Earth-Fixed (ECEF) coordinates.

    This transformer can be used in a PySpark ML pipeline to transform geographic data into
    ECEF coordinates, which are useful for various spatial computations and analyses.
    """

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
    def __init__(
            self,
            outputCol="ecef",
            latitudeCol="latitude",
            longitudeCol="longitude",
            heightCol=None
    ):
        """
        Initializes the ECEFTransformer with specified column names for input and output.

        Parameters:
            outputCol (str): Output column name prefix for ECEF coordinates.
            latitudeCol (str): Column name for latitude in degrees.
            longitudeCol (str): Column name for longitude in degrees.
            heightCol (str, optional): Column name for height in meters. Defaults to None.
        """
        super().__init__()
        self._setDefault(
            outputCol="ecef",
            latitudeCol="latitude",
            longitudeCol="longitude",
            heightCol=None
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(
            self,
            outputCol="ecef",
            latitudeCol="latitude",
            longitudeCol="longitude",
            heightCol=None
    ):
        """
        Sets the parameters for the ECEFTransformer.

        Parameters:
            outputCol (str): Output column name prefix for ECEF coordinates.
            latitudeCol (str): Column name for latitude in degrees.
            longitudeCol (str): Column name for longitude in degrees.
            heightCol (str, optional): Column name for height in meters. Defaults to None.

        Returns:
            ECEFTransformer: The transformer instance with updated parameters.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setLatitudeCol(self, value):
        """
        Sets the column name for latitude.

        Parameters:
            value (str): Column name for latitude in degrees.

        Returns:
            ECEFTransformer: The transformer instance with updated latitude column.
        """
        return self.setParams(latitudeCol=value)

    def setLongitudeCol(self, value):
        """
        Sets the column name for longitude.

        Parameters:
            value (str): Column name for longitude in degrees.

        Returns:
            ECEFTransformer: The transformer instance with updated longitude column.
        """
        return self.setParams(longitudeCol=value)

    def setHeightCol(self, value):
        """
        Sets the column name for height.

        Parameters:
            value (str): Column name for height in meters.

        Returns:
            ECEFTransformer: The transformer instance with updated height column.
        """
        return self.setParams(heightCol=value)

    def setOutputCol(self, value):
        """
        Sets the output column name prefix for ECEF coordinates.

        Parameters:
            value (str): Output column name prefix.

        Returns:
            ECEFTransformer: The transformer instance with updated output column.
        """
        return self.setParams(outputCol=value)

    def getLatitudeCol(self):
        """
        Gets the column name for latitude.

        Returns:
            str: The column name for latitude.
        """
        return self.getOrDefault(self.latitudeCol)

    def getLongitudeCol(self):
        """
        Gets the column name for longitude.

        Returns:
            str: The column name for longitude.
        """
        return self.getOrDefault(self.longitudeCol)

    def getHeightCol(self):
        """
        Gets the column name for height.

        Returns:
            str: The column name for height.
        """
        return self.getOrDefault(self.heightCol)

    def getOutputCol(self):
        """
        Gets the output column name prefix for ECEF coordinates.

        Returns:
            str: The output column name prefix.
        """
        return self.getOrDefault(self.outputCol)

    def _transform(self, dataset):
        """
        Transforms the dataset by converting geographic coordinates to ECEF coordinates.

        Parameters:
            dataset (DataFrame): The input dataset containing geographic coordinates.

        Returns:
            DataFrame: The dataset with added ECEF coordinate columns.
        """
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
