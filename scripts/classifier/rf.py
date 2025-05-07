"""
Random Forest Model Preparation Module

This module provides functionality to prepare a Random Forest model for a machine learning pipeline.
It includes a function `prepare_rf` that configures a Random Forest model with a parameter grid for
hyperparameter tuning using PySpark ML libraries.

Functions:
    prepare_rf: Configures and returns a Random Forest model along with a parameter grid for tuning.
"""
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_rf():
    """
     Configures and returns a Random Forest model
      along with a parameter grid for hyperparameter tuning.

     The Random Forest model is set up with a label
     column "label" and features column "features".
     The parameter grid includes different values for
      the number of trees, maximum depth, and feature subset strategy.

     Returns:
         tuple: A tuple containing the Random Forest model and the parameter grid.
     """
    rf_model = RandomForestClassifier(labelCol="label", featuresCol="features")
    grid = (
        ParamGridBuilder()
        .addGrid(rf_model.numTrees, [16, 32, 64])
        .addGrid(rf_model.maxDepth, [3, 5, 8])
        .addGrid(rf_model.featureSubsetStrategy, ["auto", "sqrt", "log2"])
        .build()
    )
    return rf_model, grid
