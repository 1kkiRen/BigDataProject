"""
Naive Bayes Model Preparation Module

This module provides functionality to prepare a Naive Bayes model for a machine learning pipeline.
It includes a function `prepare_nb` that configures a Naive Bayes model with a parameter grid for
hyperparameter tuning using PySpark ML libraries.

Functions:
    prepare_nb: Configures and returns a Naive Bayes model along with a parameter grid for tuning.
"""

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.tuning import ParamGridBuilder


def prepare_nb():
    """
    Configures and returns a Naive Bayes model along
     with a parameter grid for hyperparameter tuning.

    The Naive Bayes model is set up with a label column "label" and features column "features".
    The parameter grid includes different values for smoothing and thresholds.

    Returns:
        tuple: A tuple containing the Naive Bayes model and the parameter grid.
    """
    nb_model = NaiveBayes(labelCol="label", featuresCol="features", modelType="gaussian")
    grid = (
        ParamGridBuilder()
        .addGrid(nb_model.smoothing, [0.5, 1.0, 1.5])
        .addGrid(
            nb_model.thresholds, [
                [1.0, 1.0, 1.0],  # Neutral (no weighting)
                [1.5, 1.0, 0.5],  # Favor class 0
                [0.5, 1.0, 1.5]  # Favor class 2
            ]
        )
        .build()
    )
    return nb_model, grid
