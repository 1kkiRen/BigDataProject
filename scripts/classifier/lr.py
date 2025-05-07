"""
Logistic Regression Model Preparation Module

This module provides functionality to prepare a
Logistic Regression model for a machine learning pipeline.
It includes a function `prepare_lr` that configures
 a Logistic Regression model with a parameter grid
for hyperparameter tuning using PySpark ML libraries.

Functions:
    prepare_lr: Configures and returns a Logistic
     Regression model along with a parameter grid for tuning.
"""

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder


def prepare_lr():
    """
        Configures and returns a Logistic Regression model
        along with a parameter grid for hyperparameter tuning.

        The Logistic Regression model is set up with a label
        column "label" and features column "features".
        The parameter grid includes different values for
        regularization parameter, elastic net parameter, and threshold.

        Returns:
            tuple: A tuple containing the Logistic
            Regression model and the parameter grid.
        """
    lr_model = LogisticRegression(labelCol="label", featuresCol="features", maxIter=100)
    grid = (
        ParamGridBuilder()
        .addGrid(lr_model.regParam, [0.01, 0.1, 1.0])
        .addGrid(lr_model.elasticNetParam, [0.0, 0.5, 1.0])
        .addGrid(lr_model.threshold, [0.3, 0.5, 0.7])
        .build()
    )
    return lr_model, grid
