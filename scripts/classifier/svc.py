"""
Support Vector Classifier (SVC) Model Preparation Module

This module provides functionality to prepare a Support Vector Classifier (SVC) model
for a machine learning pipeline. It includes a function `prepare_svc` that configures
a LinearSVC model with a OneVsRest strategy and a parameter grid for hyperparameter tuning
using PySpark ML libraries.

Functions:
    prepare_svc: Configures and returns a LinearSVC
     model with OneVsRest strategy and a parameter grid for tuning.
"""

from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.tuning import ParamGridBuilder

def prepare_svc():
    """
    Configures and returns a LinearSVC model with OneVsRest
     strategy and a parameter grid for hyperparameter tuning.

    The LinearSVC model is set up with a label column "label" and features column "features".
    The parameter grid includes different values for the
     regularization parameter and aggregation depth.

    Returns:
        tuple: A tuple containing the OneVsRest classifier with LinearSVC and the parameter grid.
    """
    svc = LinearSVC(labelCol="label", featuresCol="features", maxIter=2)
    svc_vs = OneVsRest(classifier=svc)
    grid = (
        ParamGridBuilder()
        .addGrid(svc.regParam, [0.01, 0.1, 1.0])
        .addGrid(svc.aggregationDepth, [1, 2])
        .build()
    )
    return svc_vs, grid
