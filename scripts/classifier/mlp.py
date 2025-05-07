"""
Multilayer Perceptron Model Preparation Module

This module provides functionality to prepare a
 Multilayer Perceptron (MLP) model for a machine learning pipeline.
It includes a function `prepare_mlp` that configures
 an MLP model with a parameter grid for hyperparameter tuning
using PySpark ML libraries.

Functions:
    prepare_mlp: Configures and returns a Multilayer
    Perceptron model along with a parameter grid for tuning.
"""


from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_mlp(features: int, labels: int):
    """
     Configures and returns a Multilayer Perceptron
      model along with a parameter grid for hyperparameter tuning.

     The Multilayer Perceptron model is set up with a
      label column "label" and features column "features".
     The parameter grid includes different configurations
     for the network layers, step size, and solver.

     Parameters:
         features (int): The number of input features.
         labels (int): The number of output labels.

     Returns:
         tuple: A tuple containing the Multilayer
          Perceptron model and the parameter grid.
     """
    mlp = MultilayerPerceptronClassifier(labelCol="label", featuresCol="features", maxIter=2)
    grid = (
        ParamGridBuilder()
        .addGrid(
            mlp.layers,
            [
                [features, 8, labels],
                [features, 16, labels],
                [features, 24, labels],
            ]
        )
        .addGrid(mlp.stepSize, [0.01, 0.05, 0.1])
        .addGrid(mlp.solver, ["l-bfgs", "gd"])
        .build()
    )
    return mlp, grid
