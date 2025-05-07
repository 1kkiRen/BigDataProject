from pyspark.ml.classification import NaiveBayes
from pyspark.ml.tuning import ParamGridBuilder


def prepare_nb():
    nb = NaiveBayes(labelCol="label", featuresCol="features", modelType="gaussian")
    grid = (
        ParamGridBuilder()
        .addGrid(nb.smoothing, [0.5, 1.0, 1.5])
        .addGrid(
            nb.thresholds, [
                [1.0, 1.0, 1.0],  # Neutral (no weighting)
                [1.5, 1.0, 0.5],  # Favor class 0
                [0.5, 1.0, 1.5]  # Favor class 2
            ]
        )
        .build()
    )
    return nb, grid
