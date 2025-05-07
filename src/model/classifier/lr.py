from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder


def prepare_lr():
    lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=100)
    grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.01, 0.1, 1.0])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .addGrid(lr.threshold, [0.3, 0.5, 0.7])
        .build()
    )
    return lr, grid
