from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_rf():
    rf = RandomForestClassifier(labelCol="label", featuresCol="features")
    grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [16, 32, 64])
        .addGrid(rf.maxDepth, [3, 5, 8])
        .addGrid(rf.featureSubsetStrategy, ["auto", "sqrt", "log2"])
        .build()
    )
    return rf, grid
