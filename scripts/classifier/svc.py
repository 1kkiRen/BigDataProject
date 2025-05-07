from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.tuning import ParamGridBuilder


def prepare_svc():
    svc = LinearSVC(labelCol="label", featuresCol="features", maxIter=2)
    svc_vs = OneVsRest(classifier=svc)
    grid = (
        ParamGridBuilder()
        .addGrid(svc.regParam, [0.01, 0.1, 1.0])
        .addGrid(svc.aggregationDepth, [1, 2])
        .build()
    )
    return svc_vs, grid
