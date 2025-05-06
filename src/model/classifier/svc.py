from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.tuning import ParamGridBuilder


def prepare_svc():
	svc = LinearSVC(labelCol="label", featuresCol="features")
	svc_vs = OneVsRest(classifier=svc)
	grid = (
		ParamGridBuilder()
		.addGrid(svc.regParam, [0.01, 0.1, 1.0])  # Algorithm hyperparameter
		.addGrid(svc.tol, [1e-6, 1e-4, 1e-2])  # Model hyperparameter
		.addGrid(svc.standardization, [True, False])  # Model hyperparameter
		.build()
	)
	return svc_vs, grid
