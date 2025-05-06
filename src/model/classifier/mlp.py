from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_mlp(features: int, labels: int):
	mlp = MultilayerPerceptronClassifier(labelCol="label", featuresCol="features", layers=[features, 32, labels])
	grid = (
		ParamGridBuilder()
		.addGrid(mlp.stepSize, [0.01, 0.05, 0.1])  # Algorithm hyperparameter
		.addGrid(mlp.blockSize, [32, 64, 128])  # Model hyperparameter
		.addGrid(mlp.tol, [1e-4, 1e-3, 1e-2])  # Model hyperparameter
		.build()
	)
	return mlp, grid
