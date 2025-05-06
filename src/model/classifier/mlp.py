from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_mlp(features: int, labels: int):
	mlp = MultilayerPerceptronClassifier(labelCol="label", featuresCol="features")
	grid = (
		ParamGridBuilder()
		.addGrid(
			mlp.layers,
			[
				[features, 8, labels],
				[features, 16, labels],
				[features, 32, labels],
			]
		)
		.addGrid(mlp.stepSize, [0.01, 0.05, 0.1])
		.addGrid(mlp.tol, [1e-4, 1e-3, 1e-2])
		.build()
	)
	return mlp, grid
