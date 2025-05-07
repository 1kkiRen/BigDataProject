from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_mlp(features: int, labels: int):
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
