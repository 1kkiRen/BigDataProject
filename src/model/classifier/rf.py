from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder


def prepare_rf():
	rf = RandomForestClassifier(labelCol="label", featuresCol="features")
	grid = (
		ParamGridBuilder()
		.addGrid(rf.numTrees, [10, 25, 50])  # Algorithm hyperparameter
		.addGrid(rf.maxDepth, [5, 10, 15])  # Model hyperparameter
		.addGrid(rf.featureSubsetStrategy, ["auto", "sqrt", "log2"])  # Model hyperparameter
		.build()
	)
	return rf, grid
