from typing import Dict, Tuple, Any, List

import pandas as pd
from pyspark import SparkConf, StorageLevel
from pyspark.ml import Model
from pyspark.ml.classification import Classifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, Evaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.sql import SparkSession, DataFrame

from src.model.classifier.lr import prepare_lr
from src.model.classifier.mlp import prepare_mlp
from src.model.classifier.nb import prepare_nb
from src.model.classifier.rf import prepare_rf
from src.model.classifier.svc import prepare_svc
from src.model.feature import Feature


def status(task: str, done: bool, width: int = 80):
	green = '\033[92m'
	yellow = '\033[93m'
	reset = '\033[0m'

	color = green if done else yellow
	status_str = f"[{color}{'Done' if done else 'Todo'}{reset}]"
	print(f"{task:<{width - len(status_str) - 1}} {status_str}")


def create_session() -> SparkSession:
	team = 29

	conf = (
		SparkConf()
		.setAppName(f"{team} - spark ML")
		.set("spark.executor.memory", "4g")
		.set("spark.driver.memory", "4g")
		# .setMaster("yarn")
	)

	spark = (
		SparkSession.builder
		.config(conf=conf)
		.getOrCreate()
	)

	return spark


def load_data(spark: SparkSession) -> DataFrame:
	# TODO: replace with Hive
	records_df = (
		spark.read.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load("data/records.csv")
	)

	stations_df = (
		spark.read.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load("data/stations.csv")
	)

	df = records_df.join(
		stations_df.select("station_id", "latitude", "longitude"),
		on="station_id",
		how="left"
	)

	return df


def prepare_models() -> Dict[str, Tuple[Classifier, Any]]:
	rf, rf_grid = prepare_rf()
	nb, nb_grid = prepare_nb()
	lr, lr_grid = prepare_lr()
	mlp, mlp_grid = prepare_mlp(16, 3)
	svc, svc_grid = prepare_svc()

	models = {
		"rf": (rf, rf_grid),
		"mlp": (mlp, mlp_grid),
		"svc": (svc, svc_grid),
		"nb": (nb, nb_grid),
		"lr": (lr, lr_grid),
	}

	return models


def prepare_evaluators() -> Dict[str, Evaluator]:
	metrics = ["accuracy", "f1"]
	evaluators = {
		m: MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=m)
		for m in metrics
	}
	return evaluators


def train_model(model, grid, train) -> Model:
	cv = CrossValidator(
		estimator=model,
		estimatorParamMaps=grid,
		evaluator=MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1"),
		numFolds=3
	)

	model = cv.fit(train)
	model = model.bestModel

	return model


def train_models(evaluators, models, train, test) -> List[Dict[str, Any]]:
	summary = []
	for name, (model, grid) in models.items():
		# Train model
		model = train_model(model, grid, train)
		status(f"Train {name}", True)

		# Save model
		model.write().overwrite().save(f"project/models/model_{name}")
		status(f"Save {name}", True)

		# Predict with the model
		predictions = model.transform(test)
		status("Predict test labels", True)

		# Save predictions
		predictions = predictions.select("label", "prediction")
		predictions.coalesce(1).write.mode("overwrite").csv(f"project/output/model_{name}_predictions.csv")
		status("Save predictions", True)

		# Evaluate the model
		row = {"model": str(model)}
		for m, e in evaluators.items():
			score = e.evaluate(predictions)
			row[m] = score
			status(f"Evaluate {m}: {score}", True)
		summary.append(row)

	return summary


def main():
	# Create session
	spark = create_session()
	status("Create session", True)

	# Load data
	df = load_data(spark)
	status("Load data", True)

	# Split data
	train, test = df.randomSplit([0.7, 0.3], seed=42)
	status("Split train/test", True)

	# Create pipline
	pipeline = Feature.pipeline()
	status("Create feature extraction pipeline", True)

	# Fit pipeline
	pipeline = pipeline.fit(train)
	status("Fit feature extraction pipeline", True)

	# Apply pipeline
	train = pipeline.transform(train)
	test = pipeline.transform(test)
	status("Apply feature extraction pipeline", True)

	# Select the necessary columns
	cols = ["features", "label"]
	train = train.select(cols)
	test = test.select(cols)

	# Repartition train/test
	train = train.repartition(100).persist(StorageLevel.MEMORY_AND_DISK)
	test = test.persist(StorageLevel.MEMORY_ONLY)
	status("Repartition train/test", True)

	# TODO: Save train/test
	# train.write.mode("overwrite").json("project/data/train")
	# test.write.mode("overwrite").json("project/data/test")
	status("Save train/test", True)

	# Prepare evaluators and models
	evaluators = prepare_evaluators()
	models = prepare_models()

	summary = train_models(evaluators, models, train, test)
	pd.DataFrame(summary).to_csv("project/output/evaluation.csv", index=False)
	status("Save summary", True)

	status("Bye!", True)


if __name__ == "__main__":
	main()
