"""
model.py

This module provides the complete machine learning pipeline for training and evaluating
multiple classification models using Apache Spark. It is designed for use in a distributed
environment (YARN cluster) and reads data from Hive tables.

Main functionality includes:
- Creating a Spark session with custom configuration.
- Loading and joining datasets.
- Extracting features using a pipeline.
- Preparing and training various ML models (Logistic Regression, Random Forest, etc.).
- Evaluating model performance using metrics like accuracy and F1 score.
- Saving trained models and their predictions to disk.

The script is tailored for team29 and utilizes PySpark's MLlib and Hive support for data access.

To execute the pipeline, run this module directly:
    python model.py
"""

import os
import sys
from typing import Dict, Tuple, Any, List

import pandas as pd
from pyspark import SparkConf
from pyspark.ml import Model
from pyspark.ml.classification import Classifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, Evaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# C0413 checks only if some code is present before imports.
# This line is required to run the script with pyspark.
# pylint: disable=C0413
sys.path.append(os.getcwd())
from src.model.classifier.lr import prepare_lr
from src.model.classifier.mlp import prepare_mlp
from src.model.classifier.nb import prepare_nb
from src.model.classifier.rf import prepare_rf
from src.model.classifier.svc import prepare_svc
from src.model.feature import Feature


def status(task: str, done: bool, width: int = 80):
    """
    Print a formatted status message for a task.

    Args:
        task (str): Description of the task.
        done (bool): Indicates whether the task is completed (True) or not (False).
        width (int, optional): Total width for formatting output. Defaults to 80.
    """
    green = "\033[92m"
    yellow = "\033[93m"
    reset = "\033[0m"

    color = green if done else yellow
    status_str = f"[{color}{'Done' if done else 'Todo'}{reset}]"
    print(f"{task:<{width - len(status_str) - 1}} {status_str}")


def create_session() -> SparkSession:
    """
    Initialize and return a SparkSession with predefined configuration for team29.

    Returns:
        SparkSession: A configured SparkSession instance with Hive support enabled.
    """

    team = 29

    conf = (
        SparkConf()
        .setAppName(f"team{team} - spark ML")
        .setMaster("yarn")
        .set("spark.executor.memory", "4g")
        .set("spark.driver.memory", "4g")
        .set("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
        .set("spark.sql.warehouse.dir", "/user/team29/project/data/warehouse")
        .set("spark.sql.avro.compression.codec", "snappy")
    )

    spark = (
        SparkSession.builder
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    return spark


def load_data(spark: SparkSession) -> DataFrame:
    """
    Load and join 'records' and 'stations' datasets from Hive tables.

    Args:
        spark (SparkSession): The Spark session to use for data loading.

    Returns:
        DataFrame: A joined DataFrame combining records and station metadata.
    """

    records_df = (
        spark.read
        .format("avro")
        .table("team29_projectdb.records")
    )

    stations_df = (
        spark.read
        .format("avro")
        .table("team29_projectdb.stations")
    )

    dataset = records_df.join(
        stations_df.select("id", "latitude", "longitude"),
        stations_df["id"] == records_df["station_id"],
        how="left"
    )

    return dataset


def prepare_models() -> Dict[str, Tuple[Classifier, Any]]:
    """
    Prepare machine learning models and their associated hyperparameter grids.

    Returns:
        Dict[str, Tuple[Classifier, Any]]: A dictionary mapping model names to
        tuples of Classifier instances and parameter grids.
    """

    rf_model, rf_grid = prepare_rf()
    nb_model, nb_grid = prepare_nb()
    lr_model, lr_grid = prepare_lr()
    mlp_model, mlp_grid = prepare_mlp(18, 3)
    svc_model, svc_grid = prepare_svc()

    models = {
        "lr": (lr_model, lr_grid),
        "nb": (nb_model, nb_grid),
        "rf": (rf_model, rf_grid),
        "mlp": (mlp_model, mlp_grid),
        "svc": (svc_model, svc_grid),
    }

    return models


def prepare_evaluators() -> Dict[str, Evaluator]:
    """
    Create evaluation metrics for classification tasks.

    Returns:
        Dict[str, Evaluator]: A dictionary of evaluators for accuracy and F1 score.
    """

    metrics = ["accuracy", "f1"]
    evaluators = {
        m: MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName=m,
        )
        for m in metrics
    }
    return evaluators


def train_model(model, grid, train) -> Model:
    """
    Train a single model using cross-validation.

    Args:
        model: A Spark ML classifier model.
        grid: A parameter grid for hyperparameter tuning.
        train (DataFrame): The training dataset.

    Returns:
        Model: The best model from cross-validation.
    """

    cpu_count = os.cpu_count()
    cv_model = CrossValidator(
        estimator=model,
        estimatorParamMaps=grid,
        evaluator=MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1",
        ),
        numFolds=3,
        parallelism=cpu_count,
    )

    model = cv_model.fit(train)
    model = model.bestModel

    return model


def train_models(evaluators, models, train, test) -> List[Dict[str, Any]]:
    """
    Train multiple models, evaluate them, and save results and predictions.

    Args:
        evaluators (Dict[str, Evaluator]): Evaluation metrics for the models.
        models (Dict[str, Tuple[Classifier, Any]]): Dictionary of models and their parameter grids.
        train (DataFrame): The training dataset.
        test (DataFrame): The test dataset.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries with evaluation results for each model.
    """

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
        predictions = predictions.select("label", "prediction")
        status("Predict test labels", True)

        # Evaluate the model
        row = {"model": str(model)}
        for score_name, evaluator in evaluators.items():
            score = evaluator.evaluate(predictions)
            row[score_name] = score
            status(f"Evaluate {score_name}: {score}", True)
        summary.append(row)
        print(row)

        # Save predictions
        predictions = predictions.withColumn("prediction", F.col("prediction").cast("integer"))
        predictions = predictions.coalesce(1)
        predictions.write.mode("overwrite").csv(f"project/output/model_{name}_predictions")
        status("Save predictions", True)

    return summary


def main():
    """
    Main pipeline to orchestrate the Spark ML workflow: setup, data processing,
    model training, evaluation, and saving results.
    """

    # Create session
    spark = create_session()
    status("Create session", True)

    # Load data
    dataset = load_data(spark)
    status("Load data", True)

    # Split data
    train, test = dataset.randomSplit([0.7, 0.3], seed=42)
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
    train = train.repartition(16).cache()
    test = test.repartition(16).cache()
    status("Repartition train/test", True)

    # Save train/test
    train.write.mode("overwrite").json("project/data/train")
    test.write.mode("overwrite").json("project/data/test")
    status("Save train/test", True)

    # Prepare evaluators and models
    evaluators = prepare_evaluators()
    models = prepare_models()

    summary = train_models(evaluators, models, train, test)
    os.makedirs("output", exist_ok=True)
    pd.DataFrame(summary).to_csv("output/evaluation.csv", index=False)
    status("Save summary", True)

    status("Bye!", True)


if __name__ == "__main__":
    main()
