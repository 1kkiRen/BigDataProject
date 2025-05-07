# Solar Irradiance Classification

---

## Table of Contents
1) [Project Description](#Project-description)
2) [Data](#data)
3) [Methodology](#Methodology)
4) [Results](#Results)
5) [Installation and Running](#Installation-and-Running)
6) [Challenges](#Challenges)
7) [Future Work](#Future Work)

---

## Project Description

This project explores a novel approach to weather prediction by attempting to infer solar irradiance (solar radiation)
using data from inexpensive and robust sensors, such as thermometers and clocks, along with other environmental
parameters like ozone levels. Inspired by the CubeSat concept for satellites, the goal is to develop a methodology for
creating compact, mass-producible, and easily replaceable weather stations that can infer complex environmental
properties from simpler measurements. This work focuses on building a data processing and machine learning pipeline to
classify solar irradiance into predefined categories (low, medium, high).

## Data

The project utilizes the **Ozone Training Data** dataset, curated by Geoweaver
[[link]](https://huggingface.co/datasets/Geoweaver/ozone_training_data).

* **Size:** Approximately 9.29 million rows (1.5 GB)

* **Format:** CSV

* **Source:** Loaded from HuggingFace

* **Key Features:** Includes various meteorological and chemical variables such as Latitude, Longitude, Ozone
  concentration (CMAQ and AirNow), Nitrogen Dioxide, Carbon Monoxide, Organic Carbon, Surface Pressure, Planetary
  Boundary Layer height, Temperature, Wind Speed, Wind Direction, Cloud Fraction, Month, Day, and Hour.

* **Target Variable:** Ground Solar Radiation (RGRND W/m²), discretized into three classes:

    * Low: 0-100 W/m²

    * Medium: 100-500 W/m²

    * High: > 500 W/m²

## Methodology

The project pipeline consists of several key stages:

1. **Data Loading and Preprocessing:** Loading the dataset, identifying and removing redundant features (e.g., duplicate
   geographical coordinates), and splitting data into `stations.csv` and `records.csv`.

2. **Data Ingestion into SQL Database:** Creating `records` and `stations` tables in a PostgreSQL database and ingesting
   data using Python scripts.

3. **Data Transfer to Hadoop (HDFS):** Transferring data from SQL to HDFS using Apache Sqoop, storing it in Apache
   Parquet format for efficient big data processing.

4. **Hive Integration:** Creating external Hive tables over the Parquet files in HDFS to enable SQL-like querying and
   analysis. The `records` table was partitioned by month and day and bucketed by `station_id` for optimized
   performance.

5. **Feature Engineering in PySpark:** Applying transformations using PySpark:

    * Numerical feature scaling (StandardScaler).

    * Cyclical feature transformation for time-based features (month, day, hour) using SinCos transformation.

    * Coordinate transformation for latitude and longitude into the Earth-Centered, Earth-Fixed (ECEF) system.

    * Label discretization for the target variable (solar radiation).

6. **ML Modeling:** Evaluating five different classification models: Logistic Regression, Multilayer Perceptron, Naive
   Bayes, Random Forest, and One-vs-Rest Linear SVC. Each model was trained using grid search and 3-fold
   cross-validation, with F1 score as the metric for best model selection.

7. **Evaluation:** Assessing the performance of the best-performing models on a test set using Accuracy and F1 score.

## Results

The Random Forest model demonstrated the highest performance among the evaluated models:

* **Accuracy:** 0.85

* **F1 Score:** 0.84

* **Optimal Hyperparameters:** `numTrees = 64`, `maxDepth = 8`, `featureSubsetStrategy = "auto"`.

These results indicate that the initial idea of inferring solar irradiance from simpler measurements using ML models is
possible and shows promising potential.


## Installation and running
The whole project was done with **Python 3.6**.

```bash
./main.sh
```

Note: Connection with HuggingFace is not perfect. It may require to launch the script again if `Timeout Error` occurred.

## Challenges

The project involved navigating several technical challenges, primarily related to the frameworks and tools used:

* **Python Version and Dependency Issues:** Difficulties with Python version incompatibilities and managing complex
  library dependencies, including issues importing the `datasets` library on specific Python versions due to
  installation problems.

* **HIVE Data Import Distortion:** Significant distortion of values during the data import process into HIVE, impacting
  subsequent analysis and model training.

* **Horovod Installation and Configuration:** Complexities encountered while installing and configuring the Horovod
  library for Deep Learning model training.

* **PySpark Optimization:** Significant time invested in configuring PySpark for optimal training performance.

## Future Work

Based on the project findings, future work should focus on:

* Considering additional relevant features to potentially improve model performance.

* Expanding the dataset both numerically and geographically beyond the initial focus on the USA and Canada to enhance
  the model's accuracy and generalizability.

* Further exploring hyperparameter tuning for the best-performing models, particularly increasing `numTrees` and
  `maxDepth` for the Random Forest model.