# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow and AzureML Integration
# MAGIC ### Build, train and test a simple SciKitLearn linear model using MLflow and then deploy to AzureML (AML)
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/intro.jpg" alt="Better Together" width="800">

# COMMAND ----------

import mlflow

# COMMAND ----------

# Import the required MLflow libraries
import mlflow
import mlflow.tracking
import mlflow.entities
import mlflow.azureml
from mlflow.tracking import MlflowClient

# Get the required classes from the AzureML libraries
from azureml.core import Workspace
from azureml.core.webservice import AciWebservice, Webservice

# COMMAND ----------

# Setup code
import os
import warnings
import sys, time

import pandas as pd
import numpy as np

subscription_id = "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"
resource_group = "azure-psa-apj-001"
location = "Australia Southeast"

csv_url =\
    'http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv'
try:
    data = pd.read_csv(csv_url, sep=';')
except Exception as e:
        print(
            "Unable to download training & test CSV, check your internet connection. Error: %s", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can explore the dataset to identify useful features and labels

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the machine learning function used to create and train the model
# MAGIC We will be using the wine quality data from the UCI repository.
# MAGIC 
# MAGIC Attribution
# MAGIC * The data set used in this example is from http://archive.ics.uci.edu/ml/datasets/Wine+Quality
# MAGIC * P. Cortez, A. Cerdeira, F. Almeida, T. Matos and J. Reis.
# MAGIC * Modeling wine preferences by data mining from physicochemical properties. In Decision Support Systems, Elsevier, 47(4):547-553, 2009.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/skl.jpg" alt="SciKit Learn" width="150">
# MAGIC ### Using the popular SciKit Learn Python ML framework to create, train and evaluate the model.

# COMMAND ----------

# Wine quality (0-10) based on various physicochemical tests
def train(in_alpha, in_l1_ratio):
    import os
    import warnings
    import sys

    import pandas as pd
    import numpy as np
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import ElasticNet

    import mlflow
    import mlflow.sklearn
    
    import logging
    logging.basicConfig(level=logging.WARN)
    logger = logging.getLogger(__name__)

    def eval_metrics(actual, pred):
        rmse = np.sqrt(mean_squared_error(actual, pred))
        mae = mean_absolute_error(actual, pred)
        r2 = r2_score(actual, pred)
        return rmse, mae, r2


    warnings.filterwarnings("ignore")
    np.random.seed(40)

    # Read the wine-quality csv file from the URL
    csv_url =\
        'http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv'
    try:
        data = pd.read_csv(csv_url, sep=';')
    except Exception as e:
        logger.exception(
            "Unable to download training & test CSV, check your internet connection. Error: %s", e)

    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    # Set default values if no alpha is provided
    if float(in_alpha) is None:
        alpha = 0.5
    else:
        alpha = float(in_alpha)

    # Set default values if no l1_ratio is provided
    if float(in_l1_ratio) is None:
        l1_ratio = 0.5
    else:
        l1_ratio = float(in_l1_ratio)

    # Useful for multiple runs (only doing one run in this sample notebook)    
    with mlflow.start_run():
        # Execute ElasticNet
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        # Evaluate Metrics
        predicted_qualities = lr.predict(test_x)
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        # Print out metrics
        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        # Log parameter, metrics, and model to MLflow
        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        mlflow.sklearn.log_model(lr, "model")

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can then train the model based on different hyperparameters. Here we are passing in alpha () and L1 ratio and evaluating the resulting accuracy metrics like Root Mean Squared Error (RMSE) and R2.

# COMMAND ----------

train(0.5, 0.5)

# COMMAND ----------

# Test a few more times with different alpha/l1_ratio combinations
train(0.5, 0.4)
train(0.5, 0.6)
train(0.6, 0.3)
train(0.6, 0.4)
train(0.6, 0.5)
train(0.6, 0.6)
train(0.7, 0.3)
train(0.7, 0.4)
train(0.7, 0.5)
train(0.7, 0.6)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can interact with MLflow programatically via the API (REST), the language APIs (Python, R, Java), the CLI or the GUI - lots of I's!

# COMMAND ----------

# We can see details about the experiment via the API
expid = "04f1afeb325c40a99bbeb59cfa5bb137"
print(mlflow.get_experiment(expid))

# COMMAND ----------

# And see information about experimental runs
dfRuns = mlflow.search_runs()

# COMMAND ----------

dfRuns

# COMMAND ----------

dfRuns.iloc[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Below we will register the model as Version 1 - this can also be done via the GUI

# COMMAND ----------

# We can register the model from the MLflow UI or via the API
client = MlflowClient()

model_name = 'WineQuality'
model_source = dfRuns.iloc[0].artifact_uri + '/model'
model_run_id = dfRuns.iloc[0].run_id

mdlWine = client.create_registered_model(model_name)
mdlWineVersion = client.create_model_version(model_name, model_source, model_run_id)
time.sleep(5)

# COMMAND ----------

# And list the currently registered models

client.list_registered_models()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model stage management can be incorporated into an orchestration framework like Azure DevOps to automate the stage transition and deployment steps as part of a CI/CD pipeline.

# COMMAND ----------

# Transition model to staging

client.transition_model_version_stage(name=model_name, version=1, stage='Staging')

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy the model to Azure Machine Learning (ACI)

# COMMAND ----------

# Load or create an Azure ML Workspace
workspace_name = "gsethi-azml"

azure_workspace = Workspace.create(name=workspace_name,
                                   subscription_id=subscription_id,
                                   resource_group=resource_group,
                                   location=location,
                                   create_resource_group=True,
                                   exist_ok=True)

# Build an Azure ML Container Image for an MLflow model
azure_image, azure_model = mlflow.azureml.build_image(model_uri=model_source,
                                                      workspace=azure_workspace,
                                                      synchronous=True)
# If your image build failed, you can access build logs at the following URI:
print("Access the following URI for build logs: {}".format(azure_image.image_build_log_uri))

# Set the deployment name
deployment_name = model_name.lower() + '-adb-v' + mdlWineVersion.version
# Deploy the image to Azure Container Instances (ACI) for real-time serving
webservice_deployment_config = AciWebservice.deploy_configuration()
webservice = Webservice.deploy_from_image(
                    image=azure_image, workspace=azure_workspace, name=deployment_name)
webservice.wait_for_deployment()

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing the REST Endpoint

# COMMAND ----------

# Test request
uri = webservice.scoring_uri # Retrieve the REST Endpoint URI
sample_json = {
  "columns": [
    "fixed acidity",
    "volatile acidity",
    "citric acid",
    "residual sugar",
    "chlorides",
    "free sulfur dioxide",
    "total sulfur dioxide",
    "density",
    "pH",
    "sulphates",
    "alcohol"
  ],
  "data": [
    [7.4,0.7,0,1.9,0.076,11,34,0.9978,3.51,0.56,9.4]
  ]
}
print(sample_json)

# COMMAND ----------

import requests
import json

def service_query(input_data):
  response = requests.post(
    url = uri, data=json.dumps(input_data),
    headers = {"Content-Type": "application/json"}
  )
  prediction = response.text
  print(prediction)
  return prediction

service_query(sample_json)

# COMMAND ----------

# Cleanup

Webservice.list(workspace=azure_workspace)[0].delete()
client.transition_model_version_stage(name=model_name, version=1, stage='Archived')
client.delete_registered_model(model_name)

# COMMAND ----------


