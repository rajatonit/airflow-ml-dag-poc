import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer

import os

if __name__ == "__main__":
    spark_home = os.getenv("SPARK_HOME")
   
    spark = SparkSession\
        .builder\
        .config("spark.jars", f"{spark_home}/jars/gcs-connector-latest-hadoop3.jar") \
        .appName("simple-ml-ex")\
        .getOrCreate()

    df = (spark.read
          .format("csv")
          .option('header', 'true')
          .load("gs://spark-poc-ca/train.csv"))

    dataset = df.select(col('Survived').cast('float'),
                         col('Pclass').cast('float'),
                         col('Sex'),
                         col('Age').cast('float'),
                         col('Fare').cast('float'),
                         col('Embarked')
                        )
    
    dataset = dataset.replace('?', None)\
        .dropna(how='any')

    dataset = StringIndexer(
    inputCol='Sex', 
    outputCol='Gender', 
    handleInvalid='keep').fit(dataset).transform(dataset)
    
    dataset = StringIndexer(
    inputCol='Embarked', 
    outputCol='Boarded', 
    handleInvalid='keep').fit(dataset).transform(dataset)
    dataset = dataset.drop('Sex')
    dataset = dataset.drop('Embarked')


    required_features = ['Pclass',
                    'Age',
                    'Fare',
                    'Gender',
                    'Boarded'
                   ]

    assembler = VectorAssembler(inputCols=required_features, outputCol='features')
    transformed_data = assembler.transform(dataset)

    (training_data, test_data) = transformed_data.randomSplit([0.8,0.2])

    rf = RandomForestClassifier(labelCol='Survived', 
                            featuresCol='features',
                            maxDepth=5)
    
    model = rf.fit(training_data)

    predictions = model.transform(test_data)

    evaluator = MulticlassClassificationEvaluator(
        labelCol='Survived', 
        predictionCol='prediction', 
        metricName='accuracy')

    accuracy = evaluator.evaluate(predictions)
    print('Test Accuracy = ', accuracy)

    spark.stop()
