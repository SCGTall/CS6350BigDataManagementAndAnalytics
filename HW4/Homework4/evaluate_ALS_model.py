# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.recommendation import ALS, Rating

#configuring the conf and setting the master & app name
conf = SparkConf().setAppName("ALSaccuracy").setMaster("local")
sc = SparkContext(conf=conf)


def ratingMap(line):
    temp = line.split('::')
    return Rating(int(temp[0]), int(temp[1]), float(temp[2]))


file_dir = 'ratings.dat'
ratings = sc.textFile(file_dir).map(ratingMap)
training, testing = ratings.randomSplit(weights=[0.6, 0.4])  # split data set
numIterations = 10
rank = 10

als_model = ALS.train(training, rank, numIterations)
testing_data = testing.map(lambda x : (x[0], x[1]))

predictions_data = als_model.predictAll(testing_data).map(lambda x : ((x[0],x[1]),x[2]))
ratings_predictions_data = predictions_data.join(testing.map(lambda r: ((r[0], r[1]), r[2])))
prep_data = ratings_predictions_data.map(lambda r: r[1])

metrics = RegressionMetrics(prep_data)
mse = metrics.meanSquaredError  # report mse of model
with open('part3_results.txt', 'w') as f:
    f.write('Accuracy (MSE) = ' + str(mse) + "\n")
f.close()
