import sys
import numpy as np
import collections
from itertools import combinations
import pdb

from pyspark import SparkConf,SparkContext
from math import sqrt
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

'''
	Using ALS to compare between the two algorithms
'''	
if __name__ == "__main__":
	if len(sys.argv) !=2:
		print >> sys.stderr, "Usage: first <datafile>"
		exit(-1)
	sc = SparkContext(appName="Performance")
	# Load and parse the data
	lines = sc.textFile(sys.argv[1])
	ratings = lines.map(lambda line: line.split(',')).map(lambda row: Rating(int(row[0]), int(row[1]), float(row[2])))
	# Build the recommendation model using Alternating Least Squares
	rank = 10
	numberOfIterarions = 10
	model = ALS.train(ratings, rank, numberOfIterarions)

	# Evaluate the model on training data
	testdata = ratings.map(lambda p: (p[0], p[1]))
	predictions = model.predictAll(testdata).map(lambda row: ((row[0], row[1]), row[2])).sortBy(lambda row:row[0])
	ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
	print ratesAndPreds.take(3)
	ratesAndPreds_nonZero = ratesAndPreds.filter(lambda r:r[0][0]==10).sortBy(lambda r:r[0][0])
	print ratesAndPreds_nonZero.take(100)
	
	MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
	print("Mean Squared Error = " + str(MSE))


	sc.stop()