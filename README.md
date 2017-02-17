# Spark_Streaming-Application

Use SynchronizedQueue to simulate the streaming data.

CollabFilter APIs as follows:
     train(data: DStream[MatrixEntry]))
     RMSE(ratings: RDD[(Int, Int, Double)])
     predictOn(data: DStream[test]) 

train() method is to train the collaborative filtering model.
RMSE() method is to get the root mean square error.
predictOn() method is to make predictions according to a input query stream. 

training data: http://grouplens.org/datasets/movielens/
