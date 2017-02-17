/**
 * Created by sgf on 2016/4/16.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

import org.jblas.DoubleMatrix

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.evaluation.RankingMetrics
import breeze.linalg.Vector
object recsys_als {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //User based Rec
    val sc = new SparkContext("local[2]","recsys_als")
    val rawData = sc.textFile("/Users/andy/Downloads/ml-100k/u.data")
    val movies = sc.textFile("/Users/andy/Downloads/ml-100k/u.item")
    //rawData.first()
    val rawRatings = rawData.map(_.split("\t").take(3))
    //rawRatings.first()
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    //ratings.first()
    val model = ALS.train(ratings, 50, 10, 0.01)
    //model.userFeatures.count
    val predictedRating = model.predict(421, 123)
    val userId = 421
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    println(titles(123))

    val moviesForUser = ratings.keyBy(_.user).lookup(421)
    println(moviesForUser.size)
    println("Watching History:")
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    println("Recommendation List:")
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)

    //Item Based Rec

    //val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    //println(aMatrix)
    val itemId = 123
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })

    println(sortedSims.mkString("\n"))

    //check the movie title
    println(titles(itemId))
    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    val recItems =sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim) }.mkString("\n")
    println(recItems)

    //Evaluating, Compute squared error:MSE
    val actualRating = moviesForUser.take(1)(0)
    val predictedRating421 = model.predict(421, actualRating.product)
    println(actualRating)
    println(predictedRating421)
    val squaredError = math.pow(predictedRating421 - actualRating.rating, 2.0)
    println(squaredError)

    val usersProducts = ratings.map{ case Rating(user, product, rating)  => (user, product)}

    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    val ratingsAndPredictions = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)

    val MSE = ratingsAndPredictions.map{
      case ((user, product), (actual, predicted)) =>  math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)


    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)

    //MAPK K值平均准确率

    val actualMovies = moviesForUser.map(_.product)
    val predictedMovies = topKRecs.map(_.product)

    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
    println(apk10)


    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)

    val imBroadcast = sc.broadcast(itemMatrix)

    val allRecs = model.userFeatures.map{ case (userId, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq
      (userId, recommendedIds)
    }

    val userMovies = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)

    val K1 = 10
    val MAPK = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, K1)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)


    //RMSE & MSE
    val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Build-in Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Build-in Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)


    //MAP

    val predictedAndTrueForRanking = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)

    val MAPK2000 = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)




  }

  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }



}
