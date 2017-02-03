/**
  * Created by andy on 16/2/28.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.mllib.recommendation.ALS
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object rec1 {

  def main(args: Array[String]) {
    //    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))

    //    val conf = new SparkConf().setMaster("local").setAppName("simpleApp")
    //    val sc = new SparkContext(conf)
    val sc = new SparkContext("local[2]", "simpleApp")

    val rawData = sc.textFile("/Users/andy/Downloads/ml-100k/u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    //ratings.take(10).foreach(println)
    //rank = 50, numIterations = 10
    val model = ALS.train(ratings, 50, 10, 0.01)

    val predictedRating = model.predict(789, 123)
    println(predictedRating)
    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))

    val movies = sc.textFile("/Users/andy/Downloads/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
    val moviesForUser = ratings.keyBy(_.user).lookup(789)

    val movieId = moviesForUser.map(rating => rating.product)
    movieId.foreach(println)
    val predictedRatings= movieId.map(id => (id, model.predict(789, id)))
    println(moviesForUser.size)
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    predictedRatings.sortBy(-_._2).take(10).map(x=>(titles(x._1),x._2)).foreach(println)
    //val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    //println(aMatrix)


/*
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }

    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    cosineSimilarity(itemVector, itemVector)

    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }

    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case
      (id, similarity) => similarity })

    //println(sortedSims.take(10).mkString("\n"))
    //println(titles(itemId))

    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    val recTitle = sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim)}.mkString("\n")
    
    //println(recTitle)

    val actualRating = moviesForUser.take(1)(0)
    val predictedRating2 = model.predict(789, actualRating.product)
    val squaredError = math.pow(predictedRating2 - actualRating.rating, 2.0)

    println(squaredError)

    val usersProducts = ratings.map{ case Rating(user, product, rating) => (user, product)}
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
*/
    // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price)
    /*
    val data = sc.textFile("/Users/andy/IdeaProjects/test/data/1.csv").map(line => line.split(",")).map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))
    // let's count the number of purchases
    val numPurchases = data.count()
    // let's count how many unique users made purchases
    val uniqueUsers = data.map{ case (user, product, price) => user}.distinct().count()
    // let's sum up our total revenue
    val totalRevenue = data.map{ case (user, product, price) => price.toDouble }.sum()
    // let's find our most popular product
    val productsByPopularity = data.map{ case (user, product, price) => (product, 1) }.reduceByKey(_ + _).collect().sortBy(+_._2)
    val mostPopular = productsByPopularity(0)
    println("Total purchases: " + numPurchases)
    println("Unique users: " + uniqueUsers)
    println("Total revenue: " + totalRevenue)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))


    val broadcastAList = sc.broadcast(List("a","b","c","d","e"))
    val data = sc.parallelize(List("1","2","3")).map(x=>broadcastAList.value ++x).collect
    data.foreach(println)*/
  }
}
