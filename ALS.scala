// 필요한 클래스 Import
import java.io.File
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

// 로깅 레벨 설정
Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

// 데이터 디렉토리
val movieLensHomeDir = "movielens"

// 사용자 선호도 로딩
val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map { line =>
    val fields = line.split("::")
    (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
}

// 컨텐츠 메타데이터 로딩 
val movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map { line =>
    val fields = line.split("::")
    // format: (movieId, movieName, genre)
    (fields(0).toInt, fields(1) + "::" + fields(2))
}.collect().toMap

// 분석 대상 데이터의 개수를 확인
val numRatings = ratings.count()
val numUsers = ratings.map(_._2.user).distinct().count()
val numMovies = ratings.map(_._2.product).distinct().count()

// 학습에 60%, 검증에 20%, 테스트에 20%를 사용하도록 날짜를 기준으로 데이터를 분리
val numPartitions = 4
val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()
val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()
val test = ratings.filter(x => x._1 >= 8).values.cache()

// 학습하기 위한 데이터 건수
val numTraining = training.count()
val numValidation = validation.count()
val numTest = test.count()

// 로그 출력
println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

// 평가에 사용할 RMSE (Root Mean Squared Error)를 계산
def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
        .join(data.map(x => ((x.user, x.product), x.rating)))
        .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
}

// 모델을 훈련하고 검증할 데이터로 모델을 평가
val ranks = List(8, 12)
val lambdas = List(0.1, 10.0)
val numIters = List(10, 20)
var bestModel: Option[MatrixFactorizationModel] = None
var bestValidationRmse = Double.MaxValue
var bestRank = 0
var bestLambda = -1.0
var bestNumIter = -1
for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
    val model = ALS.train(training, rank, numIter, lambda)
    val validationRmse = computeRmse(model, validation, numValidation)
    println("RMSE (validation) = " + validationRmse + " for the model trained with rank = " + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
    if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
    }
}

// 베스트 모델을 선택
var model = bestModel.get

// 모든 사용자에게 상품을 추천
var productsPerUser = model.recommendProductsForUsers(5)

// 추천 항목을 저장하기 위해 데이터 구조를 변경
var recommendationSet = productsPerUser.values.flatMap(u => u).map(r => (r.user + "::" + r.product + "::" + r.rating + "::" + movies.get(r.product).get))

println("Recommendation Set : " + recommendationSet.count())

// 추천 항목을 저장
recommendationSet.repartition(1).saveAsTextFile("recommendationSet")

// 사용자별 추천 상품 목록 구성
var recommendedProductsPerUser = productsPerUser.map(u => (
    (u._1) + "\t" + (u._2).map(r => (r.product)).mkString(",")
))

// 사용자별 추천 상품 저장
recommendedProductsPerUser.repartition(1).saveAsTextFile("recommendedProductsPerUser")

