# ALS Recommendation Mining Using Scala and Apache Spark #

* movlens - MovieLens 영화 평가 데이터
* ALS.scala - Spark Shell에서 실행하기 위한 추천 로직
* personalRatings.txt - 특정 사용자의 상품 평점 (현재 로직에서는 사용하지 않으나 몇 줄 추가하면 전체 로그에 사용 가능)
* recommendationSet.txt - (ALS 결과 데이터) 사용자별 추천 데이터 (사용자,영화,예상평점,영화 메타)
* recommendedProductsPerUser.txt - (ALS 결과 데이터) 사용자별 추천 데이터 (사용자, 영화N개)
* rateMovies - 특정 사용자의 영화 평점을 생성하기 위한 Python 코드
