package org.apache.spark.examples.streaming
/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties


object Q1{
	def main(args: Array[String]) {
  	
  	val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

 	val dataset = sc.textFile(args(0)).map(line=>line.split("\\^"))
 	val businessData  = sc.textFile(args(1)).map(line=>line.split("\\^"))

 	val category = businessData.map(line=>(line(0),line(1)+"  "+line(2))).distinct

 	val rate=dataset.map(line=>(line(2),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct 	
	val count=dataset.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b)
	val sumcount=rate.join(count)
	val result=sumcount.map(a=>(a._1,a._2._1/a._2._2))

	val finalResut = category.join(result)
	val list = finalResut.collect()	
	val sortedList = list.sortWith(_._2._2 > _._2._2 )
	sortedList.take(10).foreach(println)
	//val result10=sortedList.take(10).foreach(println)
  }
}

object Q2{
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
	    val sc = new SparkContext(conf)
		val userData =sc.textFile(args(1)).map(line=>line.split("\\^"))
		val reviewData = sc.textFile(args(2)).map(_.split("\\^"))

		val userFilter = userData.filter(line=>(line(1)==args(0))).map(line=>(line(0),line(1)))
		
		val reviewFilter = reviewData.map(line=>(line(1),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct 
		val countNumReviews = reviewData.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b)
		val sumcount=reviewFilter.join(countNumReviews)
		val result1=sumcount.map(a=>(a._1,a._2._1/a._2._2))

		val finalResult = userFilter.join(result1)

		val averageRating = finalResult.map(a=>(a._2._1,a._2._2))
		val numUser = averageRating.count()
		val sumRating = averageRating.reduceByKey((a,b)=>a+b)
		val result = sumRating.map(a=>(a._1,a._2/numUser))
		result.take(10).foreach(println)

	}
}	

object Q3{
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
	    val sc = new SparkContext(conf)

		val dataset=sc.textFile(args(0)).map(line=>line.split("\\^"))
		val reviewData = sc.textFile(args(1)).map(_.split("\\^"))

		val filter=dataset.filter(line=>line(1).contains("Stanford")).map(line=>(line(0),line(1)))
		val splitData = reviewData.map(p => (p(2), p(1)+", "+p(3)))
		val res = filter.join(splitData)
		val r = res.map(a=> (a._2._2))
		r.foreach(println)
	}
}

object Q4 {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)

		val userData =sc.textFile(args(0)).map(line=>line.split("\\^"))
		val reviewData = sc.textFile(args(1)).map(_.split("\\^"))
		val splitData1 = userData.map(p => (p(0),p(1)))
		val splitData2 = reviewData.map(p => (p(1), 1)).reduceByKey((a,b) => a+b)
		val joinres = splitData1.join(splitData2)
		val list = joinres.collect
		val sortedList = list.sortWith(_._2._2 > _._2._2 ).map(a => (a._1, a._2._1))
		sortedList.take(10).foreach(println)
	}
}

object Q5 {
  def main(args: Array[String]) {
  	val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)
    val businessData = sc.textFile(args(0)).map(line=>line.split("\\^"))
	val ratingData = sc.textFile(args(1)).map(line=>line.split("\\^"))
	
	val businessFilter = businessData.filter(line=>line(1).contains("TX")).map(line=>(line(0),line(1))).distinct
	val ratingFilterCount = ratingData.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b)

	val joinResult = businessFilter.join(ratingFilterCount)
	val result = joinResult.map(a=>(a._1,a._2._2))
	result.foreach(println)
	//val res10 = result.foreach(println)
	//result.saveAsTextFile("/home/hduser/UJG/Assignment_5/scala/Q5/")
	// val result = businessFilter.join(ratingFilter)
  }
}
