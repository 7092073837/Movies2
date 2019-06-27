package org.com.analytics.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Rddjoin {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val src_ratings = sc.textFile("file:///C:/Users/Public/source/ratings.csv")
   // src_ratings.take(10).foreach(println)

    val src_tags = sc.textFile("file:///C:/Users/Public/source/tags.csv")
  //  src_tags.take(10).foreach(println)

    val rat_kv = src_ratings.map{x =>

      val cols = x.split(",")

      val uid = cols(0).toInt
      val mid = cols(1).toInt
      val rating = cols(2).toFloat
      val u_m_id = uid+","+mid
      (u_m_id,rating)
    }
    rat_kv.take(10).foreach(println)

    val tags_kv = src_tags.map{x =>

      val cols = x.split(",")

      val uid = cols(0).toInt
      val mid = cols(1).toInt
      val tag = cols(2).toFloat
      val u_m_id = uid+","+mid
      (u_m_id,tag)
    }
    tags_kv.take(10).foreach(println)


  }

}
