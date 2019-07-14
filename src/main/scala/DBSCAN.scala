package main.scala

import breeze.numerics.sqrt
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*
    1. grid graph generate
    2. graph status maintains
    3. graph query
 */


object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("homework").setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile("./data/iris.data")
    //var cnt = 0
    val data = line.map(x => {
      val tmp = x.split(",").toArray
      var vec = Vector[Double]()
      for (i <- tmp.indices) {
        if (i != tmp.length-1) vec ++= Vector(tmp(i).toDouble)
      }
      //cnt += 1
      vec
    })
    val eps = 1
    val rho = 0.2
    val minPts = 3
    val dim = 4
    val maxdis = (1+rho)*eps
    val blk_size = eps/sqrt(dim)
    val search_rad = sqrt(dim).toInt
    val data2blk = data.map(x => {
      var tmp = Vector[Int]()
      for(item <- x){
        tmp ++= Vector((item/blk_size).toInt)
      }
      (tmp, x)
    }).groupByKey()
    var stat = true
    val core = data2blk.map(x => (x._1, x._2.toArray.length >= minPts, x._2))
    core.foreach(println)

  }
}

