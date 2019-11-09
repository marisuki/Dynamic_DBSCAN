import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

@deprecated
object CCGraph extends Serializable {

  def traitAPI(sc: SparkContext, edges: RDD[(Vector[Int], Vector[Int])]): RDD[Iterable[Vector[Int]]]={
    val localize = edges.collect()
    var v2i: Map[Vector[Int], Int] = Map()
    var edge = Array[(Int, Int)]()
    for((f, t) <- localize){
      if(!v2i.contains(f)) v2i ++= Map(f -> v2i.size)
      if(!v2i.contains(t)) v2i ++= Map(t -> v2i.size)
      edge ++= Array((v2i(f), v2i(t)))
    }
    var vertex = v2i.map(x => (x._2, x._1)).toArray
    CCRun(sc.makeRDD(vertex), sc.makeRDD(edge))
  }

  def CCRun(vertexes: RDD[(Int, Vector[Int])], edges: RDD[(Int, Int)]): RDD[Iterable[Vector[Int]]]={
    val edge: RDD[Edge[String]] = edges.map(x => Edge(x._1.toLong, x._2.toLong))
    val vertex: RDD[(VertexId, Vector[Int])] = vertexes.map(x => (x._1.toLong, x._2))
    val graph = Graph(vertex, edge)
    val result = graph.connectedComponents()
    result.vertices.join(vertex).map(x => x._2).groupByKey().map(x => x._2)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Dynamic DBSCAN")
    val sc = new SparkContext(conf)
    var vertex = sc.makeRDD(Array((1, Vector(1,2)), (2, Vector(2,3)), (3, Vector(1,5)), (4, Vector(4,5)), (5, Vector(9,2))))
      .map(x => (x._1.toLong, x._2))
    var edge:RDD[(Int, Int)] = sc.makeRDD(Array((1,2), (3,4), (3,5)))
    val edges:RDD[Edge[String]] = edge.map(x => Edge(x._1.toLong, x._2.toLong))
    val graph = Graph(vertex, edges)
    val res = graph.connectedComponents()
    res.vertices.foreach(println)
  }
}
