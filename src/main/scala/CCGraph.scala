import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag


object CCGraph extends Serializable {

  def CCRun(vertexes: RDD[(Int, Vector[Int])], edges: RDD[(Int, Int)]): RDD[Iterable[Vector[Int]]]={
    val edge: RDD[Edge[String]] = edges.map(x => Edge(x._1.toLong, x._2.toLong))
    val vertex: RDD[(VertexId, Vector[Int])] = vertexes.map(x => (x._1.toLong, x._2))
    val graph = Graph(vertex, edge)
    val result = graph.connectedComponents()
    result.vertices.join(vertex).map(x => x._2).groupByKey().map(x => x._2)
  }

}
