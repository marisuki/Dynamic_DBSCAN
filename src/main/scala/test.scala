import DBSCAN.{makeData, sc}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sun.security.provider.certpath.Vertex

@deprecated
object test {
  private val conf = new SparkConf().setMaster("local[2]").setAppName("Dynamic DBSCAN")
  val sc = new SparkContext(conf)
  var appendingRDD: RDD[String] = sc.makeRDD(Array[String]())
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    val scc = new StreamingContext(sc, Seconds(1))
    var data = scc.socketTextStream("localhost", 8080)
    //var tmp = Array[String]()
    val edge = Set((7,6), (6,7), (0,2), (0,0), (3,4), (7,7), (6,6), (6,1), (6,2), (2,0), (4,4), (1,6), (1,1), (4,5), (2,6), (5,4), (2,2), (5,5), (0,1), (3,3), (4,3), (1,0), (1,0), (4,3), (7,0), (0,0), (3,3), (5,3), (6,0), (2,0))
    val EdgeS = edge.map(x => Edge(x._1.toLong, x._2.toLong, "")).toArray
    val vertex = Set(0, 1, 2,3,4,5,6,7).map(x => (x.toLong, "")).toArray
    val rdde = sc.makeRDD(EdgeS)
    val rddv = sc.makeRDD(vertex)
    val graph = Graph(rddv, rdde)
    graph.connectedComponents().vertices.foreach(println)
    //scc.start()
    //scc.awaitTermination()
  }

}
