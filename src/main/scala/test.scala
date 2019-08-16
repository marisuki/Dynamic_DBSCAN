import DBSCAN.{makeData, sc}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}


object test {
  private val conf = new SparkConf().setMaster("local[2]").setAppName("Dynamic DBSCAN")
  val sc = new SparkContext(conf)
  var appendingRDD: RDD[String] = sc.makeRDD(Array[String]())
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    val scc = new StreamingContext(sc, Seconds(1))
    var data = scc.socketTextStream("localhost", 8080)
    //var tmp = Array[String]()
    var spout = data.transform(rdd => {
      var ans = 0.0
      for(x <- rdd.collect()) ans += x.toDouble
      sc.makeRDD(Array(ans))
    }).saveAsTextFiles("Test")


    scc.start()
    scc.awaitTermination()
  }

}
