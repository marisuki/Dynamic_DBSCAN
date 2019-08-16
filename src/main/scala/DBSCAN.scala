import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._


object DBSCAN extends Serializable {
  private val conf = new SparkConf().setMaster("local[2]").setAppName("Dynamic DBSCAN")
  val sc = new SparkContext(conf)

  var appendingRDD: Array[(Vector[Int], Set[Vector[Int]])] = Array[(Vector[Int], Set[Vector[Int]])]()
  var globalRDD: RDD[(Vector[Int], Set[Vector[Int]])] = sc.makeRDD(Array[(Vector[Int], Set[Vector[Int]])]())
  var deleteRDD: Array[(Vector[Int], Set[Vector[Int]])] = Array[(Vector[Int], Set[Vector[Int]])]()
  var cellStatus: RDD[(Vector[Int], Boolean)] = sc.makeRDD(Array[(Vector[Int], Boolean)]())
  var edgeCollection: RDD[(Vector[Int], Vector[Int])] = sc.makeRDD(Array[(Vector[Int], Vector[Int])]())
  var quadForest: QuadForest = _
  var eps: Double = _
  var dim: Int = _
  var rho: Double = _
  var minPts: Int = _
  var precision: Int = _

  private def Dynamic_DBSCAN(): RDD[(Vector[Int], Vector[Int])]={
    // adding
    // Patient: asking locker for appendingRDD,deletingRDD?
    for((group, items) <- appendingRDD){
      val (base, convert) = scalaToJavaGroupItems(group, items)
      quadForest.insert(convert, true, base)
    }
    //deleting
    for((group, items) <- deleteRDD){
      val (base, convert) = scalaToJavaGroupItems(group, items)
      quadForest.delete(convert, true, base)
    }
    globalRDD = globalRDD.union(sc.makeRDD(appendingRDD)).groupByKey().map(x => (x._1, x._2.foldLeft(Set[Vector[Int]]())((pre, now) => pre.++(now))))
        .union(sc.makeRDD(deleteRDD)).groupByKey().map(x => (x._1, x._2.foldLeft(Set[Vector[Int]]())((pre, now) => pre.--(now))))
    appendingRDD = Array[(Vector[Int], Set[Vector[Int]])]()
    deleteRDD = Array[(Vector[Int], Set[Vector[Int]])]()
    val updCell = JavaToScalaMapV2I(quadForest.cellSizeUpd())
    val vinCntUpd = JavaToScalaMapV2I(quadForest.updateVinCntStatus())
    quadForest.clearModified()
    //dense?
    updCell.map(x => (x._1, x._2>=minPts))
    //core point
    val updateCorePoint = vinCntUpd.map(x => (x._1, x._2>=minPts)).toArray.map(x => (makeGroup(x._1), x._2))
    //core cell
    val updateCoreCellStatus = updateCorePoint.groupBy(_._1).
      map(x => {
        var ans = false
        for(t <- x._2) ans |= t._2
        (x._1, ans)
      })
    cellStatus = cellStatus.map(x => (x._1, (x._2, 0))).union(sc.makeRDD(updateCoreCellStatus.map(x => (x._1, (x._2, 1))).toArray))
      .groupByKey().map(x => {
      var ans = false
      var flag = false
      for((st, id) <- x._2.toArray){
        if(!flag && id == 1) {
          ans = st
          flag = true
        }
        else if(!flag) ans |= st
      }
      (x._1, ans)
    })
    val failureCell = updateCoreCellStatus.filter(!_._2).keySet
    edgeCollection = edgeCollection.map(x => (x, failureCell.contains(x._1)|failureCell.contains(x._2))).filter(!_._2).map(x => x._1)
    val appendCell = updateCoreCellStatus.filter(_._2).keySet
    var addingEdge: Array[(Vector[Int], Vector[Int])] = Array()
    for(cell <- appendCell){
      val coreneighbor = neighborCoreCell(cell)
      for(core <- coreneighbor) addingEdge ++= Array((cell, core))
    }
    edgeCollection = edgeCollection.union(sc.makeRDD(addingEdge))
    edgeCollection
  }

  def DBSCANStreamingStart(initFile: String, eps: Double, rho: Double, minPts: Int, dim: Int, precision: Int,
                           StreamingHost: String, Port: Int, interval: Int, outputFileTitle: String): Unit={
    this.eps = eps
    this.dim = dim
    this.rho = rho
    this.minPts = minPts
    this.precision = precision
    this.quadForest = new QuadForest(eps, rho, dim, precision)

    if(initFile != null) init(initFile)

    sc.setLogLevel("ERROR")
    val scc = new StreamingContext(sc, Seconds(interval))
    val data = scc.socketTextStream(StreamingHost, Port).map(_.split(","))

    data.transform(rdd => {
      val fs = rdd.collect()
      makeData(sc.makeRDD(fs))
      Dynamic_DBSCAN()
    }).transform(edges => CCGraph.traitAPI(sc, edges)).transform(cluster => outputAbstract(cluster))
        .saveAsTextFiles(outputFileTitle)

    scc.start()
    scc.awaitTermination()
  }

  private def init(filePath: String): Unit={
    val line = sc.textFile(filePath).map(_.split(","))
    makeData(line)
    Dynamic_DBSCAN()
  }

  private def makeData(input: RDD[Array[String]]): Unit={
    appendingRDD = input.map(x => {
      var tmp = Vector[Int]()
      var cell = Vector[Int]()
      for(s <- x) tmp ++= Vector((s.toDouble*precision).toInt)
      for(i <- tmp.indices) cell ++= Vector((tmp(i).toDouble/((precision*eps)/math.sqrt(tmp.size.toDouble))).toInt)
      (cell, tmp)
    }).groupByKey().map(x => (x._1, x._2.toSet)).collect()
  }

  private def makeGroup(data: Vector[Int]): Vector[Int]={
    var ans: Vector[Int] = Vector[Int]()
    for(x <- data) ans ++= Vector((x.toDouble/((precision*eps)/math.sqrt(dim.toDouble))).toInt)
    ans
  }

  private def scalaToJavaGroupItems(group: Vector[Int], items: Set[Vector[Int]]): (util.Vector[Integer], util.List[util.Vector[Integer]])={
    val convert: java.util.List[java.util.Vector[Integer]] = new util.ArrayList[util.Vector[Integer]]()
    for(it <- items) {
      val tmp: java.util.Vector[Integer] = new util.Vector[Integer]()
      for(x <- it) tmp.add(x)
      convert.add(tmp)
    }
    val base: util.Vector[Integer] = new util.Vector[Integer]()
    for(i <- group) base.add(i)
    (base, convert)
  }

  private def JavaToScalaMapV2I(data: util.Map[util.Vector[Integer], Integer]): Map[Vector[Int], Int]={
    var ans: Map[Vector[Int], Int] = Map[Vector[Int], Int]()
    for(it:util.Vector[Integer] <- data.keySet().asScala){
      var tmp = Vector[Int]()
      for(x:Integer <- it.asScala) tmp ++= Vector(x.intValue())
      ans ++= Map[Vector[Int], Int](tmp -> data.get(it))
    }
    ans
  }

  private def neighborCoreCell(cell: Vector[Int]): Array[Vector[Int]]={
    val cell4j: util.Vector[Integer] = new util.Vector[Integer]()
    for(x <- cell) cell4j.add(x)
    val ans = quadForest.generatePossibleCells(cell4j)
    val len = eps/Math.sqrt(dim)
    var append: Array[Vector[Int]] = Array()
    for(celli <- ans.asScala) {
      var tmp:Vector[Int] = Vector[Int]()
      for(x <- celli.asScala) tmp ++= Vector(x.toInt)
      append ++= Array(tmp)
    }
    val status = sc.makeRDD(append).map(x => (x, 1)).join(cellStatus).map(x => (x._1, x._2._2)).filter(_._2).map(x => x._1).collect()
    var res: Array[Vector[Int]] = Array()
    for(celli <- status){
      val dis = disCell2Cell(cell, celli)
      if(eps-dis-len/2.0>=1e-5) res ++= Array(celli)
    }
    res
  }

  private def disCell2Cell(cell1: Vector[Int], cell2:Vector[Int]): Double={
    var ans = 0.0
    val length = eps*eps/dim
    for(i <- cell1.indices) ans += (cell1(i)-cell2(i))*(cell1(i)-cell2(i))*length
    ans
  }

  private def outputAbstract(cluster: RDD[Iterable[Vector[Int]]]): RDD[String]={
    var ans: Array[String] = Array()
    val localizeCC = cluster.collect()
    for(cc <- localizeCC){
      var tmp = ""
      for(vec <- cc){
        var tt = ""
        for(x <- vec) tt.concat(x.toString)
        tt ++= "\t"
        tmp ++= tt
      }
      ans ++= Array(tmp)
    }
    sc.makeRDD(ans)
  }
}