import java.util

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

import scala.collection.mutable

/*
 1. e(c1,c2) \in E iff: exist a pair of point: (p1, p2) \in P(c1)*P(c2) s.t. is(p1, p2)<=epsilon or (1+rho)*epsilon
 2.
 */

//Use DBSCAN.scala instead
@deprecated
object Dynamic_DBSCAN extends Serializable {

  val conf = new SparkConf().setMaster("local[2]").setAppName("Dynamic DBSCAN")
  val sc = new SparkContext(conf)

  val eps = 0.9
  val rho = 0.2
  val minPts = 4
  val dim = 4
  val maxdis = (1+rho)*eps
  val blk_size = eps/sqrt(dim)
  val search_rad = sqrt(dim).toInt
  val precision = 10

  var globalRDD = sc.makeRDD(Array[(Vector[Int], Iterable[Vector[Int]])]())
  var appendingRDD = init()
  var deletionRDD = sc.makeRDD(Array[(Vector[Int], Iterable[Vector[Int]])]())

  var CCID = sc.makeRDD(Array[(Vector[Int], Int)]())
  var coreCell = sc.makeRDD(Array[(Vector[Int], Boolean)]())
  var vincnt = sc.makeRDD(Array[(Vector[Int], Int)]())
  //var cellSize = sc.makeRDD(Array[(Vector[Int], Int)]())
  var corePoint = sc.makeRDD(Array[(Vector[Int], Boolean)]())
  var edgeMap = sc.makeRDD(Array[(Vector[Int], Iterable[Vector[Int]])]())
  var Vertex = sc.makeRDD(Array[Vector[Int]]())
  var VertexHashing = mutable.HashMap[Vector[Int], Int]()
  var witnessMap = sc.makeRDD(Array[((Vector[Int], Vector[Int]), Iterable[(Vector[Int], Vector[Int])])]()) // core cell -> aBCP pairs

  val appendingInc = sc.parallelize(generateTraverse(search_rad))
  val appendingrhoInc = sc.parallelize(generateTraverse((search_rad*(1+rho)).toInt))

  var edgeX = sc.makeRDD(Array[(Vector[Int], Vector[Int])]())

  def init(): RDD[(Vector[Int], Iterable[Vector[Int]])] ={
    // Parsing Sep to dataset
    val line = sc.textFile("./data/iris.data").map(_.split(","))
      .map(x => {
        var tmp = Vector[Int]()
        for(i <- x.indices) if(i!=x.length-1) tmp ++= Vector((x(i).toDouble*precision).toInt)
        //println(tmp)
        tmp
      }).map(x => {
      var tmp = solveGroup(x)
      (tmp, x)
    }).groupByKey()
    //line.foreach(println)
    line
  }

  def semidy_dbscan(): Unit={
    appendingRDD = appendingRDD.filter(_._1.nonEmpty)
    val all = appendingRDD.union(globalRDD).map(x => (x._1, x._2.toSet)).groupByKey().map(x => {
      var tmp = Set[Vector[Int]]()
      for(s <- x._2) tmp ++= s
      (x._1, tmp.toIterable)
    })
    val cellSizeUpd = all.map(x => (x._1, x._2.size))

    //println("cellsize")
    //cellSizeUpd.foreach(println)
    //println()
    //appendingRDD.foreach(println)
    //println()
    var vinMp: mutable.HashMap[Vector[Int], Int] = new mutable.HashMap[Vector[Int], Int]()
    val append = appendingRDD.collect()
    var cnt = 1.0
    for(x <- append){
      val base= x._1
      println(cnt/append.length.toDouble)
      cnt += 1.0
      //println(base)
      //number of points in one cell: updated
      val statistic = sc.makeRDD(Array(base)).map(x => (x, -1))
        .join(cellSizeUpd)
        .map(x => x._2._2).collect()
      var edgeAddition = Array[(Vector[Int], Vector[Int])]()
      var coreCellUpd = sc.makeRDD(Array[(Vector[Int], Boolean)]())
      var upd_core = Array[(Vector[Int], Boolean)]()
      //println(base)
      val p_core = all.join(sc.makeRDD(Array(base)).map(x => (x, -1))).map(x => x._2._1.toArray).flatMap(x => x)
      if(statistic(0) >= minPts){
        //val prevStatistic = sc.makeRDD(Array(base)).map(x => (x, null)).join(cellSize).map(x => x._2._2).collect()
          // new core cell generated: update need
          // 1 mark all point related in this new core as core point, recursive pts nearby
          // 2 add edge to B(p,epsilon) covered cells where empty(c, q) = 1

        // 1
        //vincnt for epsilon-close pts
        val searchRDD_1 = appendingInc.map(y => {
          var tmp = Vector[Int]()
          for(i <- y.indices) tmp ++= Vector(y(i) + base(i))
          tmp
        }).map(x => (x, -1))
        val rel_pts = all.join(searchRDD_1).map(x => x._2._1).flatMap(x => x)//.collect()
        val self_pts = p_core.collect()
        val initial = rel_pts.map(x => (x, -1)).join(vincnt).map(x => (x._1, x._2._2)).collect()
        //val vinMp = mutable.HashMap[Vector[Int], Int]()
        for((k,v) <- initial){
          if(!vinMp.contains(k)) vinMp.put(k, v)
        }
        for(self_pt <- self_pts){
          for(rel_pt <- rel_pts.collect()){
            val dis = ecudlian(self_pt, rel_pt)
            if(dis <= eps){
              if(vinMp.contains(rel_pt)){
                vinMp.put(rel_pt, vinMp(rel_pt) + 1)
              }
              else{
                vinMp.put(rel_pt, 1)
              }
            }
            else{
              vinMp.put(rel_pt, 0)
            }
          }
        }
        for(rel_pt <- rel_pts){
          upd_core ++= Array((rel_pt, vinMp(rel_pt)>minPts))
        }
        upd_core = upd_core.++:(p_core.map(x => (x, true)).collect())
        val reverse = all.join(sc.makeRDD(Array(base)).map(x => (x, -1))).map(x => {
          var tmp = Array[(Vector[Int], Vector[Int])]()
          for(item <- x._2._1) tmp ++= Array((item, x._1))
          tmp
        }).flatMap(x => x)
        coreCellUpd = reverse.join(sc.makeRDD(upd_core)).map(x => x._2).groupByKey().map(x => {
          var ans = false
          for(item <- x._2 if !ans) ans |= item
          (x._1, ans)
        })
        vincnt = vincnt.union(sc.makeRDD(vinMp.toArray)).groupByKey().map(x => {
          var ans = -1
          for(value <- x._2) ans = Math.max(ans, value)
          (x._1, ans)
        }).filter(_._2 < minPts)
        coreCell = coreCell.union(coreCellUpd).groupByKey().map(x => {
          var ans = false
          for(item <- x._2 if !ans) ans |= item
          (x._1, ans)
        })
        corePoint = corePoint.union(sc.makeRDD(upd_core)).groupByKey().map(x => {
          var ans = false
          for (item <- x._2.toArray if !ans) ans |= item
          (x._1, ans)
        })
        // 2
        /*
        var exist = sc.makeRDD(Array(base)).map(x => (x, null)).join(edgeMap).map(x => x._2._2).flatMap(x => x).collect()
        val edgeout = exist.toSet
        val search_blks = searchRDD_1.filter(x => !edgeout.contains(x))
        val search_pts = search_blks.map(x => (x, null)).join(all).map(x => (x._1, x._2._2))
        val search_res = search_pts.map(x => {
          x._2.map(y => )
        })*/
      }
      else {
        //var upd_core = Array[(Vector[Double], Boolean)]()
        val searchRDD_1 = appendingInc.map(y => {
          var tmp = Vector[Int]()
          for(i <- y.indices) tmp ++= Vector(y(i) + base(i))
          tmp
        }).map(x => (x, -1))
        val rel_pts = searchRDD_1.join(all).map(x => x._2._2).flatMap(x => x)
        //val self_pts = sc.makeRDD(Array(base)).map(x => (x, -1)).join(all).map(x => x._2._2).flatMap(x => x).collect()
        val self_pts = p_core.collect()
        //val vinMp = mutable.HashMap[Vector[Int], Int]()
        val prevcnt = vincnt.join(rel_pts.map(x => (x, -1))).map(x => (x._1, x._2._1)).collect()
        vinMp = vinMp.++:(prevcnt)
        for(sf_pt <- self_pts){
          for(ot_pt <- rel_pts.collect()){
            val dis = ecudlian(sf_pt, ot_pt)
            if(dis <= eps){
              //println(sf_pt, ot_pt)

              if(vinMp.contains(sf_pt)) vinMp.put(sf_pt, vinMp(sf_pt) + 1)
              else vinMp.put(sf_pt, 1)

              if(vinMp.contains(ot_pt)) vinMp.put(ot_pt, vinMp(ot_pt) + 1)
              else vinMp.put(ot_pt, 1)
            }
            else{
              if(!vinMp.contains(sf_pt)) vinMp.put(sf_pt, 0)
              if(!vinMp.contains(ot_pt)) vinMp.put(ot_pt, 0)
            }
          }
        }
        //println("vinMp:")
        //vinMp.foreach(println)
        //println("self")
        //self_pts.foreach(println)
        //println("rel")
       /// rel_pts.foreach(println)
        for(pt <- self_pts) vinMp(pt) -= (self_pts.length-1)
        for(pt <- rel_pts) upd_core ++= Array((pt, vinMp(pt)>minPts))
        val reverse = all.join(sc.makeRDD(Array(base)).map(x => (x, -1))).map(x => {
          var tmp = Array[(Vector[Int], Vector[Int])]()
          for(item <- x._2._1) tmp ++= Array((item, x._1))
          tmp
        }).flatMap(x => x)
        coreCellUpd = reverse.join(sc.makeRDD(upd_core)).map(x => x._2).groupByKey().map(x => {
          var ans = false
          for(item <- x._2 if !ans) ans |= item
          (x._1, ans)
        })
        vincnt = vincnt.union(sc.makeRDD(vinMp.toArray)).groupByKey().map(x => {
          var ans = -1
          for(value <- x._2) ans =  Math.max(ans, value)
          (x._1, ans)
        }).filter(_._2 < minPts)
        coreCell = coreCell.union(coreCellUpd).groupByKey().map(x => {
          var ans = false
          for(item <- x._2 if !ans) ans |= item
          (x._1, ans)
        })
        corePoint = corePoint.union(sc.makeRDD(upd_core)).groupByKey().map(x => {
          var ans = false
          for (item <- x._2.toArray if !ans) ans |= item
          (x._1, ans)
        })
      }
      //println("Core Point:")
      //corePoint.foreach(println)
      //println()
      //println("Core cell")
      //coreCell.foreach(println)
      //println()
      val newvec = coreCellUpd.filter(_._2).map(x => x._1)
      Vertex = Vertex.union(newvec)
      //println("add edge")
      for((poi, status) <- upd_core){
        //println(poi)
        if(status){
          val group = solveGroup(poi)
          val searchRDD_1 = appendingInc.map(y => {
            var tmp = Vector[Int]()
            for(i <- y.indices) tmp ++= Vector(y(i) + group(i))
            tmp
          }).map(x => (x, -1))
          //searchRDD_1.foreach(println)
          var outer = coreCell.join(searchRDD_1).filter(_._2._1).map(x => x._1).filter(_!=group)
          //outer.foreach(println)
          //outer.map(x => (x, -1)).join(all).foreach(println)
          val solv = outer.map(x => (x, -1)).join(all).map(x => x._2._2).flatMap(x => x).map(x => {
            (x, ecudlian(x, poi) < eps)
          }).collect()
          //println("solv:")
          //for(x <- solv) println(x)
          //outer = outer.map(x => (x, poi)).map(x => (x, empty(x._2, x._1))).filter(_._2==1).map(x => x._1._1)
          //outer.foreach(println)
          for(o <- solv) edgeAddition ++= Array((group, solveGroup(o._1)))
        }
        //edgeAddition.foreach(println)
        //println()
      }
      edgeMap = edgeMap.union(sc.makeRDD(edgeAddition).groupByKey()).map(x => (x._1, x._2.toSet))
      val edges = edgeMap.map(x => {
        var tmp = Array[(Int, Int)]()
        for(it <- x._2){
          tmp ++= Array((hash(x._1), hash(it)))
        }
        tmp
      }).flatMap(x => x)
      val vertex = Vertex.map(hash)
      //val cgraph = CCGraph()
      //val searchPointRDD_1 = searchRDD_1.map(x => (x, null)).join(all).map(x => (x._1, x._2._2))
      //val searchPointRDD_2 = searchRDD_2.map(x => (x, null)).join(all).map(x => (x._1, x._2._2))
      //(vertex, edges)
      //println("vertex")
      //vertex.foreach(println)
      //println()
      //println("edge")
      //edges.foreach(println)
      //println()
    }
    //status upd
    globalRDD = all
    appendingRDD = sc.makeRDD(Array[(Vector[Int], Iterable[Vector[Int]])]())

    //test
    globalRDD.foreach(println)
    println()
    appendingRDD.foreach(println)
    println()
    VertexHashing.foreach(println)
    println()
    edgeMap.foreach(println)
    println()
    Vertex.foreach(println)
    println()
    println("CC:")
    val cc = queryCC()
    cc.foreach(println)
    evaluate(cc.collect())
  }

  def queryCC(): RDD[Iterable[Vector[Int]]]={
    CCGraph.CCRun(Vertex.map(x => (hash(x), x)), edgeMap.map(x => {
      var tmp = Array[(Int, Int)]()
      for(item <- x._2) tmp ++= Array((hash(x._1), hash(item)))
      tmp
    }).flatMap(x => x))
  }

  def queryCC_(): RDD[Iterable[Vector[Int]]]={
    CCGraph.CCRun(Vertex.map(x => (hash(x), x)), edgeX.map(x => (hash(x._1), hash(x._2))))
  }

  def dynamic_dbscan(): Unit={
    // appendingRDD - deletionRDD => globalRDD
    // witnessPair: witnessMap =+=> Maintain

    // 1 add
    appendingRDD = appendingRDD.filter(_._1.nonEmpty)
    deletionRDD = deletionRDD.filter(_._1.nonEmpty)
    var all = appendingRDD.union(globalRDD).groupByKey().map(x => {
      var tmp = Set[Vector[Int]]()
      for(s <- x._2) tmp ++= s.toSet
      (x._1, tmp.toArray)
    })
    val cellSizeUpd = all.map(x => (x._1, x._2.length))
    var vinMp = new mutable.HashMap[Vector[Int], Int]()
    var edge = new mutable.HashMap[Vector[Int], Set[Vector[Int]]]()
    for((k, v) <- edgeMap.collect()) edge.put(k, v.toSet)
    var vertex = Vertex.collect().toSet

    // ADDITION: BATCH
    var cnt = 0.0
    val append = appendingRDD.collect()
    for(x <- append){
      val base = x._1
      println(base)
      cnt += 1.0
      println(cnt/append.length.toDouble)
      val EpsSearch = appendingInc.map(x => {
        var tmp = Vector[Int]()
        for(i <- base.indices) tmp ++= Vector(base(i) + x(i))
        tmp
      })
      val relatedPts = EpsSearch.map(x => (x, -1)).join(all).map(x => (x._1, x._2._2))
      val relCnt = relatedPts.map(x => x._2).flatMap(x => x).map(x => (x, -1)).join(vincnt).map(x => (x._1, x._2._2)).collect()
      for((k, v) <- relCnt) if(!vinMp.contains(k)) vinMp.put(k, v)
      val selfPts = appendingRDD.join(sc.makeRDD(Array(base)).map(x => (x, -1))).map(x => x._2._1).flatMap(x => x).collect()
      val relatedUpdPts = relatedPts.map(x => x._2).flatMap(x => x).filter(!selfPts.toSet.contains(_)).collect()
      //core point: aBCP
      var possibleBCPMaintain = Array[(Vector[Int], Vector[Int])]()
      for(sf <- selfPts){
        for(rel <- relatedUpdPts){//could be more fast using Segment Tree
          val dis = ecudlian(sf, rel)
          if(dis <= eps){
            if(vinMp.contains(sf)) vinMp.put(sf, vinMp(sf) + 1)
            else vinMp.put(sf, 1)
            if(vinMp.contains(rel)) vinMp.put(rel, vinMp(rel) + 1)
            else vinMp.put(rel, 1)
            //possibleBCPMaintain ++= Array((sf, rel))
          }
          else if(dis <= maxdis){
            if(vinMp.contains(sf)) vinMp.put(sf, vinMp(sf) + new util.Random().nextInt(2))
            else vinMp.put(sf, new util.Random().nextInt(2))
            if(vinMp.contains(rel)) vinMp.put(rel, vinMp(rel) + new util.Random().nextInt(2))
            else vinMp.put(rel, new util.Random().nextInt(2))
          }
          else{
            if(!vinMp.contains(sf)) vinMp.put(sf, 0)
            if(!vinMp.contains(rel)) vinMp.put(rel, 0)
          }
        }
      }

      vinMp.foreach(println)

      var upd_core = Array[(Vector[Int], Boolean)]()
      for((k, v) <- vinMp) upd_core ++= Array((k, v>=minPts))
      corePoint = corePoint.union(sc.makeRDD(upd_core)).groupByKey().map(x => (x._1, x._2.foldLeft(false)((prev, v) => prev|v)))
      println("corePoint: after add")
      corePoint.foreach(println)
      //val judge = coreCell.join(sc.makeRDD(Array(base)).map(x => (x, -1))).collect()
      coreCell = coreCell.union(sc.makeRDD(upd_core).map(x => (base, x._2))).groupByKey().map(x => (x._1, x._2.foldLeft(false)((prev, v) => prev|v)))
      println("core cell")
      coreCell.foreach(println)
      //val pejudge = coreCell.join(sc.makeRDD(Array(base)).map(x => (x, -1))).collect()(0)._2._1
      //edge, aBCP maintain
      //possibleBCPMaintain.map(x => ((solveGroup(x._1), solveGroup(x._2)), (x._1, x._2))).groupBy(_._1).map

      if(possibleBCPMaintain.length!=0){
        witnessMap = witnessMap.union(sc.makeRDD(possibleBCPMaintain).map(x => ((solveGroup(x._1), solveGroup(x._2)), x)).groupByKey()).groupByKey().map(x => {
          var tmp = Array[(Vector[Int], Vector[Int])]()
          for(y <- x._2) for(item <- y) tmp ++= Array(item)
          (x._1, tmp)
        })
      }
      println("witness")
      witnessMap.map(x => {
        println(x._1)
        x._2.foreach(println)
      })
      //edge
      //val edges = possibleBCPMaintain.map(x => (solveGroup(x._1), solveGroup(x._2)))
      /*
      val corecelllocal = coreCell.filter(_._2).map(x => x._1).collect().toSet
      if(corecelllocal.contains(base)){
        vertex ++= Set(base)
        for((k, v) <- edges){
          if(corecelllocal.contains(v)){
            vertex ++= Set(v)
            if(edge.contains(k))
              edge.put(k, edge(k).++:(Set(v)))
            else
              edge.put(k, Set(v))
          }
        }
      for((k, v) <- edges){
        if(edge.contains(k)){
          var tmp = edge(k)
          tmp ++= Set(v)
          edge.put(k, tmp)
        }
        else{
          edge.put(k, Set(v))
        }
      }*/
    }
    // END ADDITION

    all = all.join(deletionRDD).map(x => {
      var originSet = x._2._1.toSet
      for(item <- x._2._2) originSet -= item
      (x._1, originSet.toArray)
    })
    val deletion = all.join(deletionRDD).map(x => {
      var tmp = Array[Vector[Int]]()
      val origin = x._2._1.toSet
      for(item <- x._2._2) if(origin.contains(item)) tmp ++= Array(item)
      (x._1, tmp)
    })
    vincnt = vincnt.union(sc.makeRDD(vinMp.toArray)).groupByKey().map(x => (x._1, x._2.foldLeft(-1)((prev, v) => math.max(prev, v))))
    // DELETION
    var removeList = Array[Vector[Int]]()
    var removeRelCore = Array[Vector[Int]]()
    for(x <- deletion){
      val base = x._1
      val EpsSearch = appendingInc.map(x => {
        var tmp = Vector[Int]()
        for(i <- base.indices) tmp ++= Vector(base(i) + x(i))
        tmp
      })
      val relatedPts = EpsSearch.map(x => (x, -1)).join(all).map(x => (x._1, x._2._2))
      val relPts = relatedPts.map(x => x._2).flatMap(x => x)
      val relCnt = relPts.map(x => (x, -1)).join(vincnt).map(x => (x._1, x._2._2)).collect()
      for((k, v) <- relCnt) if(!vinMp.contains(k)) vinMp.put(k, v)
      for(pt <- x._2){
        if(vinMp.contains(pt)){
          vinMp.remove(pt)
          removeList ++= Array(pt)
        }
        for(relpt <- relPts.collect()){
          val dis = ecudlian(pt, relpt)
          if(dis <= eps){
            if(vinMp.contains(relpt)){
              if(vinMp(relpt) == minPts) removeRelCore ++= Array(relpt)
              vinMp.put(relpt, vinMp(relpt) - 1)
            }
          }
          else if(dis <= maxdis){
            if(vinMp.contains(relpt)) {
              val minus = new util.Random().nextInt(2)
              if(vinMp(relpt)==minPts && minus==1) removeRelCore ++= Array(relpt)
              vinMp.put(relpt, vinMp(relpt) - minus)
            }
          }
        }
      }
      // removerelcore + removelist => remove
    }
    //val corePtRemove = removeRelCore.union(sc.makeRDD(removeList.map(x => (x, -1))).join(corePoint).filter(_._2._2).collect().map(x => x._1)).toSet
    //val coreCellRemove = corePtRemove.
    val degreeCorePts = removeRelCore.toSet
    val deletedCorePts = sc.makeRDD(removeList.map(x => (x, -1))).join(corePoint).filter(_._2._2).map(x => x._1).collect().toSet
    witnessMap = witnessMap.map(x => {
      val ori = x._2
      var tmp = Array[(Vector[Int], Vector[Int])]()
      for(p <- ori){
        if(!degreeCorePts.contains(p._1) && !degreeCorePts.contains(p._2) && !deletedCorePts.contains(p._1) && !deletedCorePts.contains(p._2))
          tmp ++= Array(p)
      }
      (x._1, tmp.toIterable)
    })
    val check = witnessMap.filter(_._2.isEmpty).collect().map(x => x._1)
    var addwitness = Array[((Vector[Int], Vector[Int]), Iterable[(Vector[Int], Vector[Int])])]()
    for(ck <- check){
      val fPt = corePoint.join(sc.makeRDD(Array(ck._1)).map(x => (x, -1)).join(all).map(x => x._2._2).flatMap(x => x).map(x => (x, -1))).filter(_._2._1).map(x => x._1)
      val tPt = corePoint.join(sc.makeRDD(Array(ck._2)).map(x => (x, -1)).join(all).map(x => x._2._2).flatMap(x => x).map(x => (x, -1))).filter(_._2._1).map(x => x._1)
      var tmp = Array[(Vector[Int], Vector[Int])]()
      for(f <- fPt.collect() if tmp.isEmpty){
        for(t <- tPt.collect() if tmp.isEmpty){
          val dis = ecudlian(f, t)
          if(dis <= eps){
            tmp ++= Array((f, t))
          }
        }
      }
      if(tmp.nonEmpty) addwitness ++= Array(((ck._1, ck._2), tmp.toIterable))
    }
    witnessMap = witnessMap.filter(_._2.nonEmpty).union(sc.makeRDD(addwitness))
    witnessMap.map(x => {
      println(x._1)
      x._2.foreach(println)
    })
    // END DELETION

    //edge: from witness Map
    edgeX = witnessMap.map(x => x._1)
    Vertex = edgeX.map(x => Array(x._1, x._2)).flatMap(x => x)

    // Status Update
    vincnt = vincnt.union(sc.makeRDD(vinMp.toArray)).groupByKey().map(x => (x._1, x._2.foldLeft(-1)((prev, v) => math.min(prev, v))))
    globalRDD = all.map(x => (x._1, x._2.toIterable))
    appendingRDD = sc.makeRDD(Array[(Vector[Int], Iterable[Vector[Int]])]())

    queryCC_().map(x => {
      x.foreach(println)
      println()
    })
  }

  def hash(cell: Vector[Int]): Int={
    if(!VertexHashing.contains(cell)){
      VertexHashing.put(cell, VertexHashing.size)
    }
    VertexHashing(cell)
  }

  def ecudlian(var1: Vector[Int], var2: Vector[Int]): Double={
    var ans = 0.0
    for(i <- var1.indices){
      ans += ((var1(i)-var2(i))/precision)*((var1(i)-var2(i))/precision)
    }
    ans
  }

  def empty(p: Vector[Int], c: Vector[Int]): Int={
    val inner_pts = sc.makeRDD(Array(c)).map(x => (x, -1)).join(globalRDD).map(x => x._2._2).flatMap(x => x).map(x => (x, -1))
    var corept_cnt = corePoint.join(inner_pts).map(x => (x._1, x._2._1)).map(x => {
      val px = x._1
      val item = x._2
      //println(px, item, ecudlian(px, p))
      (px, item && ecudlian(px, p)<eps, ecudlian(px, p))
    }).collect()
    inner_pts.foreach(println)
    for(x <- corept_cnt) println(x)
    val cp = corept_cnt.map(x => x._2)
    var flag = cp.foldLeft(false)((fla, it) => fla|it)
    println(flag)
    if(flag) 1 else 0
  }

  def search_domain(base: Vector[Int], adder: Array[Vector[Int]]): RDD[Vector[Int]]={
    var tmp = Array[Vector[Int]]()
    for(item <- adder){
      var items = Vector[Int]()
      for(i <- items.indices){
        items ++= Vector(item(i) + base(i))
      }
      tmp
    }
    sc.parallelize(tmp)
  }

  def append(prev: Array[Vector[Int]], search: Int): Array[Vector[Int]]={
    var ans = Array[Vector[Int]]()
    for(item <- prev){
      for(i <- -search until search){
        ans ++= Array(item.++(Vector(i)))
      }
    }
    ans
  }

  def generateTraverse(search_rads: Int): Array[Vector[Int]]={
    var cnt = dim - 1
    var tmp = Array[Vector[Int]]()
    for(i <- -search_rads until search_rads){
      tmp ++= Array(Vector(i))
    }
    while(cnt!=0){
      cnt -= 1
      tmp = append(tmp, search_rads)
    }
    tmp
  }

  def solveGroup(poi: Vector[Int]): Vector[Int]={
    var tmp = Vector[Int]()
    for(item <- poi){
      tmp ++= Vector((item/(blk_size*precision)).toInt)
    }
    tmp
  }

  def evaluate(result: Iterable[Iterable[Vector[Int]]]): Unit={
    var mp = mutable.HashMap[Vector[Int], Int]()
    val cla = mutable.HashMap(("Iris-setosa", 0), ("Iris-versicolor", 1), ("Iris-virginica", 2))
    val line = sc.textFile("./data/iris.data").map(_.split(",")).filter(_.length>0)
      .map(x => {
        var tmp = Vector[Int]()
        for(i <- x.indices) if(i!=x.length-1) tmp ++= Vector((x(i).toDouble*precision).toInt)
        //println(tmp)
        x.foreach(println)
        if(x.length == 5) (tmp, cla(x(4)))
        else (tmp, -1)
      }).map(x => {
      var tmp = solveGroup(x._1)
      (tmp, x)
    }).groupByKey().map(x => {
      val t = x._2.toArray
      (x._1, (t(0)._2, t.length))
    })
    var cnt = 0
    var tot = 0
    for(res <- result){
      val tmp = sc.makeRDD(res.toArray).map(x => (x, -1)).join(line).map(x => x._2._2).reduceByKey(_+_).map(x => (x._2, x._1)).sortByKey().collect()
      if(tmp(0)._1 > tmp(tmp.length-1)._1) cnt += tmp(0)._1
      else cnt += tmp(tmp.length-1)._1
      tot += tmp.map(x => x._1).sum
    }
    println(cnt)
    println("accurancy:")
    println(cnt.toDouble/tot.toDouble)
  }

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    //dynamic_dbscan()
    semidy_dbscan()

    val scc = new StreamingContext(sc, Seconds(1))
    val newdata = scc.socketTextStream("localhost", 8080)
    val structure = newdata.map(_.split(","))
      .map(x => {
      var tmp = Vector[Int]()
      for(item <- x) tmp ++= Vector((item.toDouble*precision).toInt)
      tmp
    })

    structure.foreachRDD(rdd => {
      appendingRDD ++= rdd.map(x => (solveGroup(x), x)).groupByKey()
      //dynamic_dbscan()
      semidy_dbscan()
      //queryCC().map(x => x.foreach(println))
      appendingRDD.foreach(println)
    })

    scc.start()

    scc.awaitTermination()
  }
}
