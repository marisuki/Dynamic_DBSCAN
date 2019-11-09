/*import java.io.{File, PrintWriter}
import java.util


import breeze.linalg._
import breeze.numerics._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object UVPre extends Serializable {
  private val conf = new SparkConf().setMaster("local[2]").setAppName("Dynamic DBSCAN")
  val sc = new SparkContext(conf)
  val prefile = "C://Users//Administrator//Desktop//uvmatrix//data//"
  def mainz(args: Array[String]): Unit={
    sc.setLogLevel("ERROR")
    val fin = Source.fromFile(prefile + "artist_alias.txt").getLines()
    val tc = fin.map(x =>{
      val sp = x.split(" ")
      (sp, sp.length)
    }).filter(_._2==2).map(x => x._1).toArray
    var alias = Map[String, String]()
    tc.foreach(x => alias.+=((x(0), x(1))))

    val fic = Source.fromFile(prefile + "user_artist_data.txt").getLines()
    val tp = fic.map(x => {
      val tmp = x.split(" ")
      (tmp(0), tmp(1), tmp(2))
    }).toArray
    var count = Map[(String,String), Int]()
    tp.foreach(x => {
      val k1 = x._1
      val k2 = x._2
      val v = x._3
      count += (((k1,k2), v.toInt))
    })
    var mpk1int = Map[String, Int]()
    var mpk2int = Map[String, Int]()
    //var finalist = DenseMatrix.zeros(mpk1int.size, mpk2int.size)
    for((k1,k2) <- count.keys){
      if(!mpk1int.contains(k1)) mpk1int += ((k1, mpk1int.size))
      if(!mpk2int.contains(k2)) mpk2int += ((k2, mpk2int.size))
      //finalist(mpk1int(k1), mpk2int(k2)).->(count((k1,k2)))
    }
    val out = count.toArray.map(x => (x._1._1, x._1._2, x._2)).groupBy(_._1).map(x => {
      val tmp = x._2.map(y => (mpk2int(y._2), y._3)).sortBy(_._1)
      var st = ""
      tmp.foreach(y => st += (mpk1int(x._1).toString + " " + y._1.toString + " " + y._2.toString + "\n"))
      st
    })
    val pt = new PrintWriter(new File("./uvtmp/"))
    out.foreach(x => pt.write(x))
    pt.close()
  }

  def main(args: Array[String]): Unit= {
    val fic = Source.fromFile(prefile + "trainM.txt").getLines()
    val Mc = fic.map(_.split(" "))
    var cntr = 0
    val M = Mc.flatMap(x => {
      cntr += 1
      var ar = Array[(Int, Int, Int)]()
      var cntl = 0
      for (item <- x) {
        ar ++= Array((cntr, cntl, item.toInt))
        cntl += 1
      }
      ar
    })
    val fix = Source.fromFile(prefile + "trainMx.txt").getLines()
    cntr = 0
    val Mxc = fix.map(_.split(" "))
    val Mx = Mxc.flatMap(x => {
      cntr += 1
      var ar = Array[(Int, Int, Int)]()
      var cntl = 0
      for (item <- x) {
        ar ++= Array((cntr, cntl, item.toInt))
        cntl += 1
      }
      ar
    }).toArray
    val m = 51871
    val k = 1000
    val n = 1000
    val lambda = 1e-6
    val alpha = 1e-5
    var u = DenseMatrix.ones[Double](m,k)
    var v = DenseMatrix.ones[Double](n,k)
    def em_u(): Unit={
      val Y = u.*(v)
      //println(u(1 to 4, 2 to 6))
      val update = Mx.map(x => (x._1, (x._2, x._3))).groupBy(_._1).map(x => {
        var bias = DenseVector.zeros[Double](k)
        for((j, vx) <- x._2){
          var tmp = u(x._1,::).inner
          var tmp2 = v(j, ::).inner
          for(i <- 0 to k){
            tmp.update(i, tmp(i)*(Y(x._1, j) - vx)*alpha)
            tmp2.update(i, lambda*tmp2(i)*alpha)
          }
          tmp.:+=(tmp2)
          bias.:+=(tmp)
        }
        (x._1, bias)
      })
    }
    em_u()

  }
}
*/