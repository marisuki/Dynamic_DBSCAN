object Main {
  def main(args: Array[String]): Unit = {
    //val st = System.currentTimeMillis()
    DBSCAN.DBSCANStreamingStart(
      "./data/cluster5.data",
      4.0,
      0.01,
      50,
      2,
      1,
      "localhost",
      8080,
      1,
      "Scatter-Result"
    )

  }
}
