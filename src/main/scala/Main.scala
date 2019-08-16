object Main {
  def main(args: Array[String]): Unit = {

    DBSCAN.DBSCANStreamingStart(
      "./data/dbscan_input.dat",
      1.0,
      0.01,
      4,
      2,
      10,
      "localhost",
      8080,
      1,
      "Scatter-Result"
    )

  }
}
