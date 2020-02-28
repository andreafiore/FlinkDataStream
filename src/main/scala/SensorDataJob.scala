

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val sink = new SensorDataSink()

    Stream.createDataStream("src/main/resources/data.csv").addSink(sink)

    Stream.execute()
  }

}
