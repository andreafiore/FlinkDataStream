

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val sensorDataSink = new SensorDataSink()

    val dataStream = Stream.createDataStream("src/main/resources/data.csv")
    Stream.createTumblingEventTimeWindowsStream(dataStream).addSink(sensorDataSink)

    Stream.execute()
  }

}
