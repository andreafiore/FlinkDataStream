import org.apache.log4j.Logger

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val logger: Logger = Logger.getLogger("SensorDataJob")

    val sensorDataSink = new SensorDataSink()

    val dataStream = SensorDataStream.createDataStream("src/main/resources/data.csv")
    SensorDataStream.createTumblingEventTimeWindowsStream(dataStream).addSink(sensorDataSink)

    SensorDataStream.execute()

    SensorDataSink.values.foreach( sensorData => {
      logger.info(s"Sensor Data $sensorData")
    })
  }

}
