
import org.apache.logging.log4j.LogManager

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val executor = new SensorDataJobExecutor

    executor.init()

  }
}

class SensorDataJobExecutor {

  val logger = LogManager.getLogger(classOf[SensorDataJobExecutor])
  def init() = {
    val sensorDataSink = new SensorDataSink()

    val dataStream = SensorDataStream.createDataStream("src/main/resources/data.csv")
    SensorDataStream.createTumblingEventTimeWindowsStream(dataStream).addSink(sensorDataSink)

    SensorDataStream.execute()

    SensorDataSink.values.foreach( sensorData => {
      logger.info(s"Sensor Data $sensorData")
    })
  }
}
