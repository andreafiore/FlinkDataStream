
import org.apache.logging.log4j.LogManager

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val executor = new SensorDataJobExecutor

    executor.execute()

    SensorDataSink.values.foreach( sd => {
      executor.logger.info(s"SENSOR DATA AGGREGATED: $sd")
    })
  }
}

class SensorDataJobExecutor {

  val logger = LogManager.getLogger(classOf[SensorDataJobExecutor])

  def execute() = {
    val sensorDataSink = new SensorDataSink()

    val dataStream = SensorDataStream.createDataStream("src/main/resources/data.csv")
    SensorDataStream.createTumblingEventTimeWindowsStream(dataStream).addSink(sensorDataSink)

    SensorDataStream.execute()
  }
}
