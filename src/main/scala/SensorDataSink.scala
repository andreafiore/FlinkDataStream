import SensorData.SensorData
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ArrayBuffer

object SensorDataSink {

  val values: ArrayBuffer[SensorData] = scala.collection.mutable.ArrayBuffer[SensorData]()
}

  class SensorDataSink extends SinkFunction[SensorData] {


  override def invoke(value: SensorData): Unit = {
    synchronized {
      SensorDataSink.values += value
    }
  }

  def size: Int = {
    SensorDataSink.values.size
  }

  def clearSink() = SensorDataSink.values.clear()
}

