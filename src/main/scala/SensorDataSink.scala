import SensorData.SensorData
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ArrayBuffer


class SensorDataSink extends SinkFunction[SensorData] {

  private var values: ArrayBuffer[SensorData] = scala.collection.mutable.ArrayBuffer[SensorData]()

  override def invoke(value: SensorData): Unit = {
    synchronized {
      values += value
    }
  }

  def size: Int = {
    values.size
  }

  def clearSink() = values.clear()
}

