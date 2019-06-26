import SensorData.SensorData
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable.ArrayBuffer

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val environment = Stream.env

    SensorDataSink.values.clear()
    val dataStream: DataStream[SensorData] = Stream.stream

    dataStream.addSink(new SensorDataSink())

    environment.execute()
    

    println(SensorDataSink.values)
  }

  object SensorDataSink {

    val values: ArrayBuffer[SensorData] = new ArrayBuffer[SensorData]()
  }

  class SensorDataSink extends SinkFunction[SensorData] {

    override def invoke(value: SensorData): Unit = {
      synchronized {
        SensorDataSink.values += value
      }
    }

  }

}
