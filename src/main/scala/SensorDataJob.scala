
import java.lang

import SensorData.SensorData
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val env = Stream.env

    val dataStream = Stream.stream

    SensorDataSink.values.clear()

    implicit val typeInfo = TypeInformation.of(classOf[SensorData])
    implicit val typeInfoInt = TypeInformation.of(classOf[Int])
    implicit val typeInfoDouble = TypeInformation.of(classOf[Double])


    /*
      this code is to partition the stream by key
     */
//    dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorData](Time.minutes(2)) {
//      override def extractTimestamp(t: SensorData): Long = t.date.getTime
//    })
//        .keyBy(_.occupancy)
//        .window(SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(10)))
//        .process(new SensorDataWindowProcessor())


    dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorData](Time.minutes(2)) {
      override def extractTimestamp(t: SensorData): Long = t.date.getTime
    }).windowAll(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
        .process(new SensorDataAllWindowProcessor())

    env.execute("Sensor Data Stream job")

  }


  object SensorDataSink {
    var values: ArrayBuffer[SensorData] = scala.collection.mutable.ArrayBuffer[SensorData]()
  }

  class SensorDataSink extends SinkFunction[SensorData] {

    override def invoke(value: SensorData): Unit = {
      synchronized {
        SensorDataSink.values += value
      }
    }
  }


  class SensorDataWindowProcessor extends org.apache.flink.streaming.api.scala.function.ProcessWindowFunction[SensorData, Double, Int, TimeWindow] {
    override def process(key: Int, context: Context, elements: Iterable[SensorData], out: Collector[Double]): Unit = {
      var sum: Double = 0
      var count = 0

      elements.foreach( sd => {

        println(s"ID ${ sd.id }, TEMPERATURE ${ sd.temperature }")
        sum += sd.temperature

        count+= 1
      })

      val avgTemp = sum / count

      println(s"AVERAGE TEMPERATURE IS ${ avgTemp }")

      out.collect(avgTemp)
    }
  }

  class SensorDataAllWindowProcessor extends org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction[SensorData, Double, TimeWindow] {
    override def process(context: Context, elements: Iterable[SensorData], out: Collector[Double]): Unit = {

      var sum: Double = 0
      var count = 0

      elements.foreach(sd => {
        println(s"ID ${ sd.id }, TEMPERATURE ${ sd.temperature }")

        sum += sd.temperature
        count+= 1
      })

      val avgTemp = sum / count

      println(s"AVERAGE TEMPERATURE IS ${ avgTemp }")

      out.collect(avgTemp)
    }
  }

}
