
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val dataStream = Stream.createDataStream("src/main/resources/data.csv")

    val sink = new SensorDataSink()
    dataStream.assignTimestampsAndWatermarks(new SensorDataTimestampExtractor(Time.minutes(2)))
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
        .process(new SensorDataAllWindowProcessor())
        .addSink(sink)

    Stream.execute()

    println(SensorDataSink.values.size)

  }

}
