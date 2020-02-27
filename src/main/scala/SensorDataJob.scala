
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SensorDataJob {

  def main(args: Array[String]): Unit = {

    val dataStream = Stream.createDataStream("src/main/resources/data.csv")

    dataStream.assignTimestampsAndWatermarks(new SensorDataTimestampExtractor(Time.minutes(2)))
      .windowAll(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
        .process(new SensorDataAllWindowProcessor())


    Stream.execute()

  }

}
