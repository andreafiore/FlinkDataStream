import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.{Assert, Test}

class SensorDataJobTest {

  implicit val typeInfo: TypeInformation[SensorData.SensorData] = TypeInformation.of(classOf[SensorData.SensorData])

  @Test
  def sensorDataJobTest(): Unit = {
    val dataStream = Stream.createDataStream("src/test/resources/data_test.csv")

    val sensorDataSink = new SensorDataSink()

    dataStream.assignTimestampsAndWatermarks(new SensorDataTimestampExtractor(Time.minutes(2)))
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
      .process(new SensorDataAllWindowProcessor())
      .addSink(sensorDataSink)

    Stream.execute()

    Assert.assertEquals(3, sensorDataSink.size)
  }

}
