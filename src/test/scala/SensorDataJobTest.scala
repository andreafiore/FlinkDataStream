import org.apache.flink.api.common.typeinfo.TypeInformation
import org.junit.{Assert, Test}

class SensorDataJobTest {

  implicit val typeInfo: TypeInformation[SensorData.SensorData] = TypeInformation.of(classOf[SensorData.SensorData])

  @Test
  def sensorDataJobTest(): Unit = {

    val sensorDataSink = new SensorDataSink()

    val dataStream = Stream.createDataStream("src/test/resources/data_test.csv")

    Stream.createTumblingEventTimeWindowsStream(dataStream).addSink(sensorDataSink)

    Stream.execute()

    Assert.assertEquals(3, sensorDataSink.size)
  }

}
