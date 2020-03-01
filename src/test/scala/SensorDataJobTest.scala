import org.apache.flink.api.common.typeinfo.TypeInformation
import org.junit.{Assert, Before, Test}

class SensorDataJobTest {

  implicit val typeInfo: TypeInformation[SensorData.SensorData] = TypeInformation.of(classOf[SensorData.SensorData])
  val sink = new SensorDataSink

  @Before
  def init() = {
    sink.clearSink()
  }


  @Test
  def tumblingEventTimeWindowsTest() = {
    val dataStream = SensorDataStream.createDataStream("src/test/resources/data_test.csv")
    SensorDataStream.createTumblingEventTimeWindowsStream(dataStream).addSink(sink)
    SensorDataStream.execute()

    Assert.assertEquals(3, sink.size)
  }

  @Test
  def slidingEventTimeWindowTest() = {
    val dataStream = SensorDataStream.createDataStream("src/test/resources/slidingEventTimeWindows_data.csv")
    SensorDataStream.createSlidingEventTimeWindow(dataStream).addSink(sink)
    SensorDataStream.execute()

    SensorDataSink.values.toIterable.foreach( value => {
      println(value)
    })

    Assert.assertEquals(5, sink.size)
  }

}
