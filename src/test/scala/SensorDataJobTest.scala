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
    val dataStream = Stream.createDataStream("src/test/resources/data_test.csv")
    Stream.createTumblingEventTimeWindowsStream(dataStream).addSink(sink)
    Stream.execute()

    Assert.assertEquals(3, sink.size)
  }

  @Test
  def slidingEventTimeWindowTest() = {
    val dataStream = Stream.createDataStream("src/test/resources/slidingEventTimeWindows_data.csv")
    Stream.createSlidingEventTimeWindow(dataStream).addSink(sink)
    Stream.execute()

    SensorDataSink.values.toIterable.foreach( value => {
      println(value)
    })

    Assert.assertEquals(5, sink.size)
  }

}
