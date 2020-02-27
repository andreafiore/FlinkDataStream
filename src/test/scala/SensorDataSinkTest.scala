import java.util.Date

import org.junit.{Assert, Test}

class SensorDataSinkTest {

  @Test
  def invokeTest() = {
    val sink = new SensorDataSink()
    Assert.assertEquals(0, sink.size)
    val sensorData = SensorData.SensorData(1, new Date(), 12.0, 34.5, 10.2, 45.5, 56.0, 1)
    sink.invoke(sensorData)

    Assert.assertEquals(1, sink.size)
  }

  @Test
  def clearSinkTest() = {
    val sink = new SensorDataSink()
    val sensorData1 = SensorData.SensorData(1, new Date(), 12.0, 34.5, 10.2, 45.5, 56.0, 1)
    val sensorData2 = SensorData.SensorData(2, new Date(), 14.0, 12.3, 14.5, 17.5, 55.0, 0)

    sink.invoke(sensorData1)
    sink.invoke(sensorData2)

    Assert.assertEquals(2, sink.size)
    sink.clearSink()
    Assert.assertEquals(0, sink.size)
  }

}
