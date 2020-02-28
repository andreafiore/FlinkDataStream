import java.util.Date

import org.junit.{Assert, Before, Test}

class SensorDataWindowProcessorTest {

  val processor = new SensorDataAllWindowProcessor()
  var list: List[SensorData.SensorData] = Nil

  @Before
  def init(): Unit = {
    val sd1 = SensorData.SensorData(1, new Date(), 12.0, 34.5, 10.2, 45.5, 56.0, 1)
    val sd2 = SensorData.SensorData(2, new Date(), 14.0, 12.3, 14.5, 17.5, 55.0, 0)
    val sd3 = SensorData.SensorData(3, new Date(), 10.0, 11.2, 13.5, 14.5, 50.0, 0)
    val sd4 = SensorData.SensorData(4, new Date(), 15.5, 11.5, 13.3, 14.5, 50.0, 1)
    val sd5 = SensorData.SensorData(5, new Date(), 17, 13, 11, 20.5, 50.0, 1)

    list = List(sd1, sd2, sd3, sd4, sd5)
  }


  @Test
  def calculateAverageValueTest(): Unit = {
    val avgTemp = processor.calculateAverageValue(list.map(sd => sd.temperature))

    Assert.assertEquals(13.7,avgTemp , 0.001)
  }

  @Test
  def calculateAverageOccupancy(): Unit = {
    val actualAvgOccupancy = processor.calculateAverageOccupancy(list.map(sd => sd.occupancy))

    Assert.assertEquals(1, actualAvgOccupancy)
  }

  @Test
  def averageSensorDataTest(): Unit = {
    val avgSensorData = processor.averageSensorData(list)

    Assert.assertEquals(5, avgSensorData.id)
    Assert.assertEquals(list(4).date, avgSensorData.date)
    Assert.assertEquals(13.7, avgSensorData.temperature, 0.001)
    Assert.assertEquals(16.5, avgSensorData.humidity, 0.001)
    Assert.assertEquals(12.5, avgSensorData.light, 0.001)
    Assert.assertEquals(22.5, avgSensorData.co2, 0.001)
    Assert.assertEquals(52.2, avgSensorData.humidityRatio, 0.001)
    Assert.assertEquals(1, avgSensorData.occupancy)
  }

}
