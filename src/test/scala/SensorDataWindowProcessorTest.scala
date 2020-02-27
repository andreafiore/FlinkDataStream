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

    list = List(sd1, sd2, sd3, sd4)
  }


  @Test
  def calculateAverageTemperatureTest(): Unit = {

    var sumTemp: Double = 0

     list.foreach( sd => {
      sumTemp = sumTemp + sd.temperature
    })
    val expectedAvg = sumTemp / list.size
    val avgTemp = processor.calculateAverageTemperature(list)

    Assert.assertEquals(expectedAvg, avgTemp, 0.001)
  }

}
