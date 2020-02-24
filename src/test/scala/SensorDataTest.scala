
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Calendar

import SensorData.SensorData
import org.junit.{Assert, Test}

class SensorDataTest {

  var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  @Test
  def sensorDataConstructorTest(): Unit = {
    val date = sdf.parse("2020-02-15 11:35:30")
    val sensorData: SensorData = SensorData.SensorData(1, date, 25, 26.5, 585.5, 750.2, 0.005231, 1)
    assertSensorData(sensorData)(1, 25, 26.5, 585.5, 750.2, 0.005231, 1)
  }

  @Test
  def parseDoubleFromStringTest(): Unit = {
    val doubleString = "123.45"
    val d = SensorData.parseDoubleFromString(doubleString)
    Assert.assertEquals(123.45, d, 0)
  }

  @Test
  def parseIntFromStringTest(): Unit = {
    val intString = "123"
    val n = SensorData.parseIntFromString(intString)
    Assert.assertEquals(123, n)
  }

  @Test
  def parseDateFromStringTest(): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val s = s""""2020-02-15 11:30:35""""
    val expectedDate = sdf.parse(s.substring(1, s.length - 1))
    val date = SensorData.parseDateFromString(s)
    Assert.assertEquals(expectedDate, date)

  }

  @Test
  def parseCSVLineTest() = {
    val csvLine = s""""141","2015-02-02 14:19:59",23.718,26.29,578.4,760.4,0.00477266099212519,1"""
    val sensorData = SensorData.parseFromCsvLine(csvLine)
    assertSensorData(sensorData)(141, 23.718, 26.29, 578.4, 760.4, 0.00477266099212519, 1)
  }

  private def assertSensorData(sensorData: SensorData)(id: Int, temperature: Double, humidity: Double, light: Double, co2: Double, humidityRatio: Double, occupancy: Int) = {
    Assert.assertEquals(id, sensorData.id, 0)
    Assert.assertEquals(temperature, sensorData.temperature, 0)
    Assert.assertEquals(humidity, sensorData.humidity, 0)
    Assert.assertEquals(light, sensorData.light, 0)
    Assert.assertEquals(co2, sensorData.co2, 0)
    Assert.assertEquals(humidityRatio, sensorData.humidityRatio, 0)
    Assert.assertEquals(occupancy, sensorData.occupancy, 0)
  }
}
