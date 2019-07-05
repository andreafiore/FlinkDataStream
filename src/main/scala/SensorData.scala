import java.text.SimpleDateFormat
import java.util.Date

object SensorData {

  case class SensorData(id:Int,
                        date: Date,
                        temperature: Double,
                        humidity: Double,
                        light: Double,
                        co2: Double,
                        humidityRatio: Double,
                        occupancy: Int)

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parseFromCsvLine(line: String): SensorData = {

    val lines: Array[String] = line.split(",")

    val id = lines(0).substring(1, lines(0).length - 1).toInt
    val date = sdf.parse(lines(1).substring(1, lines(1).length - 1))
    val temp = lines(2).trim.toDouble
    val humidity = lines(3).trim.toDouble
    val light = lines(4).trim.toDouble
    val co2 = lines(5).trim.toDouble
    val ratio = lines(6).trim.toDouble
    val occupancy = lines(7).trim.toInt

    SensorData(id, date, temp, humidity, light, co2, ratio, occupancy)
  }
}