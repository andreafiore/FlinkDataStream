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

  def parseFromCsvLine(line: String): SensorData = {

    val tokens: Array[String] = line.split(",")

    val id = parseIntFromString(tokens(0).substring(1, tokens(0).length - 1))
    val date = parseDateFromString(tokens(1).substring(1, tokens(1).length - 1))
    val temp = parseDoubleFromString(tokens(2))
    val humidity = parseDoubleFromString(tokens(3))
    val light = parseDoubleFromString(tokens(4))
    val co2 = parseDoubleFromString(tokens(5))
    val ratio = parseDoubleFromString(tokens(6))
    val occupancy = parseIntFromString(tokens(7))

    SensorData(id, date, temp, humidity, light, co2, ratio, occupancy)
  }

  def parseIntFromString(str: String): Int = {
    str.trim.toInt
  }

  def parseDoubleFromString(str: String): Double = {
    str.trim.toDouble
  }

  def parseDateFromString(str: String): Date = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(str.substring(1, str.length - 1))
  }

}