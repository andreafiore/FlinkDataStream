import java.text.SimpleDateFormat
import java.util.Date

object SensorData {

  case class SensorData(date: Date,
                        temperature: Double,
                        humidity: Double,
                        light: Double,
                        co2: Double,
                        humidityRatio: Double,
                        occupancy: Int)

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parseFromCsvLine(line: String): SensorData = {

    val lines: Array[String] = line.split(",")

    lines.foreach( line => {
      println(line)
    })

    val date = sdf.parse(lines(1).substring(1, lines(1).length - 1))
    val temp = lines(2).trim.toDouble
    val humidity = lines(3).trim.toDouble
    val light = lines(4).trim.toDouble
    val co2 = lines(5).trim.toDouble
    val ratio = lines(6).trim.toDouble
    val occupancy = lines(7).trim.toInt

    println(date)

    SensorData(date, temp, humidity, light, co2, ratio, occupancy)
  }
}
