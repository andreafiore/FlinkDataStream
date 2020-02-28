import SensorData.SensorData
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.util.Collector


class SensorDataAllWindowProcessor extends ProcessAllWindowFunction[SensorData, SensorData.SensorData, TimeWindow] {
  implicit val typeInfo: TypeInformation[SensorData.SensorData] = createTypeInformation[SensorData.SensorData]


  override def process(context: Context, elements: Iterable[SensorData], out: Collector[SensorData.SensorData]): Unit = {
    val avgSensorData = averageSensorData(elements.toList)

    out.collect(avgSensorData)
  }

  def calculateAverageValue(elements: List[Double]) = {
    var sum: Double = 0

    elements.foreach(elem => {
      sum += elem
    })
    sum / elements.size
  }

  def calculateAverageOccupancy(occupancies: List[Int]): Int = {
    var sum: Double = 0

    occupancies.foreach(occ => {
      sum += occ
    })

    Math.round(sum/occupancies.size).asInstanceOf[Int]
  }

  def averageSensorData(list: List[SensorData]): SensorData.SensorData = {
    val reverseSortedList = list.sortBy(_.id).reverse
    val id = reverseSortedList.head.id
    val date = reverseSortedList.head.date
    val temperatureAvg = calculateAverageValue(reverseSortedList.map(_.temperature))
    val humidityAvg = calculateAverageValue(reverseSortedList.map(_.humidity))
    val lightAvg = calculateAverageValue(reverseSortedList.map(_.light))
    val co2Avg = calculateAverageValue(reverseSortedList.map(_.co2))
    val humidityRatioAvg = calculateAverageValue(reverseSortedList.map(_.humidityRatio))
    val occupancyAvg = calculateAverageOccupancy(reverseSortedList.map(_.occupancy))

    new SensorData.SensorData(id, date,temperatureAvg, humidityAvg, lightAvg, co2Avg, humidityRatioAvg, occupancyAvg)
  }
}


