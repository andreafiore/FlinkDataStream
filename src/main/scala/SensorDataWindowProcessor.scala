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
    val sortedList = list.sortBy(_.id).reverse
    val id = sortedList.head.id
    val date = sortedList.head.date
    val temperatureAvg = calculateAverageValue(sortedList.map(_.temperature))
    val humidityAvg = calculateAverageValue(sortedList.map(_.humidity))
    val lightAvg = calculateAverageValue(sortedList.map(_.light))
    val co2Avg = calculateAverageValue(sortedList.map(_.co2))
    val humidityRatioAvg = calculateAverageValue(sortedList.map(_.humidityRatio))
    val occupancyAvg = calculateAverageOccupancy(sortedList.map(_.occupancy))

    new SensorData.SensorData(id, date,temperatureAvg, humidityAvg, lightAvg, co2Avg, humidityRatioAvg, occupancyAvg)
  }
}


