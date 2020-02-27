import SensorData.SensorData
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.util.Collector


class SensorDataAllWindowProcessor extends ProcessAllWindowFunction[SensorData, Double, TimeWindow] {

  implicit val typeInfo: TypeInformation[Double] = createTypeInformation[Double]

  override def process(context: Context, elements: Iterable[SensorData], out: Collector[Double]): Unit = {
    val avgTemp = calculateAverageTemperature(elements.toList)
    out.collect(avgTemp)
  }

  def calculateAverageTemperature(elements: List[SensorData]) = {
    var sum: Double = 0
    var count: Int = 0

    elements.foreach(sd => {
      println(s"ID ${sd.id}, TEMPERATURE ${sd.temperature}")
      sum += sd.temperature
      count += 1
    })
    sum / count
  }
}


