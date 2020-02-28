
import SensorData.SensorData
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Stream {

  implicit val typeInfo = TypeInformation.of(classOf[SensorData])

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  def createDataStream(filePath: String): DataStream[SensorData] = {
    env.readTextFile(filePath).map(line => SensorData.parseFromCsvLine(line))
    .assignTimestampsAndWatermarks(new SensorDataTimestampExtractor(Time.minutes(2)))
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
      .process(new SensorDataAllWindowProcessor())
  }

  def execute(): Unit = {
    env.execute("Sensor Data Stream")
  }

}
