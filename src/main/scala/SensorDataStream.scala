
import SensorData.SensorData
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object SensorDataStream {


  implicit val typeInfo = TypeInformation.of(classOf[SensorData])

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  def createDataStream(filePath: String): DataStream[SensorData] = {
    env.readTextFile(filePath).map(line => SensorData.parseFromCsvLine(line))
  }

  def createTumblingEventTimeWindowsStream(dataStream: DataStream[SensorData.SensorData]): DataStream[SensorData.SensorData] = {
    dataStream.assignTimestampsAndWatermarks(new SensorDataTimestampExtractor(Time.minutes(2)))
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
      .process(new SensorDataAllWindowProcessor())
  }

  def createSlidingEventTimeWindow(dataStream: DataStream[SensorData]): DataStream[SensorData.SensorData] = {
    dataStream.assignTimestampsAndWatermarks(new SensorDataTimestampExtractor(Time.minutes(2)))
      .timeWindowAll(Time.minutes(10), Time.minutes(5))
      .process(new SensorDataAllWindowProcessor())
  }
  def execute(): Unit = {
    env.execute("Sensor Data Stream")
  }

}
