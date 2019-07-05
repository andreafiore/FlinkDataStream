
import SensorData.SensorData
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Stream {



  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  implicit val typeInfo = TypeInformation.of(classOf[SensorData])

  val stream: DataStream[SensorData] = env.readTextFile("src/main/resources/data.csv")
    .map(line => SensorData.parseFromCsvLine(line))




}
