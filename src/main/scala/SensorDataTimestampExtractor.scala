import SensorData.SensorData
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class SensorDataTimestampExtractor(time: Time) extends BoundedOutOfOrdernessTimestampExtractor[SensorData](time) {

  override def extractTimestamp(t: SensorData): Long = t.date.getTime

}
