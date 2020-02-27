import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.{Assert, Test}


class SensorDataTimestampExtractorTest {

  val extractor = new SensorDataTimestampExtractor(Time.minutes(2))

  @Test
  def extractTimestampTest(): Unit = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = sdf.parse("2020-02-25 15:25:30")
    val sensorData = SensorData.SensorData(1, date, 20, 30, 200, 50, 0.5, 0)

    val timestamp: Long = extractor.extractTimestamp(sensorData)

    Assert.assertEquals(date.getTime, timestamp)
  }

}
