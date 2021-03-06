package screen_analyze
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.kafka010.{ OffsetRange, HasOffsetRanges, KafkaUtils }
import kafka.utils.{ ZKGroupTopicDirs, ZkUtils }
import org.apache.kafka.common.serialization.StringDeserializer
import screen_analyze.util.KafkaUtil
import java.util.Properties
//import org.apache.spark.streaming.Duration
//import screen_analyze.util.RedisUtil
//import scala.reflect.internal.util.Statistics.Counter

object Driver {
  def main(args: Array[String]): Unit = {
    val driverProps = new Properties()
    // 获取配置文件
    val driverConfig = getClass.getResourceAsStream("/driver.properties")
    driverProps.load(driverConfig)
    //spark
    val master = driverProps.getProperty("master")
    val appname = driverProps.getProperty("appname")
    val checkpointDir = driverProps.getProperty("checkpointDir")
    val streamingtime = driverProps.getProperty("streamingtime").toInt
    //zookeeper
    val zkUrl = driverProps.getProperty("zkUrl")
    val sessionTimeout = driverProps.getProperty("sessionTimeout").toInt
    val connectionTimeout = driverProps.getProperty("connectionTimeout").toInt
    //kafka
    val bootstrap_servers = driverProps.getProperty("bootstrap_servers")
    val auto_offset_reset = driverProps.getProperty("auto_offset_reset")
    val group_id = driverProps.getProperty("group_id")
    val topic = driverProps.getProperty("topic")
    //redis
    val keytimeout = driverProps.getProperty("keytimeout")
    val esTime = driverProps.getProperty("esTime")
    val anyTime = driverProps.getProperty("anyTime")
    val sparkConf = new SparkConf().setMaster(master).setAppName(appname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.streaming.backpressure.enabled","true")
//      .set("spark.streaming.stopGracefullyOnShutdown","true")
//      .set("spark.streaming.backpressure.initialRate","200")
      .set("spark.streaming.kafka.maxRatePerPartition","100")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(streamingtime))
//    ssc.checkpoint(checkpointDir)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrap_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> auto_offset_reset,
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics = Array(topic)

    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
    val kaf = new KafkaUtil(kafkaParams, zkUtils, topics)
    val data = kaf.getConnect(ssc)
    val update = new DataUpdate(esTime,anyTime,keytimeout.toInt,topic)
    update.updateToAnalyze(data)
    ssc.start()
    ssc.awaitTermination()

  }
}