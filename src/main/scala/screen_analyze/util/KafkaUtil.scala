package screen_analyze.util

import scala.collection.JavaConversions._
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import kafka.utils.ZkUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.log4j.Logger

//读取kafka数据，更新offset
class KafkaUtil extends Serializable{
  private var kafkaP: Map[String, Object] = null
  private var zkUtils: ZkUtils = null
  private var topic: Array[String] = null
  def this(kafkaP: Map[String, Object], zkUtils: ZkUtils, topic: Array[String]) {
    this()
    this.kafkaP = kafkaP
    this.zkUtils = zkUtils
    this.topic = topic
  }
 
  //获取kafka连接
  def getConnect(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
  KafkaUtils.createDirectStream[String, String](
 ssc,
 PreferConsistent,
 ConsumerStrategies.Subscribe[String, String](topic, kafkaP))
  }
  
}