package com.sun.bigdata.realtime

import java.util

import com.sun.bigdata.sparkmall.common.model.MyKafkaMessage
import com.sun.bigdata.sparkmall.common.util.{DateUtil, KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * title: Request_05_Application
  * projectName sparkmall
  * description: 广告黑名单实时统计
  * author Sun-Smile
  * create 2019-06-21 18:39
  */
object Request_05_Application {
  def main(args: Array[String]): Unit = {
    // 准备配置对象
    val sparkConf = new SparkConf().setAppName("Request_05_Application").setMaster("local[*]")
    // 构建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setCheckpointDir("cp")

    val kafkaDream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log" , ssc)
    val messageKafkaDream: DStream[MyKafkaMessage] = kafkaDream.map {
      record => {
        val message: String = record.value()
        val datas: Array[String] = message.split(" ")
        MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    }
    messageKafkaDream.print()

    // 对数据进行结构的转换 (date:area:city:ads, 1)
    val messageADSCount: DStream[(String, Long)] = messageKafkaDream.map(
      message => {
        val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.area + "_" + message.city + "_" + message.adid  , 1L)
      }
    )
    // 将转换结构后的数据进行聚合：(date-adv-user, sum)
    val messageADSCountSum: DStream[(String, Long)] = messageADSCount.updateStateByKey(
      (seq: Seq[Long], option: Option[Long]) => {
        val sum: Long = option.getOrElse(0L) + option.sum
        Option(sum)
      }
    )
    // 对聚合的结果进行阈值的判断
    messageADSCountSum.foreachRDD(
      result=>{
        result.foreachPartition{
          data=>{
            val resultClient: Jedis = RedisUtil.getJedisClient
            data.foreach{
              case (key , sum)=>{
                //val keys: Array[String] = key.split("_")
                resultClient.hset("date:area:city:ads", key ,sum.toString)
                resultClient.close()
              }
            }
          }
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
