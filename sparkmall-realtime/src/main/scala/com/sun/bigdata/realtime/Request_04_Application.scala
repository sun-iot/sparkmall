package com.sun.bigdata.realtime

import java.util

import com.sun.bigdata.sparkmall.common.util.{DateUtil, KafkaUtil, RedisUtil}
import com.sun.bigdata.sparkmall.common.model.MyKafkaMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * title: Request_04_Application 
  * projectName sparkmall 
  * description: 广告黑名单实时统计
  * author Sun-Smile 
  * create 2019-06-21 18:39 
  */
object Request_04_Application {
  def main(args: Array[String]): Unit = {
    // 准备配置对象
    val sparkConf = new SparkConf().setAppName("Request_04_Application").setMaster("local[*]")
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

    // TODO 0. 对原始数据进行筛选（黑名单数据过滤）
    // 获取redis的客户端

    // 取出不包含 userid
//    val messageFilterDream: DStream[MyKafkaMessage] = messageKafkaDream.filter(
//      message => {
//        !blacklist.contains(message.userid)
//      }
//    )

    val messageFilterDream: DStream[MyKafkaMessage] = messageKafkaDream.transform(
      rdd => {
        // Coding :Driver n
        val client: Jedis = RedisUtil.getJedisClient
        val blacklist: util.Set[String] = client.smembers("blacklist")
        // Driver => object => Driver
        // 使用广播变量来实现序列化操作
        val blacklistBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blacklist)
        client.close()

        rdd.filter(message => {
          // coding:Executor m
          !blacklistBroadcast.value.contains(message.userid)
        })
      }
    )

    // 对数据进行结构的转换 (date-adv-user, 1)
    val dateUserCountStream: DStream[(String, Long)] = messageFilterDream.map(
      message => {
        val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
        (date + "_" + message.adid + "_" + message.userid, 1L)
      }
    )
    // TODO 2. 将转换结构后的数据进行聚合：(date-adv-user, sum)
    val stateDream: DStream[(String, Long)] = dateUserCountStream.updateStateByKey {
      (seq: Seq[Long], option: Option[Long]) => {
        val sum: Long = option.getOrElse(0L) + seq.sum
        Option(sum)
      }

    }
    // TODO 3. 对聚合的结果进行阈值的判断
    stateDream.foreachRDD(
      rdd=>{
        rdd.foreach{
          case (key , sum)=>{
            if (sum >= 100 ){
              val keys: Array[String] = key.split("_")
              val inner: Jedis = RedisUtil.getJedisClient
              inner.sadd("blacklist" , keys(2))
              inner.close()
            }
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
