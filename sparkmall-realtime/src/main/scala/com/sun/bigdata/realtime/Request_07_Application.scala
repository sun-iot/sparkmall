package com.sun.bigdata.realtime

import com.sun.bigdata.sparkmall.common.model.MyKafkaMessage
import com.sun.bigdata.sparkmall.common.util.{DateUtil, KafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * title: Request_07_Application 
  * projectName sparkmall 
  * description: 统计最近一分钟的广告点击的趋势,使用窗口函数
  * author Sun-Smile 
  * create 2019-06-23 10:30 
  */
object Request_07_Application {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Request_07_Application").setMaster("local[*]")
    val ssc = new StreamingContext(conf , Seconds(5))



    // 从Kafka获取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log",ssc)
    // 把数据存放在MyKafkaMesage中
    val kafkaMessage: DStream[MyKafkaMessage] = kafkaDStream.map {
      reconds => {
        val message: String = reconds.value()
        val messages: Array[String] = message.split(" ")
        MyKafkaMessage(messages(0), messages(1), messages(2), messages(3), messages(4))
      }
    }
    // 一分钟的，使用window窗口函数
    val kafkaWindowDStream: DStream[MyKafkaMessage] = kafkaMessage.window(Seconds(60),Seconds(10))

    val dateDStream: DStream[(String, Long)] = kafkaWindowDStream.map {
      message => {
        var date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd HH:mm:SS")
        date = date.substring(0, date.length - 1) + "0"
        (date, 1L)
      }
    }
    val dateReduce: DStream[(String, Long)] = dateDStream.reduceByKey(_+_)

    // 按照时间进行排序
    val sortDate: DStream[(String, Long)] = dateReduce.transform(
      rdd => {
        rdd.sortByKey()
      }
    )
    sortDate.print()
    ssc.start()
    ssc.awaitTermination()










  }
}
