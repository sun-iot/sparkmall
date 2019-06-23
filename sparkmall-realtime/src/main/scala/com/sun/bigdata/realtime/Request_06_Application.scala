package com.sun.bigdata.realtime

import com.sun.bigdata.sparkmall.common.model.MyKafkaMessage
import com.sun.bigdata.sparkmall.common.util.{DateUtil, KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * title: Request_06_Application 
  * projectName sparkmall 
  * description: 每天各地区 top3 热门广告
  * author Sun-Smile 
  * create 2019-06-22 16:19 
  */
object Request_06_Application {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Request_06_Application").setMaster("local[*]")
    val ssc = new StreamingContext(conf , Seconds(5))
    ssc.sparkContext.setCheckpointDir("cp")
    // 先从Kafka中获取数据
    val kafkaInputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log",ssc)
    // 将获取到的Kafka的数据存入 MyKafkaMessage类中
    val kafkaMessageDStream: DStream[MyKafkaMessage] = kafkaInputDStream.map {
      record => {
        val message: String = record.value()
        val datas: Array[String] = message.split(" ")
        MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    }
    // 对Kafka中获取的数据，进行解析
    val kafkaMessageMap: DStream[(String, Long)] = kafkaMessageDStream.map {
      kafkaInput => {
        // 解析时间戳
        val date: String = DateUtil.formatTime(kafkaInput.timestamp.toLong, "yyyy-MM-dd")
        // 将解析的数据进行重新组合 （date_area_city_adid , 1L）
        (date + "_" + kafkaInput.area + "_" + kafkaInput.city + "_" + kafkaInput.adid, 1L)
      }
    }
    // 将转换结构后的数据进行聚合 ： (date-area-city-adv, sum)
    val kafkaMessageSum: DStream[(String, Long)] = kafkaMessageMap.updateStateByKey {
      (seq: Seq[Long], option: Option[Long]) => {
        val sum: Long = option.getOrElse(0L) + option.sum
        Option(sum)
      }
    }
    // (date-area-city-adv, sum) => (date-area-adv, sum)
    val kafkaMessageMapDS: DStream[(String, Long)] = kafkaMessageSum.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1) + "_" + keys(3), sum)
      }
    }
    // 对得到的结果再次做聚合操作
    val kafkaMapReduce: DStream[(String, Long)] = kafkaMessageMapDS.reduceByKey(_+_)
    // 对聚合后的数据进行结构转换：(date-area-adv, totalSum) =>  (date-area, (adv, totalSum))
    val dateAreaToAdvSum: DStream[(String, (String, Long))] = kafkaMapReduce.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1), (keys(2), sum))
      }
    }
    // 对转换后的数据进行分组
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdvSum.groupByKey()
    // 开始取前3名
    val resultDStream: DStream[(String, List[(String, Long)])] = groupDStream.mapValues {
      datas => {
        datas.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }
      }
    }
    // 将结果保存到redis里面
    resultDStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          datas=>{
            val inner: Jedis = RedisUtil.getJedisClient
            datas.foreach(
              data=>{
                val keys: Array[String] = data._1.split("_")
                val date: String = keys(0)
                val area: String = keys(1)
                val map: List[(String, Long)] = data._2
                // list => json
                // [{},{},{}]
                // {"xx":10}
                import org.json4s.JsonDSL._
                val listJson: String = JsonMethods.compact(JsonMethods.render(map))
                inner.hset("top3_ads_per_day:"+date , area , listJson)
              }

            )
            inner.close()
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }
}
