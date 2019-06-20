package com.sun.bigdata.sparkmall.offline.req

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.sun.bigdata.sparkmall.common.model.UserVisitAction
import com.sun.bigdata.sparkmall.common.util.{ConfigUtils, StrUtils}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/**
  * title: Request_01_Application
  * projectName sparkmall
  * description: 对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
  * author Sun-Smile
  * create 2019-06-19 12:54
  */
object Request_02_Application {
  // TODO 需求2 ：对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
  private val conf: SparkConf = new SparkConf().setAppName("Request_01_Application").setMaster("local[*]")
  private val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  import session.implicits._

  def main(args: Array[String]): Unit = {
    // 从Hive中获取表格数据
    session.sql("use " + ConfigUtils.getValueConfig("hive.database"))
    var sql = "select * from user_visit_action where 1 = 1 "
    val startDate: String = ConfigUtils.getValueCondition("startDate")
    val endDate: String = ConfigUtils.getValueCondition("endDate")

    if (StrUtils.isNotEmpty(startDate)){
      sql = sql + " and date >='" + startDate + "'"
    }
    if (StrUtils.isNotEmpty(endDate)){
      sql = sql + " and date <='" + endDate + "'"
    }
    val dataFrame: DataFrame = session.sql(sql)
//    dataFrame.show
    // 转化成DataSet
    val ds: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd
    println(userVisitActionRDD.count())

    // 使用累加器将不同的品类的不同指标数据聚合在一起：(k-v)(categrory - 指标 。 SumCount)
    // 注册自定义一个累加器
    val accumlator = new CategoryAccumlator
    session.sparkContext.register(accumlator)

    //
    userVisitActionRDD.foreach(
      action=>{
        if (action.click_category_id != -1){
          accumlator.add(action.click_category_id+"-click")
        }else if (action.order_category_ids!=null){
          val order_category_ids_array: Array[String] = action.order_category_ids.split(" ")
          for (elem <- order_category_ids_array) {
            accumlator.add(elem+"-order")
          }
        }else if(action.pay_category_ids!=null){
          val pay_category_ids_array: Array[String] = action.pay_category_ids.split(",")
          for (elem <- pay_category_ids_array) {
            accumlator.add(elem+"-pay")
          }
        }
      }
    )
    val taskid: String = UUID.randomUUID().toString
    taskid
    // TODO 将聚合后的结果转化结构：(category-指标, SumCount) (category,(指标, SumCount))
    val categoryCountMap: mutable.HashMap[String, Long] = accumlator.value
    // 上一步与下一步直接一起写
    val categoryToCountMap: mutable.HashMap[String, (String, Long)] = accumlator.value.map {
      case (key, count) => {
        val keys: Array[String] = key.split("-")
        (keys(0), (keys(1), count))
      }
    }
    // TODO 将转换结构后的相同品类的数据分组在一起
    val groupByMap: Map[String, mutable.HashMap[String, (String, Long)]] = categoryToCountMap.groupBy {
      case (k, v) => {
        k
      }
    }
    // groupByMap.foreach(println)

    // map (10 -> (click, 30))
    // map (10 -> (order, 20))
    // map (10 -> (pay, 10))
    val categoryTOP_10: immutable.Iterable[CategoryTOP_10] = groupByMap.map {
      case (category, map) => {
        var clickCount = 0L
        var orderCount = 0L
        var payCount = 0L
        val maybeTuple: Option[(String, Long)] = map.get(category)
        val value: (String, Long) = maybeTuple.get

        if ("click".equals(value._1)) {
          clickCount = value._2
        } else if ("order".equals(value._1)) {
          orderCount = value._2
        } else if ("pay".equals(value._1)) {
          payCount = value._2
        }
        CategoryTOP_10(taskid, category, clickCount, orderCount, payCount)
      }
    }
    // TODO 根据品类的不同指标进行排序（降序）
    val sortList: List[CategoryTOP_10] = categoryTOP_10.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }
    // 取前10
    val top10: List[CategoryTOP_10] = sortList.take(10)
    /***************************************************************************************************************/
    // 需求二：对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
    /***************************************************************************************************************/
    val ids: List[String] = top10.map(_.categoryId)
    val idBro: Broadcast[List[String]] = session.sparkContext.broadcast(ids)
    // TODO 对需求1中的结果对原始数据进行过滤。得到排名在前10的品类
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          idBro.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )
    // TODO  将过滤后的数据进行结构的转换：（ category-sessionid, 1 ）
    val mapRDD: RDD[(String, Int)] = filterRDD.map(
      action => {
        (action.click_category_id + "-" + action.session_id, 1)
      }
    )
    // TODO 将转化结构后的数据进行聚合：（ category-sessionid, sum）
    val categorySessionIdToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    // TODO 将聚合后的数据进行结构转换：（ category-sessionid, sum）（ category,(sessionid, sum)）
    val categoryToSessionSumRDD: RDD[(String, (String, Int))] = categorySessionIdToSumRDD.map {
      case (categorySessionId, sum) => {
        val key: Array[String] = categorySessionId.split("-")
        (key(0), (key(1), sum))
      }
    }
    // TODO 将转换结构的数据进行分组：（category， Iterator[(sessionid, sum) ]）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = categoryToSessionSumRDD.groupByKey()

    // TODO 对分组后的数据进行排序，取前10名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      data => {
        data.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(10)
      }
    )
    val listRDD: RDD[List[CategoryTop10SessionTop10]] = resultRDD.map {
      case (category, list) => {
        list.map {
          case (sessions, sum) => {
            CategoryTop10SessionTop10(taskid, category, sessions, sum)
          }
        }
      }
    }
    val result: RDD[CategoryTop10SessionTop10] = listRDD.flatMap(list=>list)

    result.foreachPartition(datas=>{
      val driver: String = ConfigUtils.getValueConfig("jdbc.driver.class")
      val url: String = ConfigUtils.getValueConfig("jdbc.url")
      val user: String = ConfigUtils.getValueConfig("jdbc.user")
      val password: String = ConfigUtils.getValueConfig("jdbc.password")
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url , user , password)
      val sqlMySQL = "insert into category_top10_session_count values (?,?,?,?)"
      val statement: PreparedStatement = connection.prepareStatement(sqlMySQL)

      datas.foreach(
        data=>{
          statement.setString(1, data.taskId)
          statement.setString(2, data.categoryId)
          statement.setString(3, data.sessionId)
          statement.setLong(4, data.clickCount)
          statement.executeUpdate()
        }
      )
      statement.close()
      connection.close()
    })

    session.stop()

  }
}
case class CategoryTop10SessionTop10( taskId:String, categoryId:String, sessionId:String, clickCount:Long )
// 自定义一个累加器
class CategoryAccumlator02 extends AccumulatorV2[String,mutable.HashMap[String, Long]]{
  var map = new mutable.HashMap[String, Long]()
  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumlator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2){
      (inner , kv)=>{
        val k: String = kv._1
        val v: Long = kv._2
        inner(k) = inner.getOrElse(k , 0L) + v
        inner
      }
    }
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v,0L) + 1
  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}
