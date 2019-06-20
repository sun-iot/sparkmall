package com.sun.bigdata.sparkmall.offline.req

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.sun.bigdata.sparkmall.common.model.UserVisitAction
import com.sun.bigdata.sparkmall.common.util.{ConfigUtils, StrUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/**
  * title: Request_01_Application
  * projectName sparkmall
  * description:  获取点击、下单和支付数量排名前 10 的品类
  * author Sun-Smile
  * create 2019-06-19 12:54
  */
object Request_01_Application {
  // TODO 需求1 ： 获取点击、下单和支付数量排名前 10 的品类
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
        CategoryTOP_10(UUID.randomUUID().toString, category, clickCount, orderCount, payCount)
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
    val driver: String = ConfigUtils.getValueConfig("jdbc.driver.class")
    val url: String = ConfigUtils.getValueConfig("jdbc.url")
    val user: String = ConfigUtils.getValueConfig("jdbc.user")
    val password: String = ConfigUtils.getValueConfig("jdbc.password")
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url , user , password)
    val sqlMySQL = "insert into category_top10 values (?,?,?,?,?)"
    val statement: PreparedStatement = connection.prepareStatement(sqlMySQL)
    sortList.foreach(
      data=>{
        statement.setString(1,data.taskId)
        statement.setString(2,data.categoryId)
        statement.setLong(3,data.clickCount)
        statement.setLong(4,data.orderCount)
        statement.setLong(5,data.payCount)
        statement.executeUpdate()
      }
    )
    statement.close()
    session.stop()

  }
}
case class CategoryTOP_10 ( taskId:String, categoryId:String, clickCount:Long, orderCount:Long, payCount:Long )
// 自定义一个累加器
class CategoryAccumlator extends AccumulatorV2[String,mutable.HashMap[String, Long]]{
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
