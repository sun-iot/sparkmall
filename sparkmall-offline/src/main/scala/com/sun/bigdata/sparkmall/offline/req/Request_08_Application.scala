package com.sun.bigdata.sparkmall.offline.req

import com.sun.bigdata.sparkmall.common.model.UserVisitAction
import com.sun.bigdata.sparkmall.common.util.{ConfigUtils, DateUtil, StrUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * title: Request_08_Application 
  * projectName sparkmall 
  * description: 统计每个页面平均停留时间
  * author Sun-Smile 
  * create 2019-06-23 11:03 
  */
object Request_08_Application {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Request_08_Application").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._

    // 从Hive数据库中获取数据
    sparkSession.sql("use " + ConfigUtils.getValueConfig("hive.database"))
    var sql = "select * from user_visit_action where 1=1 "
    val startDate: String = ConfigUtils.getValueCondition("startDate")
    val endDate: String = ConfigUtils.getValueCondition("endDate")

    if (StrUtils.isNotEmpty(startDate)){
      sql = sql + "and date >='" + startDate + "'"
    }
    if (StrUtils.isNotEmpty(endDate)){
      sql = sql + "and date <= '" + endDate + "'"
    }
    val df: DataFrame = sparkSession.sql(sql)
    val ds: Dataset[UserVisitAction] = df.as[UserVisitAction]
    val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd

    // 分组，按照session
    val sessionGroup: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action=>action.session_id)
    // 将分组后的数据使用时间进行排序，保证按照页面的真正流转顺序
    val sessionListRDD: RDD[(String, List[(Long, Long)])] = sessionGroup.mapValues {
      datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // 排序号进行map
        val pageTimeList: List[(Long, String)] = actions.map(
          action => {
            (action.page_id, action.action_time)
          }
        )

        // 对页面流转的操作进行拉链处理
        val pageZip: List[((Long, String), (Long, String))] = pageTimeList.zip(pageTimeList.tail)

        // 对拉链后的数据进行计算
        pageZip.map {
          case (page1, page2) => {
            val pageTime: Long = DateUtil.getTimestamp(page1._2, "yyyy-MM-dd HH:mm:SS")
            val page2Time: Long = DateUtil.getTimestamp(page2._2, "yyyy-MM-dd HH:mm:SS")
            (page1._1, (page2Time - pageTime))
          }
        }
      }
    }
    val listRDD: RDD[List[(Long, Long)]] = sessionListRDD.map {
      case (session, list) => list
    }
    val pageTimeRDD: RDD[(Long, Long)] = listRDD.flatMap(list=>list)

    // 对计算结果进行修改
    val pageGroupRDD: RDD[(Long, Iterable[Long])] = pageTimeRDD.groupByKey()

    // 获取最终结果
    pageGroupRDD.foreach{
      case(pageid , datas)=>{
        println(pageid + "=" + (datas.sum / datas.size))
      }
    }
    sparkSession.close()
  }
}
