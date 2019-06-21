package com.sun.bigdata.sparkmall.offline.req

import com.sun.bigdata.sparkmall.common.model.UserVisitAction
import com.sun.bigdata.sparkmall.common.util.{ConfigUtils, StrUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * title: Request_01_Application
  * projectName sparkmall
  * description: 对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
  * author Sun-Smile
  * create 2019-06-19 12:54
  */
object Request_03_Application {
  // TODO 需求2 ：对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
  private val conf: SparkConf = new SparkConf().setAppName("Request_01_Application").setMaster("local[*]")
  private val sessionSpark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

  import sessionSpark.implicits._

  def main(args: Array[String]): Unit = {
    // 设置持久化目录
    sessionSpark.sparkContext.setCheckpointDir("cp")

    // 从Hive中获取表格数据
    sessionSpark.sql("use " + ConfigUtils.getValueConfig("hive.database"))
    var sql = "select * from user_visit_action where 1 = 1 "
    val startDate: String = ConfigUtils.getValueCondition("startDate")
    val endDate: String = ConfigUtils.getValueCondition("endDate")

    if (StrUtils.isNotEmpty(startDate)) {
      sql = sql + " and date >='" + startDate + "'"
    }
    if (StrUtils.isNotEmpty(endDate)) {
      sql = sql + " and date <='" + endDate + "'"
    }
    val dataFrame: DataFrame = sessionSpark.sql(sql)
    //    dataFrame.show
    // 转化成DataSet
    val ds: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd

    /*******************************************************************************************************************
    需求三
    ******************************************************************************************************************/
    // RDD在处理中会分成两部分进行处理，每部分处理都会导致RDD从Hive表进行获取数据，因此需要对RDD进行持久化,不设置会导致程序的效率低下
    // 需要在 程序开始位置进行设置持久化目录 sessionSpark.sparkContext.setCheckpointDir("cp")
    // 在获取到需要持久化的RDD后，进行持久化
    userVisitActionRDD.checkpoint()

//    1. 从行为表中获取数据（pageid）
//    2. 对数据进行筛选过滤，保留需要统计的页面数据
    // 得到我们的过滤条件
    val conditionFilter: Array[String] = ConfigUtils.getValueCondition("targetPageFlow").split(",")
    // 为方便后面过滤，我们将我们的过滤条件转换成这种形式（1-2，2-3，3-4，4-5，5-6，6-7）
    val pageConditionArray: Array[String] = conditionFilter.zip(conditionFilter.tail).map {
      case (page1, page2) => {
        (page1 + "-" + page2)
      }
    }
    
    // 得到我们的page_id
    val pageRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      action => {
        conditionFilter.contains(action.page_id.toString)
      }
    )

//    3. 将页面数据进行结构的转换（pageid, 1）
    val pageMapRDD: RDD[(Long, Long)] = pageRDD.map(
      action => {
        (action.page_id, 1L)
      }
    )

//    4. 将转换后的数据进行聚合统计（pageid, sum）(分母)
    val pageReduceRDD: RDD[(Long, Long)] = pageMapRDD.reduceByKey(_+_)
    val pageMap: Map[Long, Long] = pageReduceRDD.collect().toMap

    // TODO 计算分子
//    5. 从行为表中获取数据，使用session进行分组（sessionid, Iterator[ (pageid , action_time)]）
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(
      action => {
        action.session_id
      }
    )

//    6. 对分组后的数据进行时间排序（升序）
//    7. 将排序后的页面ID，两两结合：（1-2，2-3，3-4）
    val pageZipFlatMapRDD: RDD[(Long, Long)] = sessionGroupRDD.map {
      case (session, datas) => {
        // 针对datas进行排序
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        // 开始进行两两结合
        val pageIds: List[Long] = actions.map(
          action => {
            action.page_id
          }
        )
        // 使用 zip 拉链实现两两结合
        pageIds.zip(pageIds.tail)
      }
    }.flatMap(list => list)
    pageZipFlatMapRDD

//    8. 对数据进行筛选过滤（1-2，2-3，3-4，4-5，5-6，6-7）
    val pageFilterRDD: RDD[(Long, Long)] = pageZipFlatMapRDD.filter {
      pages => {
        pageConditionArray.contains(pages._1 + "-" + pages._2)
      }
    }
    
//    9. 对过滤后的数据进行结构的转换：（pageid1-pageid2, 1）
    val pageFilterMapRDD: RDD[(String, Long)] = pageFilterRDD.map {
      case (page1, page2) => {
        (page1 + "-" + page2, 1L)
      }
    }
//    10. 对转换结构后的数据进行聚合统计：（pageid1-pageid2, sum1）(分子)
    val pageFlowToSumRDD: RDD[(String, Long)] = pageFilterMapRDD.reduceByKey(_+_)
//    11. 查询对应的分母数据 （pageid1, sum2）
//    12. 计算转化率 ： sum1 / sum2
    pageFlowToSumRDD.foreach{
      case(pageFlow , sum)=>{
        // 分子 1-2 ， sum1
        val molecular : String = pageFlow.split("-")(0)
        // 获取分母
        val denominator: Long = pageMap.getOrElse(molecular.toLong,1L)
        println(pageFlow + "=" + (molecular.toDouble / denominator))
       }
    }

    sessionSpark.stop()

  }
}
