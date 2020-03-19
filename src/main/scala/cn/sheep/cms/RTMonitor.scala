package cn.sheep.cms

import java.lang

import cn.sheep.utils.{JedisUtils, Utils}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs
object RTMonitor {
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    //创建kafka相关参数
    val load = ConfigFactory.load("application")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> load.getString("metadata.broker.list"),
      "group.id" -> load.getString("group.id"),
      "auto.offset.reset" -> "smallest"
    )

    val topics = load.getString("kafka.topics").split(",").toSet

    //SteamingContext
    //从kafka读取数据--获取偏移量
    //结果存放在redis中

    val conf = new SparkConf().setMaster("local[*]").setAppName("实时统计")
    val ssc = new StreamingContext(conf, Seconds(2))
    //加载数据库配置信息
    DBs.setup()
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select * from streamingoffset".map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partition")), rs.long("offset"))
      }).list().apply()
    }.toMap

    val stream = if (fromOffsets.size == 0) {
      //假设程序是第一次启动
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    else {
      //程序非第一次启动
      var checkedoffset = Map[TopicAndPartition, Long]()
      val kafkaCluster = new KafkaCluster(kafkaParams)
      val earlistLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      if (earlistLeaderOffsets.isRight) {
        val topicAndPartitionToOffset = earlistLeaderOffsets.right.get
        //开始对比
        checkedoffset = fromOffsets.map(owner => {
          val clusterEarliestOffset = topicAndPartitionToOffset.get(owner._1).get.offset
          if (owner._2 >= clusterEarliestOffset) {
            owner
          } else {
            (owner._1, clusterEarliestOffset)
          }
        })
      }


      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedoffset, messageHandler)
    }
    //处理数据
    //获取到了stream流
    stream.foreachRDD(rdd => {
      //rdd.foreach(println)
      //获取偏移量的兑现
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //实时报表--业务概况
      /**
       * 订单的数量，充值金额，成功率，平均时长
       */
      val jsonObj: RDD[JSONObject] = rdd.map(line => {
        JSON.parseObject(line._2)
      }).filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))

      jsonObj.map(js => {
        val result = js.getString("businessRst")
        val fee: Double = if (result.equals("0000")) js.getDouble("chargefee") else 0
        val isSucc = if (result.equals("0000")) 1 else 0
        val receiveTime = js.getString("receiveNotifyTime")
        val startTime = js.getString("requestId")
        val costTime = if (result.equals("0000")) Utils.calculateRqt(startTime, receiveTime) else 0
        ("A-" + startTime.substring(0, 8), List[Double](1, isSucc, fee, costTime.toDouble))
      }).reduceByKey((list1, list2) => {
        (list1 zip list2) map (x => x._1 + x._2)
      }).foreachPartition(itr => {
        //存储到redis中
        val client = JedisUtils.getJedisClient()
        itr.foreach(tp => {
          client.hincrBy(tp._1, "total", tp._1(0).toLong)
          client.hincrBy(tp._1, "succ", tp._1(1).toLong)
          client.hincrByFloat(tp._1, "money", tp._1(2))
          client.hincrBy(tp._1, "timer", tp._1(3).toLong)
          client.expire(tp._1, 60 * 60 * 24 * 2)

        })
        client.close()
      }
      )

      //记录偏移量
      offsetRanges.foreach(osr => {
        println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
      })
    })

    //程序启动
    //启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()

  }

}
