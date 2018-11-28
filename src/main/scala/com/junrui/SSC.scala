package com.junrui


import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SSC {

  def main(args: Array[String]): Unit = {
    //修改控制台显示，简化，注意import是log4j
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //构造得到SSC,分区必须大于1
    val conf = new SparkConf().setMaster("local[2]").setAppName("black")
    val ssc = new StreamingContext(conf,Seconds(5))
    //从kafka拿到数据
    //createStream()将数据存在内存中，有丢失危险,使用createDirectStream()
    val topicsSet="project".split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"->"team01master:9092,team01slave01:9092,team01slave02:9092")
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
    //打印
   // kafkaDS.print()
    //格式转换,foreachRDD()里面的RDD可以再分
    kafkaDS.map(data=>{
//      val data1: (String, String) = data
      val strings: Array[String] = data._2.split("\t")
      (strings(0),strings(1))
    }).foreachRDD(dataRDD=>{
      //分区，多进程
      dataRDD.foreachPartition(data=>{
        data.foreach(da=>{
          //构造返回的key结构,key不相同
          //da._1.substring(0,8)为天数
//          val key = "black_list" + da._1.substring(0,8) + da._2
//          //判断key是否存在，存在则加1，否则则新建
//            val flag = JedisUtils.exsitKeyRedis(key)
//          if(flag){
//            JedisUtils.incrValueRedis(key)
//          }else{
//            JedisUtils.saveRedis(key, "1")
//          }
          //由于存在多进程,此种方法可能会造成脏写不能用

          //重构key，key为Zset,减少了key的数量
          val key = "black_list" + da._1.substring(0,8)
//          println(key)
          //将数据储存进redis的Zset
          JedisUtils.incrZSetRedis(key,da._2)
          //取出黑名单前10个用户
          val map: Map[String, Int] = JedisUtils.zrevrangeWithSoresRedis(key)
          for((k,v)<-map){
           println("用户名："+k+"|点击量："+v)
          }
        })
      })
    })
    //启动和关闭
    ssc.start()
    ssc.awaitTermination()
  }

}
