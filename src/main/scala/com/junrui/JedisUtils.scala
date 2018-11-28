package com.junrui

import java.util

import redis.clients.jedis.{Jedis, Tuple}

object JedisUtils {


  def exsitKeyRedis(key:String) = {
    val jedis = new Jedis("192.168.200.21",6379)
    //储存
    jedis.select(1)
    val flag = jedis.exists(key)
    jedis.close()
    flag
  }

  def saveRedis(key:String,value: String): Unit = {
    val jedis = new Jedis("192.168.200.21",6379)
    jedis.select(1)
    //储存
    jedis.set(key,value)
    jedis.close()
//    println("存入redis成功！")
  }

  def incrValueRedis(key:String): Unit = {
    val jedis = new Jedis("192.168.200.21",6379)
    jedis.select(1)
    //储存
    jedis.incr(key)
    jedis.close()
  }

  def incrZSetRedis(key:String,userId:String): Unit = {
    val jedis = new Jedis("192.168.200.21",6379)
    jedis.select(1)
    //ZSet的累加方法zincrby(key,n,key)
    jedis.zincrby(key,1,userId)
    jedis.close()
  }

  def zrevrangeWithSoresRedis(key:String)= {
    val jedis = new Jedis("192.168.200.21",6379)
    jedis.select(1)
    //ZSet的累加方法zincrby(key,n,key)
    //倒序，取出最大的10个值
    val tuples = jedis.zrevrangeWithScores(key,0,9)
    //遍历
//    tuples.forEach()//不能用这个方法，不是foreach()
    //用迭代器遍历,首先获得Iterator
    val value = tuples.iterator()
    var map: Map[String, Int] = Map()
    while (value.hasNext){
      val tuple: Tuple = value.next()
//      println(tuple.getElement+"||"+tuple.getScore)
//      map=Map(tuple.getElement->tuple.getScore.toString)
      //map增加值，不是map赋值!!!!!!!!!!
      map+=(tuple.getElement->tuple.getScore.toInt)
    }
    jedis.close()
    map
  }

//  def main(args: Array[String]): Unit = {
//    val map: Map[String, Int] = zrevrangeWithSoresRedis("black_list20181009")
//    for((k,v)<-map){
//      println("用户名："+k+"|点击量："+v)
//    }
//  }
}
