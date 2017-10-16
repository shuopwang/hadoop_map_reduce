// Databricks notebook source
import sys.process._
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!

// COMMAND ----------

val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/") 
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))

// COMMAND ----------

val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")

// COMMAND ----------

//There is mothode 2
import scala.collection.mutable.WrappedArray

println("The total hash_tags")
val hashtags_set=df.select($"entities.hashtags".getField("text")).rdd.flatMap(row=>(row.getAs[WrappedArray[String]](0)))
hashtags_set.collect()

// COMMAND ----------

println("The hash tags count:")
val tags_count=hashtags_set.map(tag=>(tag.toLowerCase(),1)).reduceByKey((a,b) => a+b)
tags_count.collect()

// COMMAND ----------

val sorted_Tags = tags_count.map{ case (k,v) => (v,k) }.sortByKey(false)
val topKTags = sorted_Tags.map{ case (v,k) => (k,v) }
println("The top 10 hash tags:")
topKTags.take(10).foreach(println)

// COMMAND ----------

val usr_set=df.select($"user.screen_name").rdd.map(row => row.getString(0))
usr_set.collect()

// COMMAND ----------

val usr_count=usr_set.map(tag=>(tag.toLowerCase(),1)).reduceByKey((a,b) => a+b)

// COMMAND ----------

val sorted_usr = usr_count.map{ case (k,v) => (v,k) }.sortByKey(false)
val topKusr = sorted_usr.map{ case (v,k) => (k,v) }
println("The top 10 user:")
topKusr.take(10).foreach(println)

// COMMAND ----------

import scala.collection.mutable.WrappedArray
val time_tag = df.select($"created_at",$"entities.hashtags.text").rdd.flatMap(i=>{val time= i.getString(0).split(" ")
                                                                             val month = time(1).toString
                                                                             val day =time(2).toInt
                                                                             val year = time(5).toInt
                                                                             i.getAs[WrappedArray[String]](1).map(tag=>((month+'/'+day+'/'+year),tag.toLowerCase()))})



// COMMAND ----------

import org.apache.spark.sql.functions.explode
val time_tag_df=time_tag.toDF()
//tag_time.show()

// COMMAND ----------

val real_tag_df=time_tag_df.groupBy($"_1",$"_2").count()
real_tag_df.registerTempTable("test")
display(sqlContext.sql("select * from test"))

// COMMAND ----------


