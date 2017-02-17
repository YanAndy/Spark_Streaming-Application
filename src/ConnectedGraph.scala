/**
  * Created by sgf on 2016/5/9.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object ConnectedGraph {
  def main(args:Array[String])
  {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("connect componentV2").setMaster("local")
    val sc = new SparkContext(conf)

    val dataSource=List("1 1","2 1","2 2","3 2",
      "4 3","5 3","5 4","6 4","6 5","7 5",
      "8 7","9 7","9 8","9 9","10 8","11 9")

    val rdd=sc.parallelize(dataSource).map { x=>{
      val data=x.split(" ")
      (data(0).toLong,data(1).toInt)
    }}.cache()

    //提取顶点
    val vertexRdd=rdd.groupBy(_._1).map(x=>{(x._1,x._2.unzip._2.min)})

    //提取边
    val edgeRdd=rdd.groupBy(_._2).flatMap(x=>{
      val vertexList=x._2.toList.unzip._1
      val ret=ListBuffer[Edge[Option[Int]]]()
      for(i<- 0 until vertexList.size;
          j<-i+1 until vertexList.size;
          if j<vertexList.size)
      {
        ret.append(Edge(vertexList(i),vertexList(j),None))
      }

      ret
    })

    //构成图
    val graph=Graph(vertexRdd,edgeRdd)
    println("init graph")
    graph.triplets.collect().foreach(println(_))

    //进行pregel计算
    val newG=graph.pregel(Int.MaxValue, 10000, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
    println("after pregel")
    newG.triplets.collect().foreach(println(_))

    println("connect component")
    newG.vertices.groupBy(_._2).map(_._2.unzip._1).collect().foreach(println(_))
  }

  /**
    * 节点数据的更新 就是取最小值
    */
  def vprog(vid:VertexId,vdata:Int,message:Int):Int=Math.min(vdata,message)

  /**
    * 发送消息
    */
  def sendMsg(e:EdgeTriplet[Int, Option[Int]])={
    if(e.srcAttr==e.dstAttr)
      Iterator.empty//迭代停止
    else{
      //哎，EdgeDirection.Either好像根本没效果，只能在这里发送双向来模拟无向图
      Iterator((e.dstId,e.srcAttr),
        (e.srcId,e.dstAttr))//将自己发送给邻接顶点
    }
  }

  /**
    * 合并消息
    */
  def mergeMsg(a:Int,b:Int):Int=Math.min(a, b)

}
