/**
  * Created by sgf on 2016/5/12.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
object CitationTree {
  def main(args: Array[String]) {

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
     val edge=List(//边的信息
   (1,2),(1,3),(2,3),(3,4),(3,5),(3,6),
   (4,5),(5,6),(7,8),(7,9),(8,9))
    //val edge :RDD[String]= sc.textFile("D:/Dataset/testdata/10000_references.txt")
    /*
    def hexToInt(s: String): Long = {
      s.toList.map{ i => "0123456789ABCDEF".indexOf(i)}.reduceLeft(_ * 16 + _)
    }*/
    //构建边的rdd
    //val edgeRdd=edge.map(line => line.split("\t")).map( parts => Edge(hexToInt(parts.head), hexToInt(parts.last), None))
    val edgeRdd=sc.parallelize(edge).map( parts => Edge(parts._1.toLong, parts._2.toLong, None))
    //构建图 顶点Int类型
    val g=Graph.fromEdges(edgeRdd, 0)

    println("/*****************************************/")
    g.degrees.collect.foreach(println(_))

    type VMap=Map[VertexId,Int]

    /**
      * 节点数据的更新 就是集合的union
      */
    def vprog(vid:VertexId,vdata:VMap,message:VMap)
    :Map[VertexId,Int]=addMaps(vdata,message)

    /**
      * 发送消息
      */
    def sendMsg(e:EdgeTriplet[VMap, _])={

      //取两个集合的差集  然后将生命值减1
      val srcMap=(e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k->(e.dstAttr(k)-1) }.toMap
      val dstMap=(e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k->(e.srcAttr(k)-1) }.toMap

      if(srcMap.size==0 && dstMap.size==0)
        Iterator.empty
      else
        Iterator((e.dstId,dstMap),(e.srcId,srcMap))
    }

    /**
      * 消息的合并
      */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1.keySet ++ spmap2.keySet).map {
        k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
      }.toMap


    val two=2  //这里是二跳邻居 所以只需要定义为2即可
    val Three=3
    val newG=g.mapVertices((vid,_)=>Map[VertexId,Int](vid->two))
      .pregel(Map[VertexId,Int](), 3, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    println("/*****************************************/")
    newG.vertices.collect().foreach(println(_))

    //过滤得到二跳邻居 就是value=0 的顶点
    val twoJumpFirends=newG.vertices.mapValues(_.filter(_._2==0).keys)

    println("/*****************************************/")
    twoJumpFirends.collect().take(20).foreach(println(_))






    sc.stop()

  }

}
