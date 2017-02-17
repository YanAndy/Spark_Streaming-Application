/**
  * Created by sgf on 2016/5/7.
  */
import org.apache.spark.graphx._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD


object ShortPath {
  def main(args: Array[String]) {
    //val sc = new SparkContext("local", "pregel test", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    // A graph with edge attributes containing distances
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)


    /**********************************************************************
    //初始化一个随机图，节点的度符合对数正态分布,边属性初始化为1
    val graph: Graph[VertexId, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
    graph.edges.foreach(println)
    val sourceId: VertexId = 4 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    //初始化各节点到原点的距离
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program，节点处理消息的函数，dist为原节点属性（Double），newDist为消息类型（Double）
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message，发送消息函数，返回结果为（目标节点id，消息（即最短距离））
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      //Merge Message，对消息进行合并的操作，类似于Hadoop中的combiner
      (a, b) => math.min(a, b)
    )

    println(sssp.vertices.collect.mkString("\n"))
    **************************************************************************/
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val referenceFile :RDD[String]= sc.textFile("D:/Dataset/testdata/10000_references.txt")
    val paperFile :RDD[String]= sc.textFile("D:/Dataset/testdata/10000_papers.txt")
    //val authorFile = sc.textFile("D:/Dataset/testdata/10000_authors.txt")

    val referenceRDD = referenceFile.map(line => line.split("\t"))
      .map( parts => Edge(hexToInt(parts.head), hexToInt(parts.last), 1)).collect

    //referenceRDD.take(20).foreach(println)

    val paperRDD = paperFile.map(line => line.split("\t"))
      .map( parts => (hexToInt(parts.head), (hexToInt(parts.last),parts.apply(1).toInt))).collect
    //paperRDD.take(20).foreach(println)

    val vertexRDD: RDD[(Long, (Long, Int))] = sc.parallelize(paperRDD)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(referenceRDD)

    val graph: Graph[(Long, Int), Int] = Graph(vertexRDD, edgeRDD)

    //Degrees操作
    //println("找出图中最大的出度、入度、度数：")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("找出图中最大的出度、入度、度数：")
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))


    //val TD = hexToInt("85740D9E")
    //println(TD)

    //println("InDegrees of paper 140251998: "+ graph.inDegrees)

/*
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点年份<1960的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 <= 1950)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._1} is ${v._2._2}"))
    println
    println("子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
*/

    /***********************************PageRank**************************************************/
    //装载顶点和边
    /*val vertices1 = paperRDD.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }

    val edges = links.map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }*/

    //cache操作
    //val graph = Graph(vertices, edges, "").persist(StorageLevel.MEMORY_ONLY_SER)

    val vertices = paperFile.map(line => line.split("\t"))
      .map( parts => (hexToInt(parts.head), hexToInt(parts.last).toString))
    val graph1 = Graph(vertices, edgeRDD, "").persist()
    //graph.unpersistVertices(false)

    //测试
    println("**********************************************************")
    println("获取5个triplet信息")
    println("**********************************************************")
    graph1.triplets.take(5).foreach(println(_))

    //pageRank算法里面的时候使用了cache()，故前面persist的时候只能使用MEMORY_ONLY
    println("**********************************************************")
    println("PageRank计算，获取最有价值的数据")
    println("**********************************************************")
    val prGraph = graph1.pageRank(0.001).cache()

    val titleAndPrGraph1 = graph1.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    titleAndPrGraph1.vertices.top(20) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._1 + " => " + t._2._2 + ": " + t._2._1))



    //***********************************************************************************
    //***************************  连接操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    case class User(name: Long, age: Int, inDeg: Int, outDeg: Int)

    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}

    //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
    }

    //println("连接图的属性：")
    //userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    //println

    println("入度大于30的Paper：")
    userGraph.vertices.filter {
      //case (id, u) => u.inDeg == u.outDeg
      case (id, u) => u.inDeg > 30
    }.collect.foreach {
      case (id, property) => println(property.name, property.age,property.inDeg)
    }
    println("/*****************/")

    userGraph.vertices.filter{case (id, u)=>id == 1775749144}.collect.foreach{
      case (id, property) => println(property.name, property.age,property.inDeg)
    }
    println

  }

  def hexToInt(s: String): Long = {
    s.toList.map{ i => "0123456789ABCDEF".indexOf(i)}.reduceLeft(_ * 16 + _)
  }


}
