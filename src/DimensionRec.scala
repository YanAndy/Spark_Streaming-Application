/**
 * Created by sgf on 2016/4/29.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

object DimensionRec {
  def main(args: Array[String]) {
    /*val sc = new SparkContext("local[2]","DimensionRec")
    val path ="D:/Dataset/lfw"
    val rdd = sc.wholeTextFiles(path)
    val files = rdd.map { case (fileName, content) => fileName.replace("file:", "") }
    println(files.first)
    println(files.count)
    */


    /*
    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <path/to/edges>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Load graph")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)*/
   // val sc = new SparkContext("local[2]","DimensionRec")

    /*val edges: RDD[Edge[String]] =
      sc.textFile("D:/Dataset/node.txt").map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(2).toLong, fields(1))
        //Edge(fields(0).toLong, fields(1).toLong)
      }

    val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

    println("num edges = " + graph.numEdges)
    println("num vertices = " + graph.numVertices)
    println(graph.degrees)
*/
    /*
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    val defaultUser = ("John Doe", "Missing")
    val graph = Graph(users, relationships, defaultUser)
    */





    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)

    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    //***********************************************************************************
    //***************************  图的属性    ****************************************
    //**********************************************************************************         println("***********************************************")
    println("属性演示")
    println("**********************************************************")
    println("找出图中年龄大于30的顶点：")
    graph.vertices.filter { case (id, (name, age)) => age > 30}.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    //边操作：找出图中属性大于5的边
    println("找出图中属性大于5的边：")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
    println("列出边属性>5的tripltes：")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    //Degrees操作
    println("找出图中最大的出度、入度、度数：")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    println

    //***********************************************************************************
    //***************************  转换操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点age + 10：")
    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e=>e.attr*2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //***********************************************************************************
    //***************************  结构操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点年纪>30的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println


    //***********************************************************************************
    //***************************  连接操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}

    //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
    }

    println("连接图的属性：")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    println

    println("出度和入读相同的人员：")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
    println

    //***********************************************************************************
    //***************************  聚合操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出年纪最大的追求者：")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.mapReduceTriplets[(String, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
      // 得到最大追求者，reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )

    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str)}
    println

    //***********************************************************************************
    //***************************  实用操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出5到各顶点的最短：")
    val sourceId: VertexId = 5L // 定义源点
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {  // 计算权重
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // 最短距离
    )
    println(sssp.vertices.collect.mkString("\n"))


    /***************************************************************************************/

    //PageRank
    //读入数据文件
    val articles: RDD[String] = sc.textFile("D:/BaiduYunDownload/Download/data/class9/graphx-wiki-vertices.txt")
    val links: RDD[String] = sc.textFile("D:/BaiduYunDownload/Download/data/class9/graphx-wiki-edges.txt")

    //装载顶点和边
    val vertices1 = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }

    val edges = links.map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }

    //cache操作
    //val graph = Graph(vertices, edges, "").persist(StorageLevel.MEMORY_ONLY_SER)
    val graph1 = Graph(vertices1, edges, "").persist()
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

    titleAndPrGraph1.vertices.top(10) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))



    /***********************************************************/
   /* val edge=List(//边的信息
      (1,2),(1,3),(2,3),(3,4),(3,5),(3,6),
      (4,5),(5,6),(7,8),(7,9),(8,9))*/
    val edge :RDD[String]= sc.textFile("D:/Dataset/testdata/10000_references.txt")
    def hexToInt(s: String): Long = {
      s.toList.map{ i => "0123456789ABCDEF".indexOf(i)}.reduceLeft(_ * 16 + _)
    }
    //构建边的rdd
    val edgeRdd=edge.map(line => line.split("\t")).map( parts => Edge(hexToInt(parts.head), hexToInt(parts.last), None))

    //构建图 顶点Int类型
    val g=Graph.fromEdges(edgeRdd, 0)

    println("/*****************************************/")
    g.degrees.collect.take(20).foreach(println(_))

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
