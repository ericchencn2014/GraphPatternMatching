import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.compat.Platform.currentTime


/**
  * @version 1.0
  * @author Jinghui Lu
  * @define
  *     This program will compute the global degree centrality of a graph, and also compute the local degree centrality,indegree, out degree and normalized degree centrality of each vertex
  *
  *     progress:
  *         load data
  *         using pregel function to make a parallel computing
  *         finished calculation of degree centrality
  * */

object Degree_centrality {
  /**
    * main function
    **/
  def main(args: Array[String]): Unit = {
    println("enter main!!!!!!!!!!")

    /** turn off the log information and set master */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
//      .setMaster("local")
      .set("spark.akka.frameSize", "2000")
      .set("spark.driver.maxResultSize","50g").setAppName("Degree_centrality")


    val sc = new SparkContext(conf)

    val str:String = "/home/eric/SparkBench/s14.csv"

//    val mid_graph = GraphLoader.edgeListFile(sc, str, numEdgePartitions = 6)
    val mid_graph = GraphLoader.edgeListFile(sc, args(0) , numEdgePartitions = args(1).toInt)

    val vertices_sum = mid_graph.vertices.count().toInt //total number of vertices

    val graph = mid_graph.mapVertices{ (v,attr) =>

      var degree:Int = 0
      var indegree:Int = 0
      var outdegree:Int = 0
      var sum:Int = vertices_sum
      var normalized:Double = 0.0

      (degree,indegree,outdegree,sum,normalized)


    }

    /**
      *
      * param of pregel
      *
      * the first para (Int) represents the degree of each vertices
      * the second para (Int) represents the indegree of each vertices
      * the third para (Int) represents the outdegree of each vertices
      * the forth para(Int) represents the sum of vertices
      * the fifth para(Double) represents the normalized degree of vertices
      */
    val seconds=10000
    val mid_dual_sec:Double = currentTime

    val centrality = graph.pregel((0, 0, 0,vertices_sum,0.0),
      maxIterations = 1, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
    val mid_end_sec:Double = currentTime
    println("process cost(seconds): "+ (mid_end_sec-mid_dual_sec)/1000)
    Thread.sleep(seconds*1000)
    println("Wake up")

    /** output the result of local degree centrality of each vertex */
    //    centrality.vertices.collect().foreach(x=>println("Vertice id:"+ x._1 +" Normalized degree centrality:"+x._2._5+" Degree centrality:" + x._2._1 + " Indegree:" + x._2._2+" Outdegree:" + x._2._3))

//    /** invoke GlobalDC to calculate global degree centrality */
//    val global_degreeCentrality = GlobalDC(centrality)
//
//    /** output the result of graph degree centrality */
//    println("The global degree centrality is "+global_degreeCentrality)


  }



  /**
    * loadData function: load data from csv file and convert into graph type.
    *
    * */
  def loadData(path: String, sc: SparkContext): Graph[(Int,Int,Int,Int,Double), PartitionID] ={
    //load from file
    val raw: RDD[Edge[Int]] = sc.textFile(path).map{s =>
      val parts = s.split("\\s+")
      Edge(parts(0).toLong, parts(1).toLong, 1)
    }.distinct

    val convert : RDD[Edge[Int]] = raw.filter{ s =>
      s.srcId != s.dstId
    }

    //build graph
    val raw_graph : Graph[(Int,Int,Int,Int,Double), PartitionID] =
      Graph.fromEdges(convert, (0,0,0,0,0.0))

    raw_graph.cache()
  }

  /**
    *
    * This method process the calculation result of various degree centrality
    * the first para (Int) represents the degree of each vertices
    * the second para (Int) represents the indegree of each vertices
    * the third para (Int) represents the outdegree of each vertices
    * the forth para(Int) represents the sum of vertices
    * the fifth para(Double) represents the normalized degree of vertices
    *
    */
  def vertexProgram(id: VertexId, attr: (Int, Int, Int, Int, Double),
                    msg: (Int, Int, Int, Int, Double)):
  (Int, Int, Int, Int, Double) = {

    val normalized_degree = msg._1.toDouble/(msg._4-1).toDouble
    val mergedMessage = (msg._1, msg._2, msg._3, msg._4, normalized_degree)
    mergedMessage
  }

  /**
    *
    * This method process calculation of various degree centrality
    * degree centrality counts the number of edges that a node has
    * indegree centrality counts the number of income edges of a node
    * outdegree centrality counts the number of out-going edges of a node
    * normalized degree centrality is teh degree centrality divided by (n-1),n is the number of nodes
    */
  def sendMessage(edge: EdgeTriplet[(Int, Int, Int,Int, Double), Int]):
  Iterator[(VertexId, (Int, Int, Int,Int, Double))] = {

    val src = (1, 0, 1, edge.srcAttr._4, 0.0)
    val dst = (1, 1, 0, edge.dstAttr._4, 0.0)
    val itSrc = Iterator((edge.srcId, src))
    val itDst = Iterator((edge.dstId, dst))
    itSrc ++ itDst
  }

  /**
    *
    * This function use mergeDegree method to combine degrees
    */
  def messageCombiner(msg1: (Int, Int, Int, Int, Double),
                      msg2: (Int, Int, Int, Int, Double)):
  (Int, Int, Int, Int, Double) ={

    val degree = mergeDegree(msg1._1, msg2._1)
    val inDegree = mergeDegree(msg1._2, msg2._2)
    val outDegree = mergeDegree(msg1._3, msg2._3)
    val vertices_sum = msg1._4
    val normalized_degree = msg1._5
    (degree, inDegree, outDegree, vertices_sum, normalized_degree)
  }

  /**
    *
    * return the sum of two Integer
    */
  def mergeDegree(a:Int, b:Int): Int={
    a+b
  }

  /**
    * This function calculate global degree centrality of a graph
    *
    */
  def GlobalDC(graph: Graph[(Int, Int, Int, Int, Double), PartitionID]):Double = {
    var a :Array[Int] = Array() //array for saving degree centrality of all nodes
    var deviation = 0.0
    var max: Int = Int.MinValue
    val vertices_sum = graph.vertices.count().toDouble // the number of nodes
    var globaldc = 0.0



    // get degree centrality of each node
    for( x <- graph.vertices.collect()) {
      a :+= x._2._1

    }

    // find the largest degree centrality
    max = a.max

    // compute the sum of divation
    deviation = Deviation(a,max)

    // compute the global degree centrality
    globaldc = ComputeGlobalDC(deviation,vertices_sum)
    globaldc

  }


  /**
    *
    * compute and return the deviation
    */
  def Deviation(a: Array[Int], max: Int):Double={
    var dvtion = 0.0

    for (x<-a){
      dvtion = dvtion + (max-x)
    }
    dvtion
  }

  /**
    *
    * compute and return the globaldc
    */
  def ComputeGlobalDC(deviation:Double,vertices:Double):Double={

    var gdc = 0.0

    gdc = deviation/(vertices-1)/(vertices-2)

    gdc

  }



}