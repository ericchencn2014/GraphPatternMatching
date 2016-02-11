/** @author  Chao Chen
  *  @version 1.0
  *  @date    Thu Aug 17 2015
  *  @see     LICENSE (MIT style license file).
  */

package GraphX

import java.text.SimpleDateFormat
import java.util.Calendar

import GraphX.GraphTypes._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import CommonUse._

import scala.util.control._
import scalation.graphalytics.SubgraphIso

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** This `TightSimulation` object provides an implementation for Distributed Tight Simulation
  *  the algorithm refer to http://cobweb.cs.uga.edu/~ar/papers/IJBD_final.pdf
  *  @param data  the data graph
  *  @param query  the query graph, this parameter is not necessary
  *  @param sc spark context
  *  @return the graph after dual simulation filter
  *
  *  @warnning this class should "extends java.io.Serializable", Otherwise it's unserializable
  */
class TightSimulation(data: Graph[SPMap,Int], query: Graph[String,Int],sc_root:SparkContext)extends java.io.Serializable {

  val sc = sc_root

  private val QueryVerticesArray = query.vertices.collect()
  private val QueryEdgesArray = query.edges.collect()
  private val QueryTripletsArray = query.triplets.collect()


  def apply_tight_simulation(model : Int, ball_num:Int) : Graph[SPMap,Int]= {

    val start_sec:Double = get_current_second()

    val dual_graph = get_dual_simulation(data, query,model, 1)


    out_put_log("start tight_simulation")
    //find the center of Query
//    val tmp_query_map = calculate_sssp(QueryVerticesArray,QueryEdgesArray,sc)

//    val conver_graph_vertices = dual_graph_vertices_array.map{ x => (x._1, x._2.head._2._1)}
//
//    val tmp_data_map = calculate_sssp(conver_graph_vertices, dual_graph.edges.toArray(),sc)

//    println("get_centered_vertex start")

    val radius_and_id = get_centered_vertex(QueryVerticesArray,QueryTripletsArray)

    val radius : Double = radius_and_id._2
    val vId :Long = radius_and_id._1
    //    println("vertex: "+ vId+ "  radius: "+ radius + "   label: "+radius_and_id._3)




    //because data graph maybe extremely large, so use the function "Broadcast"
//    val broadcastVar_data= sc.broadcast(data)




//    val broadcastVar_dual = sc.broadcast(dual_graph)
//
//    val broadcast_dual_graph = broadcastVar_dual.value

//    val dual_graph_vertices_array = dual_graph.vertices.toArray()
//    val dual_triplet_ex =    dual_graph.triplets.toArray().map{ case(tri) =>
//      (tri.srcId, tri.dstId)}



    val data_candidate = get_candidates( dual_graph, radius_and_id._3)
    out_put_log("balls: "+data_candidate.length)



    val final_vertex:Array[Long] = tight_calculate(data_candidate,radius, dual_graph, model, ball_num)

//    out_put_log("final_vertex:  ", final_vertex)

    val subgraph = dual_graph.subgraph(vpred = (v_id, attr) => final_vertex.contains(v_id))

    val mid_sec:Double = get_current_second()
    out_put_log("Tight process cost(seconds): "+ (mid_sec-start_sec))

//    out_put_time()

    out_put_log("tight simulation result:------------------------------------------------------------------")


    val result_vertices:Array[Long] = subgraph.vertices.map{ case(v,_) => v}.collect()
    val given_vertices :Array[Long] = QueryVerticesArray.map{case(v,_) => v}
    check_result(result_vertices, given_vertices)
//
//
//    val q = create_q
//    val g = create_g(subgraph)
//
//    val new_match = SubgraphIso.calculate(q._1,q._2,g._1,g._2)
//
//    val end_sec:Double = get_current_second()
//    println("Entir process cost(seconds): "+ (end_sec-start_sec))


//    for(x<-new_match){
//      val tm = x.map{case(y) => g._3(y)}
//      check_result(tm, given_vertices)
//    }
    out_put_log("subgraph result:##################################################################")
    out_put_log("")
    out_put_log("")

    return subgraph
  }

  private def get_dual_simulation(data: Graph[SPMap,Int], query: Graph[String,Int], model:Int, compare:Int): Graph[SPMap,Int]={

//    val g = init_Vertices_tri(data)
    val graph_tri = query.triplets.map{ tri =>  ((tri.srcId), (tri.dstId))}.collect

    var tmp = new DualSimulation(data, query, sc, graph_tri)

    val start_sec:Double = get_current_second()

    val dual_graph = tmp.apply_dual_simulation(model,compare)

    if(compare==1){
      val mid_dual_sec:Double = get_current_second()
      out_put_log("Dual process cost(seconds): "+ (mid_dual_sec-start_sec))
    }

    tmp = null

    return dual_graph
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** find the vertices of candidates according to the label
    *  @param graph: data graph
    *  @param label: which is the centered vertex label of query
    *  @return the computed vertices
    */
  private def get_candidates(graph:Graph[SPMap,Int], label : String):Array[Long]={

    //collect the candidate vertices which match the "label"

    val vertices_array = graph.vertices.collect()

    var data_candidate : Array[Long] = Array()
    for(x <- vertices_array){
      if(label == x._2.head._2._1){
        data_candidate = data_candidate:+x._1
      }
    }

    data_candidate
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** some idea , later
    */
//  def get_candidates_new(map_sssp:Map[Long,Array[(Long,Double)]],vId:Long,
//                         vertices_array:Array[(Long,SPMap)], label : Array[String]):Array[Long]={
//
//    //collect the candidate vertices which match the "label"
//    var data_candidate : Array[Long] = Array()
//    for(x <- vertices_array){
//      if(label == x._2.head._2._1){
//        data_candidate = data_candidate:+x._1
//      }
//    }
//
//    data_candidate
//  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** this method is serve for subgrpah isomorphism. To convert to the required structure of graph
    *  @param vertices_array: the array of vertices of data graph
    *  @param query_edges: the array of edges of data graph
    *  @return adjacent matrix . if adj = { {1, 2}, {0}, {1} } means that the graph has the following edges { (0, 1), (0, 2), (1, 0), (2, 1) }.
    *  @return array of labels
    *  @return a map. because at this step will reset the vertices id start from 0. then this a map to restore the vertices id
    */
  private def get_structure(vertices_array:Array[(Long,String)], query_edges:Array[(Long,Long)]):(Array[Set[Int]], Array[Int],Map[Int,Long])={
    var vertices_map : Map[Long,Int] = Map()
    var vertices_map_list : Map[Int,Long] = Map()
    var num:Int = 0

    var adj_set : Array[Set[Int]] = Array()

    for(x<-vertices_array) {
      vertices_map += (x._1 -> num)
      vertices_map_list += (num -> x._1)
      num += 1
    }

    for(x<-vertices_array){

      val tmp_bool = query_edges.map{case(v) => if(v._1 == x._1) true else false}

      if(tmp_bool.contains(true)){
        var tmp_set : Set[Int] = Set()
        for(m<-query_edges){

          if(m._1 == x._1){
            tmp_set = tmp_set  ++ Set(vertices_map(m._2))
          }
        }
        adj_set=adj_set:+tmp_set
      }else{
        val tmp_set : Set[Int] = Set()
        adj_set=adj_set:+tmp_set
      }

      val tm = vertices_map
    }

    val label_list = vertices_array.map{case(x) => x._2.toInt}

    return (adj_set,label_list,vertices_map_list)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** this method is serve for subgrpah isomorphism. To create query graph
    *  @return adjacent matrix . if adj = { {1, 2}, {0}, {1} } means that the graph has the following edges { (0, 1), (0, 2), (1, 0), (2, 1) }.
    *  @return array of labels
    *  @return a map. because at this step will reset the vertices id start from 0. then this a map to restore the vertices id
    */
  private def create_q():(Array[Set[Int]], Array[Int], Map[Int,Long])={

    val query_vertices = QueryVerticesArray.sortBy(_._1)
    val query_edges_array = QueryEdgesArray
    val query_edges = query_edges_array.map{case(x) => (x.srcId,x.dstId)}

    get_structure(query_vertices, query_edges)
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** this method is serve for subgrpah isomorphism. To create data graph
    *  @param graph data graph
    *  @return adjacent matrix . if adj = { {1, 2}, {0}, {1} } means that the graph has the following edges { (0, 1), (0, 2), (1, 0), (2, 1) }.
    *  @return array of labels
    *  @return a map. because at this step will reset the vertices id start from 0. then this a map to restore the vertices id
    */
  private def create_g(graph:Graph[SPMap,String]):(Array[Set[Int]], Array[Int],Map[Int,Long])={

    val data_vertices = graph.vertices.sortBy(_._1).map{case(x) => (x._1,x._2.head._2._1)}.collect()
    val data_edges_array = graph.edges.collect()
    val data_edges = data_edges_array.map{case(x) => (x.srcId,x.dstId)}

    get_structure(data_vertices, data_edges)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** according to the candidates, compute the result of tight simulation
    *  @param vertices_array candidates
    *  @param radius radius of query graph
    *  @param graph data graph
    *  @return result
    */
  private def tight_calculate(vertices_array:Array[Long],radius:Double,graph:Graph[SPMap,Int],model:Int, ball_num:Int ):Array[Long]={


    //extract the valid vertices from the balls
    var final_vertex  : Array[Long] = Array()
    val query_vertices = query.vertices.count()



    for(x <- vertices_array){
      var ball = create_ball(x, radius, graph )
//      val broadcastVar_data= sc.broadcast(ball)

      if(ball_num == 1){

        val ball_graph = get_dual_simulation(ball, query, model,0)


        //Only the ball contains the number of vertices more than QueryGraph is valid
        var tmp_array : Array[Long] = Array()
        for(v <- ball_graph.vertices.collect()) {
          tmp_array = tmp_array :+ v._1
        }

        final_vertex = final_vertex ++ tmp_array

      }else{
        if(ball.vertices.count() >= query_vertices){


          val ball_graph = get_dual_simulation(ball, query, model,0)


          //Only the ball contains the number of vertices more than QueryGraph is valid
          if(ball_graph.vertices.count() >= query_vertices){
            var tmp_array : Array[Long] = Array()
            for(v <- ball_graph.vertices.collect()){
              tmp_array = tmp_array:+v._1
            }

            final_vertex = final_vertex ++ tmp_array
          }
        }
      }


      ball = null
    }
    final_vertex
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** according to the candidates, compute the result of tight simulation
    *  @param graph_array vertices of query graph
    *  @param graph_triplets triplets of query graph
    *  @return the centered vertex id
    *  @return radius
    *  @return label
    */
  private def get_centered_vertex(graph_array:Array[(Long,String)],
                          graph_triplets: Array[EdgeTriplet[String,Int]]):(Long, Double, String)={

    val tmp_query_map = calculate_sssp(QueryVerticesArray,QueryEdgesArray,sc)

    val map_min_ecc = tmp_query_map._2


    var ratio : Double = -1.0
    val radius : Double = map_min_ecc.minBy(_._2)._2
    var candidate : Array[Long] = Array()
    var vId : Long = 0L
    var label : String = ""

    for(x <- map_min_ecc){
      if(x._2 == radius){
        candidate = candidate:+x._1
      }
    }

    for(x<-candidate){
      val tmp = get_labels_num(graph_array,x)
      val tmp_ratio =get_vertex_neighbor_num(graph_triplets,x)/tmp._1

      if(ratio < tmp_ratio) {
        ratio = tmp_ratio
        vId = x
        label = tmp._2
      }
    }
    (vId,radius, label)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** according to the vertex id, get its label
    *  @param vId current vertex id
    *  @param graph_array vertices of query graph
    *  @return label
    */
  private def get_vertex_label(vId : Long,graph_array:Array[(Long,String)]):String={
    var label : String = ""
    val loop = new Breaks;
    loop.breakable{
      for(x <- graph_array){
        if(x._1 == vId){
          label = x._2
          loop.break
        }
      }
    }
    label
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** get the number which the vertices in query have the same label
    *  @param vId current vertex id
    *  @param graph_array vertices of query graph
    *  @return total number
    *  @return label
    */
  private def get_labels_num(graph_array:Array[(Long,String)], vId : Long):(Int,String) ={

    var result:Array[Long] = Array()
    val label : String =get_vertex_label(vId, graph_array)
    for(x <- graph_array){
      if(x._2 == label){
        result = result:+x._1
      }
    }

    (result.length,label)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** the number of neighbors of the vertex
    *  @param graph_array triplets of query graph
    *  @return total number
    */
  private def get_vertex_neighbor_num(graph_array:Array[EdgeTriplet[String,Int]], vId:Long):Int={
    var result : Int = 0
    for(x<-graph_array){
      if(x.srcId == vId || x.dstId == vId){
        result = result+1
      }
    }
    result
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** the number of neighbors of the vertex
    *  @param vertices_array vertices of  graph
    *  @param edges_array edges of  graph
    *  @return the map contains every vertex sssp
    *  @return the map contains every vertex min ecc
    */
  private def calculate_sssp(vertices_array:Array[(Long,String)], edges_array:Array[Edge[Int]],sc:SparkContext)
  :(Map[Long,Array[(Long,Double)]],  Map[Long,Double])={

    var map_sssp : Map[Long,Array[(Long,Double)]] = Map()

    var map_min_ecc : Map[Long,Double] = Map()

    var tmp_edge : Array[Edge[Double]] = Array()

    for(e <- edges_array){
      tmp_edge = tmp_edge:+Edge( e.srcId , e.dstId, 1.0)
      tmp_edge = tmp_edge:+Edge( e.dstId , e.srcId, 1.0)
    }
    tmp_edge = tmp_edge.distinct
    val edges_map: RDD[Edge[Double]] =sc.parallelize(tmp_edge)

    val edge_graph = Graph.fromEdges(edges_map, "defaultProperty")
    edge_graph.cache()

    for(x <- vertices_array){

      val input_graph = edge_graph.mapVertices((id, _) => if (id == x._1) 0 else Double.PositiveInfinity)

      val sssp = shortest_path(input_graph)


      map_sssp+=(x._1 -> sssp.vertices.collect())

      val ecc = sssp.vertices.filter(distance => distance._2 != Double.PositiveInfinity).collect.sortBy(v => v._2).reverse.head._2
      map_min_ecc+=(x._1 -> ecc)
    }

    (map_sssp, map_min_ecc)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** create ball according to the map of sssp
    *  @param id centered vertex id
    *  @param radius radius of query
    *  @param original original graph
    *  @return result graph
    */
  private def create_ball(id:VertexId, radius: Double, original : Graph[SPMap,Int])
  :Graph[SPMap,Int]={

    var ball_vertex : Array[Long] = Array()
    var round = radius


//    var t_vertex : Array[Long] = Array()
//    for(x <- map(id)){
//      if(x._2 <= radius){
//        t_vertex=t_vertex:+x._1
//      }
//    }
    val triplet_ex =    original.triplets.map{ case(tri) => (tri.srcId, tri.dstId)}.collect()

    ball_vertex=ball_vertex:+id
    var target = ball_vertex
    do{
      var tmp: Array[Long] = Array()
      for(x <- triplet_ex){
        if(target.contains(x._1)  && !tmp.contains(x._2) ){
          tmp=tmp:+x._2
        }
        if(target.contains(x._2) && !tmp.contains(x._1)){
          tmp=tmp:+x._1
        }
      }

      target = tmp
      ball_vertex = ball_vertex ++ tmp
      round = round-1
    }while(round > 0)

    val subgraph = original.subgraph(vpred = (v_id, attr) => ball_vertex.contains(v_id))
//    out_put_log("create_ball subgraph: "+ subgraph.vertices.toArray().sortBy(v => v._1).mkString("\n"))
    return subgraph
  }


}





object Test_Tight_local{

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** execute main
    *  @param args [0] the data file
    *  @param args [1] the amount of labels
    *  @param args [2] the amount of vertices of query
    */

  def main(args: Array[String]): Unit = {
    //turn off the log information
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setMaster("local") // this is where the master is specified
      .setAppName("GraphPatternMatching")
      .set("spark.akka.frameSize", "2000")
      .set("spark.akka.threads", "16")
      .set("spark.rdd.compress", "true")
      .set("spark.core.connection.ack.wait.timeout","600")
      .set("spark.akka.timeout","300")
      .set("spark.driver.maxResultSize","30g")


    val sc = new SparkContext(conf)
//    val str:String = "/home/eric/SparkBench/Wiki-Vote.txt"
//      val str:String = "/home/eric/SparkBench/s16.csv"

//    val str:String = "/home/eric/1_BigData/1_graph_pattern_matching/src/GraphX/Pattern_Matching/dataset/Email-EuAll.txt"
//    val str:String = "/home/eric/SparkBench/Wiki-Vote_csv.csv"
//    val str:String = "/home/eric/SparkBench/test_6_2.csv"
//    val data = GraphConstruct.load_data_graph(str, 200,sc)

//    val data= GraphConstruct.load_data_graph(str, 200,sc,0)
//    data.cache()
//    var ran:Int = 1000
//    if(true){
//
//      val query = GraphConstruct.extract_graph(data, 100,sc,ran)
//
//      out_put_log("query graph:" + 100)
//      val tmp = new TightSimulation(data, query._1,sc)
//      tmp.apply_tight_simulation(1,1)
//    }
//
//    if(true){
//      val query = GraphConstruct.extract_graph(data, 100,sc,ran)
//
//      out_put_log("query graph:" + 100)
//      val tmp = new TightSimulation(data, query._1,sc)
//      tmp.apply_tight_simulation(0,1)
//    }




//    {
//
//      val fir :String = "/home/eric/SparkBench/s10.csv"
//
//      var data= GraphConstruct.load_data_graph(fir, 200,sc, 0)
//      data.cache()
//
//      var ran:Int = 1500
//
//      var query = GraphConstruct.extract_graph(data, 100,sc,ran)
//
//      if (true){
//        out_put_log("new app query graph:" + 100)
//        var tmp = new TightSimulation(data, query._1,sc)
//        tmp.apply_tight_simulation(1,1)
//        tmp=null
//      }
//
//      if (true){
//        out_put_log("original app query graph:" + 100)
//        //      val query = GraphConstruct.extract_graph(data, 100,sc,ran)
//        var tmp = new TightSimulation(data, query._1,sc)
//        tmp.apply_tight_simulation(0,1)
//        tmp=null
//      }
//      data = null
//      query = null
//
//    }

    {

      val fir :String = "/home/eric/SparkBench/s14.csv"

      var data= GraphConstruct.load_data_graph(fir, 200,sc, 0)
      data.cache()

      var ran:Int = 1500

      var query = GraphConstruct.extract_graph(data, 100,sc,ran)

      if (true){
        out_put_log("new app query graph:" + 100)
        var tmp = new TightSimulation(data, query._1,sc)
        tmp.apply_tight_simulation(1,1)
        tmp=null
      }

      if (true){
        out_put_log("original app query graph:" + 100)
        //      val query = GraphConstruct.extract_graph(data, 100,sc,ran)
        var tmp = new TightSimulation(data, query._1,sc)
        tmp.apply_tight_simulation(0,1)
        tmp=null
      }
      data = null
      query = null

    }


  }

}

object Test_Tight_remote{

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** execute main
    *  @param args [0] the data file
    *  @param args [1] the amount of labels
    *  @param args [2] the amount of vertices of query
    */

  def main(args: Array[String]): Unit = {

    if(args(3).toInt != 1){
      //turn off the log information
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }


    println("------------------------------------------------------")
    println("                    Version:4.38")
    println("------------------------------------------------------")
    println()

    val conf = new SparkConf()
      .setAppName("GraphPatternMatching")
      .set("spark.akka.frameSize", "2000")
      .set("spark.driver.maxResultSize","50g")
//      .set("spark.rdd.compress", "true")
//      .set("spark.core.connection.ack.wait.timeout","600")
//      .set("spark.akka.timeout","300")

    val sc = new SparkContext(conf)

    {
//      val fir :String = "/s16.csv"
      var data= GraphConstruct.load_data_graph_test( args(0), 200, sc, args(5).toInt).cache()

      var ran:Int = 4500
      var query = GraphConstruct.extract_graph_test(data, 100,sc,ran)
//      println("Iteration times: "+ args(4).toInt)
      if (true){
        val tmp = new DualSimulation(data, query._1,sc, query._3)
        val temp2 = tmp.apply_dual_simulation(args(4).toInt,0)

      }


//      if (true){
//        out_put_log("old app query graph:" + 100)
//        val tmp = new DualSimulation(data, query._1,sc)
//        val temp2 = tmp.apply_dual_simulation(0,1)
//      }
      Thread.sleep(500*3000)

      data = null
      query=null
    }

  }

}



object Test_SSSP{

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** execute main
    *  @param args [0] the data file
    *  @param args [1] the amount of labels
    *  @param args [2] the amount of vertices of query
    */

  def main(args: Array[String]): Unit = {


//    val start_sec_whole:Double = get_current_second()


      //turn off the log information
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)


    println("------------------------------------------------------")
    println("                    Version:1.42")
    println("------------------------------------------------------")
    println()

    val conf = new SparkConf()
//      .setMaster("local") // this is where the master is specified
      .setAppName("Test_SSSP")
      .set("spark.akka.frameSize", "2000")
//      .set("spark.akka.threads", "8")
      .set("spark.driver.maxResultSize","50g")
//      .set("spark.shuffle.manager","hash")

    val sc = new SparkContext(conf)


//    val str:String = "/home/eric/SparkBench/s14.csv"

//    val data=  GraphConstruct.load_data_graph( args(0), args(1).toInt,sc, args(5).toInt)
//    val data=  GraphConstruct.load_data_graph( str, 200, sc)



//    val graph = GraphLoader.edgeListFile(sc, args(0))

//    for(x <- 8 to 20){
      val graph = GraphLoader.edgeListFile(sc, args(0) , numEdgePartitions = args(5).toInt)
      out_put_log("numEdgePartitions : " + args(5).toInt)
      out_put_log("vertices : " + graph.vertices.count())
      // Run PageRank

      val time_sleep = args(2).toInt
      println("sleep(s) : " + args(2).toInt)
      Thread.sleep(time_sleep*1000)

      val start_sec:Double = get_current_second()

      println("Iteration times: "+ args(4).toInt)
      val ranks = graph.staticPageRank(args(4).toInt)

      println("start pageRank: ")

      val mid_dual_sec:Double = get_current_second()
      println("process cost(seconds): "+ (mid_dual_sec-start_sec))


//      val end_sec_whole:Double = get_current_second()
//      println("whole process cost(seconds): "+ (end_sec_whole-start_sec_whole))
      println()
      println()
//    }


    Thread.sleep(time_sleep*10000)

  }
}
