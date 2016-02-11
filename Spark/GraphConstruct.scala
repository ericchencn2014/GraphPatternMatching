package GraphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.control.Breaks
import GraphTypes._
import CommonUse._
import org.apache.spark.broadcast.Broadcast

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `GraphTypes` specifies every element in data graph.
  */
object GraphTypes
{

  /** first list :
    *     vertexId: the unique id of vertex
    *
    * second list:
    *     String: label of vertex
    *     Boolean: match flag, true: valid
    *     Array[Long]: match set of this vertex
    *     Map[Long,Array[Long]]: the match set from its children
    *     Boolean: this vertex will send message in the next round or not, true: send
    *     Int: the number of supersteps
    *     Map[Long,Array[Long]]: the match set from its parents
    *     Array[((Long,Boolean), (Long,Boolean))]: it's structure
    */
  type SPMap = Map[VertexId, (String,Boolean, Array[Long],  Map[Long,Array[Long]], Boolean,Int, Map[Long,Array[Long]], Array[((Long,Boolean,String), (Long,Boolean,String))])]

} // GraphTypes


import GraphTest._
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** This `GraphConstruct` object provides methods for construct of extract graphs
  */
object GraphConstruct extends java.io.Serializable {

  def build_graph(sc: SparkContext): (Graph[SPMap, String], Graph[String, String]) = {
    GraphTest.init_sc(sc)
    val data = build_DataGraph(vertices_map_Data_t3, edges_map_Data_t3)
    val query = build_QueryGraph(vertices_map_Query_t3, edges_map_Query_t3)

    return (data, query)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** build data graph
    * @return the data graph
    */
  def build_DataGraph(v: RDD[(VertexId, SPMap)], e: RDD[Edge[String]]): Graph[SPMap, String] = {

    // Create an RDD for the vertices
    val vertices_map: RDD[(VertexId, SPMap)] = v

    //Create an RDD for edges
    val edges_map: RDD[Edge[String]] = e

    // Build the initial Graph
    val data = Graph(vertices_map, edges_map)

    return data
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** build query graph
    * @return the query graph
    */
  def build_QueryGraph(v: RDD[(VertexId, String)], e: RDD[Edge[String]]): Graph[String, String] = {

    // Create an RDD for the vertices
    val vertices_map: RDD[(VertexId, String)] = v

    // Create an RDD for edges
    val edges_map: RDD[Edge[String]] = e

    // Build the initial Graph
    val query = Graph(vertices_map, edges_map)

    println("Create Query graph")

    return query
  }


  def read_graph_file(file_dir: String, sc: SparkContext, read_mode: Int): (RDD[Edge[String]], Array[Long]) = {

    //val file_dir : String = "src/GraphX/test_DataGraph.txt"
    //val file_dir : String = "src/GraphX/Wiki-Vote.txt"
    //val file_dir : String = "src/GraphX/soc-LiveJournal1.txt"
    //val file_dir : String = "src/GraphX/web-Stanford.txt"
    //val file_dir : String = "src/GraphX/test_10_3.txt"


    var tmp_edge: Array[Edge[String]] = Array()


    var all_vertices: Array[Long] = Array()

    out_put_log("start to read file")
    val start_sec: Double = get_current_second()
    var end_sec: Double = 0

    var i: Int = 0

    var tm1: Long = 0
    var tm2: Long = 0

    //    val textFile = sc.textFile("hdfs://input/war-and-peace.txt")


    //    val graph_file = sc.textFile("hdfs://user/eric/input/Wiki-Vote_csv.csv")
    //    println(graph_file.first())
    val graph_file = sc.textFile(file_dir)
    out_put_log("Input file : " + file_dir)







    //    if(read_mode == 1){
    //      val links = graph_file.map{ s => val parts:Array[String] = s.split(",")
    //        (parts(0), parts(1))}.collect()
    //
    //      end_sec = get_current_second()
    //      out_put_log("Read file cost(seconds): "+ (end_sec-start_sec))
    //
    //      for(line <- links){
    //        if(i<10){
    //          println(line)
    //          i = i+1
    //        }
    //        tm1 = line._1.toLong
    //        tm2 = line._2.toLong
    //
    //        if(tm1 != tm2){
    //          tmp_edge = tmp_edge:+Edge( tm1 ,tm2, "")
    //          if(!all_vertices.contains(tm1)){
    //            all_vertices =all_vertices:+tm1
    //          }
    //          if(!all_vertices.contains(tm2)){
    //            all_vertices =all_vertices:+tm2
    //          }
    //        }
    //      }
    //    }
    //    else{
    //
    //      val links = graph_file.map{ s => val parts:Array[String] = s.split("\\s+")
    //        (parts(0), parts(1))}.distinct().cache()
    //
    //      end_sec = get_current_second()
    //      out_put_log("Read file cost(seconds): "+ (end_sec-start_sec))
    //
    ////      var ranks = links.mapValues(v => 1.0)
    //
    //      for(line <- links){
    //        if(i<10){
    //          println(line)
    //          i = i+1
    //        }
    //        tm1 = line._1.toLong
    //        tm2 = line._2.toLong
    //
    //        if(tm1 != tm2){
    //          tmp_edge = tmp_edge:+Edge( tm1 ,tm2, "")
    ////          if(!all_vertices.contains(tm1)){
    ////            all_vertices =all_vertices:+tm1
    ////          }
    ////          if(!all_vertices.contains(tm2)){
    ////            all_vertices =all_vertices:+tm2
    ////          }
    //        }
    //      }
    //    }


    val broadcastVer = sc.broadcast(all_vertices)

    //    out_put_log("total lines : "+count);

    end_sec = get_current_second()
    out_put_log("finish graph creation(seconds): " + (end_sec - start_sec))

    val edges_map: RDD[Edge[String]] = sc.parallelize(tmp_edge)

    return (edges_map, broadcastVer.value)
  }

  def get_vertex_rdd(vertices_arr: Array[Long], label_types: Int = 200, sc: SparkContext, edges: RDD[Edge[String]]): RDD[(VertexId, SPMap)] = {

    val r = scala.util.Random
    val match_set: Array[Long] = Array()
    val tmp_match_set: Map[Long, Array[Long]] = Map()


    //    val str : String = ""
    var tmp_vertex: Array[(VertexId, SPMap)] = Array()

    val broadcastVer = sc.broadcast(edges.collect())

    //    var vertices_map:RDD[(VertexId,SPMap)]  =sc.parallelize(vertex_rdd)
    //    vertices_map.cache()

    //    println(vertices_arr.length)

    val tmp_triplets: Array[((Long, Boolean, String), (Long, Boolean, String))] = Array()

    for (v <- vertices_arr) {

      //      var tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
      //
      //      for(x<-broadcastVer.value){
      //        if(x.srcId == v || x.dstId == v){
      //          tmp_triplets = tmp_triplets :+ ((x.srcId,true, " "), (x.dstId,true, " "))
      //        }
      //      }
      //      println("triplet len : ", tmp_triplets.length)

      tmp_vertex = tmp_vertex :+(v, Map(v -> (((r.nextInt(label_types).toString), false, match_set, tmp_match_set, false, 1, tmp_match_set, tmp_triplets))))
    }

    val vertices_map: RDD[(VertexId, SPMap)] = sc.parallelize(tmp_vertex)

    //    val broadcastEdge = sc.broadcast(vertices_map)

    return vertices_map
  }

  def get_random_label(label_types: Int = 200, raw_graph: Graph[Int, Int]): Array[String] = {
    var total_num = raw_graph.vertices.count()
    if (total_num > 50000) {
      total_num = 50000
    }

    val r = scala.util.Random

    val random_label = new Array[String](total_num.toInt)

    for (x <- 0 to total_num.toInt - 1) {
      random_label(x) = r.nextInt(label_types).toString
    }

    return random_label
  }

  def get_map(raw_graph: Graph[String, Int], convert: RDD[Edge[Int]]): Map[VertexId, Array[((Long, Boolean, String), (Long, Boolean, String))]] = {

    var map: Map[VertexId, Array[((Long, Boolean, String), (Long, Boolean, String))]] = Map()

    val vertices = raw_graph.vertices.collect()
    for (x <- vertices) {

      val map_val: Array[((Long, Boolean, String), (Long, Boolean, String))] =
        convert.filter(e => e.srcId == x._1 || e.dstId == x._1).map {
          v =>

            ((v.srcId, true, ""), (v.dstId, true, ""))
        }.toArray()


      //      val tem_tri = raw_graph.triplets.filter{ tri =>
      //        tri.srcId == x._1 || tri.dstId ==x._1
      //      }
      //
      //      val map_val : Array[((Long,Boolean,String), (Long,Boolean,String))] = tem_tri.map{ v =>
      //
      //        val tem = ((v.srcId, true, ""), (v.dstId, true, ""))
      //        tem
      //        }.toArray()

      map += (x._1 -> map_val)
    }

    return map
  }

  def GetTripletInfo(raw_graph: Graph[Int, Int]): Graph[(Long, Array[((Long, Boolean, String), (Long, Boolean, String))]), Int] = {

    val start_sec = get_current_second()
    var test = raw_graph.mapVertices {
      (v, attr) =>
        val children: Array[((Long, Boolean, String), (Long, Boolean, String))] = Array()
        val parents: Array[((Long, Boolean, String), (Long, Boolean, String))] = Array()

        //children : the children of this vertex collection. parents, the parents of this vertex collection
        (v, children, parents)
    }

    val ttat: Array[((Long, Boolean, String), (Long, Boolean, String))] = Array()

    var getParents = test.pregel((-1, ttat, ttat), 2, EdgeDirection.Either)(

      (id, dist, newDist) =>

        if (newDist._1 == -1) {
          dist
        }
        else {
          (id, dist._2, newDist._3)
        }
      , // Vertex Program

      triplet => {
        // Send Message

        var ttt: Array[((Long, Boolean, String), (Long, Boolean, String))] = Array()
        ttt = ttt :+((triplet.srcId, true, ""), (triplet.dstId, true, ""))
        Iterator((triplet.dstId, (triplet.srcId.toInt, ttat, ttt)))
      },

      (a, b) => (a._1, a._2 ++ b._2, a._3 ++ b._3) // Merge Message
    )

    test = null

    val getChildren = getParents.pregel((-1, ttat, ttat), 2, EdgeDirection.Either)(

      (id, dist, newDist) =>

        if (newDist._1 == -1) {
          dist
        }
        else {
          (id, newDist._2, dist._3)
        }
      , // Vertex Program

      triplet => {
        // Send Message

        var ttt: Array[((Long, Boolean, String), (Long, Boolean, String))] = Array()
        ttt = ttt :+((triplet.srcId, true, ""), (triplet.dstId, true, ""))
        Iterator((triplet.srcId, (triplet.dstId.toInt, ttt, ttat)))
      },

      (a, b) => (a._1, a._2 ++ b._2, a._3 ++ b._3) // Merge Message
    )
    getParents = null

    val end_sec = get_current_second()
    out_put_log("end to exact graph triplets cost(seconds): " + (end_sec - start_sec))


    return getChildren.mapVertices((v, attr) => (v, attr._2 ++ attr._3))
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** load data graph from file, the format of file is edge description,
    * like "3  4" means the edge source id 3 to edge des id 4
    * @param file_dir  the file
    * @param label_types  the types of label which will be assigned to every vertex randomly, default 200
    * @param read_mode 1: lines in file is divided by "," . others: divided by " \t"
    * @return the data graph
    */
  def load_data_graph(file_dir: String, label_types: Int = 200, sc: SparkContext, read_mode: Int = 1): Graph[SPMap, Int]
  = {

    val start_sec = get_current_second()
    out_put_log("Input file : " + file_dir)

    //This alorithm didn't support self loop
    //    val raw : RDD[Edge[Int]] = sc.textFile(file_dir).map{ s =>
    //      val parts = s.split("\\s+")
    //        Edge(parts(0).toLong, parts(1).toLong, 1)
    //    }.distinct.filter{ s =>
    //      s.srcId != s.dstId
    //    }


    //    val raw_graph : Graph[String, Int] = Graph.fromEdges(raw,"defaultProperty")

    val raw_graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, file_dir, numEdgePartitions = read_mode)
    out_put_log("numEdgePartitions : " + read_mode)

    out_put_log("build data graph vertices: " + raw_graph.vertices.count())
    out_put_log("build data graph edges: " + raw_graph.edges.count())

    val end_sec = get_current_second()
    out_put_log("Read file cost(seconds): " + (end_sec - start_sec))

//    val raw_graph = circ_graph.mapEdges{ x => x._1 != x._2 }

    val match_set: Array[Long] = Array()
    val tmp_match_set: Map[Long, Array[Long]] = Map()

    val random_label: Array[String] = get_random_label(label_types, raw_graph)
    val range = random_label.length


    //    val map : Map[VertexId,  Array[((Long,Boolean,String), (Long,Boolean,String))] ] = get_map(raw_graph, convert)


    //    var map : Map[VertexId,  Array[((Long,Boolean,String), (Long,Boolean,String))] ] = Map()

    //    convert.cache()
    //    val vertices = raw_graph.vertices.collect()
    //    for(x <- vertices){
    //
    //      val map_val : Array[((Long,Boolean,String), (Long,Boolean,String))]=
    //        convert.filter( e => e.srcId == x._1 || e.dstId == x._1).map{
    //          v => ((v.srcId, true, ""), (v.dstId, true, ""))
    //        }.collect()
    //
    //      map+=(x._1 -> map_val)
    //    }


    //    val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()

    //    val tew = raw_graph.mapTriplets{tri=>
    //
    //
    //
    //      (   (tri.srcId, ), (tri.dstId), tri.attr)
    //
    //    }


    //    val raw_edges = raw.collect()
    val mid_graph = GetTripletInfo(raw_graph)

    val graph = mid_graph.mapVertices { (v, attr) =>

      //      val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = map(v)

      //      val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))]=
      //        raw_edges.filter( e => e.srcId == v || e.dstId == v).map{
      //        v => ((v.srcId, true, ""), (v.dstId, true, ""))
      //      }


      var num = v.toInt

      while (num >= range) {
        num = num - range
      }
      while (num < 0) {
        num = num + range
      }

      Map(v ->(random_label(num), false, match_set, tmp_match_set, false, 1, tmp_match_set, attr._2))
    }

    val build_end = get_current_second()
    out_put_log("build graph cost(seconds): " + (build_end - end_sec))

    return graph
  }



  def load_data_graph_test(file_dir: String, label_types: Int = 200, sc: SparkContext, read_mode: Int = 1): Graph[SPMap, Int]
  = {

    val start_sec = get_current_second()
    out_put_log("Input file : " + file_dir)

    val raw_graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, file_dir, numEdgePartitions = read_mode)
    out_put_log("numEdgePartitions : " + read_mode)

    out_put_log("build data graph vertices: " + raw_graph.vertices.count())
    out_put_log("build data graph edges: " + raw_graph.edges.count())

    val end_sec = get_current_second()
    out_put_log("Read file cost(seconds): " + (end_sec - start_sec))

    val match_set: Array[Long] = Array()
    val tmp_match_set: Map[Long, Array[Long]] = Map()

    val mid_graph = GetTripletInfo(raw_graph)
    val iii : String = "1234"


    val graph = mid_graph.mapVertices { (v, attr) =>
      Map(v ->(iii, false, match_set, tmp_match_set, false, 1, tmp_match_set, attr._2))
    }

    val build_end = get_current_second()
    out_put_log("build graph cost(seconds): " + (build_end - end_sec))

    return graph
  }

  def get_data_tri(data: Graph[SPMap, Int], number: Int = 100, sc: SparkContext): (Array[(Long, Long)], Int) = {

    //    val data_triplet_array = data.triplets.collect()

    val data_triplet = data.triplets.map { case (tri) =>
      (tri.srcId, tri.dstId)
    }.collect()

    val total_vertices = data.vertices.count()
    if (total_vertices < number) {
      out_put_log("query graph vertices number error!")
    }

    return (data_triplet, data_triplet.length)
  }

  def get_vertices_from_data(total_triplets: Long, broad_data: Array[(Long, Long)],
                             sc: SparkContext, ran: Int, number: Int = 100): Array[Long] = {

    var vertices_for_query_graph: Array[Long] = Array()

    val r = scala.util.Random

    //It's complicated to compute the limitation of start triplet, so just limit the range to its 4/5
    var start_triplet: Int = ran
    if (ran == 0) {
      val range = total_triplets.toInt * 4 / 5
      start_triplet = r.nextInt(range)
    }

    var length: Int = 0
    var num: Int = 0
    vertices_for_query_graph = vertices_for_query_graph :+ broad_data(start_triplet)._1

    val loop = new Breaks;

    do {

      loop.breakable {

        val currentId: Long = vertices_for_query_graph(num)
        num = num + 1


        val get_v: Array[Long] = broad_data.filter { x => currentId == x._1 }.map { v => v._2 }

        if ((get_v.length + length) >= number - 1) {
          val items = number - 1 - length
          vertices_for_query_graph = vertices_for_query_graph ++ get_v.take(items)
        } else {
          vertices_for_query_graph = vertices_for_query_graph ++ get_v
        }
      }

      vertices_for_query_graph = vertices_for_query_graph.distinct
      length = vertices_for_query_graph.length - 1

    } while (length < number - 1)

    vertices_for_query_graph
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** extract query graph from data graph
    * @param data the data graph
    * @param number  the total number of nodes which forms query graph, maximum value 100
    * @return the query graph
    *
    * @warning: this function only for amount vertices of data graph up to 400
    */
  def extract_graph(data: Graph[SPMap, Int], number: Int = 100, sc: SparkContext, ran: Int): (Graph[String, Int], Int, Array[(Long, Long)]) = {


    val end_sec = get_current_second()

    var data_triplet = get_data_tri(data, number, sc)

    val len = data_triplet._2

    val broad_data = sc.broadcast(data_triplet._1)

    data_triplet = null

    out_put_log("start build query graph")

    val vertices_for_query_graph: Array[Long] = get_vertices_from_data(len, broad_data.value, sc, ran, number)

    val graph = data.subgraph(vpred = (v_id, attr) => vertices_for_query_graph.contains(v_id)).mapVertices((id, attr) => attr.head._2._1)

    out_put_log("graph vertices  " + graph.vertices.count())

    val build_end = get_current_second()
    out_put_log("build pattern cost(seconds): " + (build_end - end_sec))

    val graph_tri = graph.triplets.map{ tri =>  ((tri.srcId), (tri.dstId))}.collect

    return (graph, ran, graph_tri)
  }

  def extract_graph_test(data: Graph[SPMap, Int], number: Int = 100, sc: SparkContext, ran: Int): (Graph[String, Int], Int, Array[(Long, Long)]) = {

//    val array = data.vertices.take(10).map{x=> x._1}
//    val graph = data.subgraph(vpred = (v_id, attr) => array.contains(v_id)).mapVertices((id, attr) => attr.head._2._1)
//    val graph_tri = graph.triplets.map{ tri =>  ((tri.srcId), (tri.dstId))}.collect

    val vertices_map: RDD[(VertexId, String)] =
      sc.parallelize(Array((1L, "A"), (2L,"B"), (3L,"C")))

    val edges_map: RDD[Edge[Int]] =sc.parallelize(Array(Edge(1, 2, 1),    Edge(2, 1, 1),Edge(2, 3, 1)))

    // Build the initial Graph

    val graph : Graph[String, Int] = Graph(vertices_map, edges_map)
    val graph_tri = graph.triplets.map{ tri =>  ((tri.srcId), (tri.dstId))}.collect
    return (graph, ran, graph_tri)
  }
}
