/**
 * @author Chao Chen
 * @email  ericchencnie@gmail.com
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object GraphPatternMatching {

  //turn off the log information
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = new SparkContext("local", "GraphPatternMatching")
  val QueryGraph : Graph[String,String] = build_QueryGraph(sc)

  //the vertex arguments of Data graph include: 1 label, 2 match flag, 3 match set, 4 label from parents, 5 message update flag
  val DataGraph : Graph[(String, Boolean,Array[Long], Array[String],Boolean,Boolean),String] = build_DataGraph(sc)

  //collect all labels in Query graph
  val QueryLabels :  Array[(String)] = QueryGraph.vertices.map{ case (id,label) => label}.collect.distinct

  //extract Query graph information
  val QueryVerticesArray = QueryGraph.vertices.toArray()
  val QueryTripletsArray = QueryGraph.triplets.toArray()
  val QuerySortMap : Map[String,Array[Long]] = extract_array()

  QuerySortMap.foreach((x) => println("SortMap label: "+ x._1 + "  array: "+ x._2.mkString(" ")))

  var DataTripletsArray = DataGraph.triplets.toArray()
  val DataVerticesArray = DataGraph.vertices.toArray()

  var DataArray :         //contains source vertexId and match flag, and des vertexId and match flag
  Array[((Long,Boolean), (Long,Boolean))] =
    DataTripletsArray.map{ case(tri) =>
      ((tri.srcId,tri.srcAttr._2), (tri.dstId, tri.srcAttr._2))}

  def main(args: Array[String]) {


    //dual_simulation(DataGraph)
    tight_simulation

  }

  
  def tight_simulation() : Graph[(String, Boolean,Array[Long], Array[String],Boolean,Boolean),String]= {

    val dual_graph =  dual_simulation(DataGraph)

    println("end of dual")

    var map_query_sssp : Map[Long,Array[(Long,Double)]] = Map()

    var map_data_sssp : Map[Long,Array[(Long,Double)]] = Map()

    var map_query_min_ecc : Map[Long,Double] = Map()

    var map_data_min_ecc : Map[Long,Double] = Map()

    for(x <- QueryVerticesArray){

      val tmp_graph = QueryGraph.mapVertices((id, _) => if (id == x._1) 0 else Double.PositiveInfinity)
      val input_graph = tmp_graph.mapEdges( (str) => 1.0)

      var sssp = shortest_path(x._1, input_graph)

      map_query_sssp+=(x._1 -> sssp.vertices.toArray())

      var ecc = sssp.vertices.filter(distance => distance._2 != Double.PositiveInfinity).collect.sortBy(v => v._2).reverse.head._2
      map_query_min_ecc+=(x._1 -> ecc)
    }

    map_query_sssp.foreach((x) => println("map_query_sssp$$$$$$$$$$$$$: "+ x._1 + "  array: "+ x._2.mkString(" ")))
    map_query_min_ecc.foreach((x) => println("map_query_min_ecc: "+ x._1 + "  array: "+ x._2))

    for(x <- DataVerticesArray){

      val tmp_graph = DataGraph.mapVertices((id, _) => if (id == x._1) 0 else Double.PositiveInfinity)
      val input_graph = tmp_graph.mapEdges( (str) => 1.0)

      var sssp = shortest_path(x._1, input_graph)

      map_data_sssp+=(x._1 -> sssp.vertices.toArray())

      var ecc = sssp.vertices.filter(distance => distance._2 != Double.PositiveInfinity).collect.sortBy(v => v._2).reverse.head._2
      map_data_min_ecc+=(x._1 -> ecc)

    }
    map_data_sssp.foreach((x) => println("map_data_sssp************: "+ x._1 + "  array: "+ x._2.mkString(" ")))
    map_data_min_ecc.foreach((x) => println("map_data_min_ecc: "+ x._1 + "  array: "+ x._2))

    val radius : Double = map_query_min_ecc.minBy(_._2)._2
    val vId :Long = map_query_min_ecc.minBy(_._2)._1
    println("vertex: "+ vId+ "  radius: "+ radius)

    var label : String = ""
    for(x <- QueryVerticesArray){
      if(x._1 == vId){
        label = x._2
      }
    }

    println("label :" + label)

    val dual_graph_array = dual_graph.vertices.toArray()
    var data_candidate : Array[Long] = Array()
    for(x <- dual_graph_array){
      if(label == x._2._1){
        data_candidate = data_candidate:+x._1
      }
    }
    println("data_candidate")
    data_candidate.foreach(println)

    var final_vertex  : Array[Long] = Array()
    for(x <- data_candidate){
      val ball = create_ball(x, radius, map_data_sssp, dual_graph)
      val ball_graph = dual_simulation(ball)

      var tmp : Array[Long] = Array()
      for(x <- ball_graph.vertices.toArray()){
        if(x._2._2 == true){
          tmp = tmp:+x._1
        }
      }

      final_vertex = final_vertex ++ tmp
    }
    final_vertex = final_vertex.distinct

    println("final_vertex")
    final_vertex.foreach(println)

    val subgraph = dual_graph.subgraph(vpred = (v_id, attr) => final_vertex.contains(v_id))
    println("tight graph: " + subgraph.vertices.take(10).mkString("\n"))

    return subgraph
  }


  def extract_array() : Map[String,Array[Long]] = {

    var map : Map[String,Array[Long]] = Map()

    for(ele <- QueryLabels)
    {
      var tmp_array : Array[Long] = Array()

      for(lab <- QueryVerticesArray){
        if(lab._2 == ele){
          tmp_array = tmp_array :+ lab._1
        }
      }
      map+= (ele -> tmp_array)
    }
    return map
  }
  
  def dual_simulation(data:Graph[(String, Boolean,Array[Long], Array[String],Boolean,Boolean),String]) :
  Graph[(String, Boolean,Array[Long], Array[String],Boolean,Boolean),String]= {

    val iniArrayChild : Array[Long] = Array()
    val iniArrayParent : Array[String] = Array("")

    println("Start Pregel")

    val result_graph = data.pregel(("", false, iniArrayChild, iniArrayParent, false, false), 10, EdgeDirection.Either )(vprog,

      triplet => {
        println("--------------DstID:->"+triplet.dstId+"  SrcId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3.mkString("")+
        "  bSend "+ triplet.dstAttr._5)

        if (triplet.dstAttr._5 == true) {
          println("DstID:->"+triplet.dstId+"  SrcId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3.mkString(""))
          Iterator((triplet.srcId, ( triplet.dstAttr._1, triplet.dstAttr._2, triplet.dstAttr._3, triplet.dstAttr._4, false, true)))

  //        println("srcID:->"+triplet.dstId+"  dstId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3.mkString(""))
//          Iterator((triplet.dstId, ( triplet.srcAttr._1, triplet.srcAttr._2, triplet.srcAttr._3, triplet.srcAttr._4, false, true)))
          //Iterator((triplet.dstId, (triplet.srcAttr._1, true, triplet.srcAttr._3, triplet.srcAttr._4)))
        } else {
          Iterator.empty
        }
      },
      mergeMsg)

    val subgraph = result_graph.subgraph(vpred = (v_id, attr) => attr._2==true)
    println("dual graph: " + subgraph.vertices.take(10).mkString("\n"))

    return subgraph
  }


  def create_ball(id:VertexId, radius: Double, map_sssp:Map[Long,Array[(Long,Double)]],
           graph_ball : Graph[(String, Boolean,Array[Long], Array[String],Boolean,Boolean),String])
  :Graph[(String, Boolean,Array[Long], Array[String],Boolean,Boolean),String]={

    var ball_vertex : Array[Long] = Array()

    for(x <- map_sssp(id)){
      if(x._2 <= radius){
        ball_vertex=ball_vertex:+x._1
      }
    }

    val subgraph = graph_ball.subgraph(vpred = (v_id, attr) => ball_vertex.contains(v_id))
    println("create_ball subgraph: "+ subgraph.vertices.take(5).mkString("\n"))
    return subgraph
  }

  def shortest_path(sourceId: VertexId, graph: Graph[Double, Double]): Graph[Double,Double] = {

    val sssp = graph.pregel(Double.PositiveInfinity)(

      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program

      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      (a, b) => math.min(a, b) // Merge Message
    )
    sssp
  }

  
  def init_nodes( id:VertexId, dist: (String, Boolean, Array[Long], Array[String],Boolean,Boolean)) :
  (String, Boolean, Array[Long], Array[String],Boolean,Boolean) = {

    val iniArray : Array[Long] = Array(0)
    val iniArrayParent : Array[String] = Array("")
    var newVertices = ("", false, iniArray, iniArrayParent, false, true)

    if(QueryLabels contains(dist._1)){
      val tempMatch: Array[Long] = QuerySortMap(dist._1)

      newVertices = (dist._1, true, tempMatch, dist._4, true, true)
    } else{newVertices = (dist._1, false, dist._3, dist._4, true, true)}

    println("Init vertex:"+id+"  label:"+newVertices._1+"  flag:"+newVertices._2+"  match_set:" + newVertices._3.mkString("\n")+"  match_parent:"+newVertices._4.mkString("\n"))

    update_data_array(id, newVertices._2)

    return newVertices
  }

  def update_data_array(id:VertexId, flag:Boolean)={
    for (ele <- 0 until DataArray.length) {
      if (DataArray(ele)._1._1 == id){
        DataArray.update(ele, ((DataArray(ele)._1._1, flag), (DataArray(ele)._2._1, DataArray(ele)._2._2)))
        println("DataArray: "+DataArray.mkString(";"))
      }

      if(DataArray(ele)._2._1 == id){
        DataArray.update(ele, ((DataArray(ele)._1._1, DataArray(ele)._1._2), (DataArray(ele)._2._1, flag)))
        println("DataArray: "+DataArray.mkString(";"))
      }
    }
  }


  //remove an element from the array
  def remove(element:Long, dest:Array[Long]):Array[Long]={
    val tmp:Array[Long] = Array(element)
    dest.diff(tmp)
  }


  //If the node(id) has children but matched node in query doesn't, return false.
  //If the node(id) doesn't have children but matched node in query has't, return false. Otherwise true
  def check_children_and_parents(id:VertexId, match_set : Array[Long]):(Boolean, Array[Long]) ={

    var b_check_child : Array[(Long)] = Array()
    var b_check_parent : Array[(Long)] = Array()
    var result_array : Array[(Long)] = match_set
    var change:Boolean = false

    println("match_set: "+ match_set.mkString(" "))

    for(x <- QueryTripletsArray){
      if(match_set.contains(x.srcId)){
        b_check_child=b_check_child:+x.srcId
      }

      if(match_set.contains(x.dstId)){
        b_check_parent = b_check_parent:+x.dstId
      }
    }
    println("children: "+ b_check_child.mkString(" "))
    println("parent: "+ b_check_parent.mkString(" "))

    val b_check_data_child = DataArray.map { case (tri) =>  if(tri._1._1 == id && tri._2._2 == true) true  else false  }

    val b_check_data_parent = DataArray.map { case (tri) =>  if(tri._2._1 == id  &&  tri._1._2 == true) true  else false  }


    println("b_check_data_child: "+ b_check_data_child.mkString(" "))
    println("b_check_data_parent: "+ b_check_data_parent.mkString(" "))
    if( !b_check_data_child.contains(true) && b_check_child.length>0 ){   //if both has children and children flag is true
      for(x<-result_array){
        if(b_check_child.contains(x) ) {
          result_array=remove(x, result_array)
          change = true
        }
      }
    }
    if(!b_check_data_parent.contains(true) && b_check_parent.length>0){
      for(x<-result_array){
        if(b_check_parent.contains(x)) {
          result_array=remove(x, result_array)
          change = true
        }
      }
    }

    return (change, result_array)
  }


  def vprog(id:VertexId, dist: (String, Boolean, Array[Long], Array[String],Boolean,Boolean),
                        newDist:(String, Boolean, Array[Long], Array[String],Boolean,Boolean))
  :(String, Boolean, Array[Long], Array[String],Boolean,Boolean)={

    if (dist._6 == false){
      return init_nodes(id, dist)
    }
    else{

      var match_parent_array : Array[String] = Array("")
      var match_array: Array[Long] = Array(0)
      var change = false

      //If the current vertex is false, vote to halt
      if(dist._2 == false){
        //update_data_array(id, false)
        return (dist._1, false, match_array, match_parent_array, false, true)
      }

      //If the received message from the vertex is false
      if(newDist._2 == false){
        //var check : Array[(Long)] = check_children_and_parents(id, dist._3)
        var tmp = check_children_and_parents(id, dist._3)

        println("id: "+id+ " tmp: "+tmp._1+" - "+tmp._2.mkString(" "))
        if(tmp._2.length>0 ){
          return (dist._1, true,  tmp._2, match_parent_array, tmp._1, true)
        }
        else{
          update_data_array(id, false)
          return (dist._1, false, tmp._2, match_parent_array, tmp._1, true)
        }
      }


      for(node <- dist._3){             // Iterate each node in match_set
        var temp_match = QueryTripletsArray.map{case(tri) =>
          if(tri.srcId == node)                     //Find the matched node in Query
            if (newDist._3.contains(tri.dstId)){    //Compare with the new match_child_set.
              true
            }
            else{false}
          else{false}
        }

        if (temp_match.contains(true)){
          match_array = dist._3
          change = false
        }
        else{
          match_array= remove(node, dist._3)
          change = true
        }
      }

      if(match_array.length>0 ){
        (dist._1, true, match_array , match_parent_array, change, true)
      }
      else{
        update_data_array(id, false)
        (dist._1, false, match_array, match_parent_array, change, true)
      }
    }
  }

/*
  def sendMsg(triplet: EdgeTriplet[(String, Boolean, Array[Long], Array[String]),
    (String, Boolean, Array[Long], Array[String])])={

    if (triplet.dstAttr._2 == true) {
      Iterator((triplet.srcId, ("1", false, triplet.dstAttr._3, triplet.dstAttr._4)))
    } else {
      Iterator.empty
    }
  }
*/


  def mergeMsg(firstMsg : (String, Boolean, Array[Long], Array[String],Boolean,Boolean),
               secondMsg : (String, Boolean, Array[Long], Array[String],Boolean,Boolean)):
  (String, Boolean, Array[Long], Array[String],Boolean,Boolean) ={

    val match_set:Array[Long] = Array(0)
    var iniArr : Array[String] = Array("")

    if (firstMsg._2 == true && secondMsg._2 == true){
      var msg = firstMsg._3 ++ secondMsg._3
      return (firstMsg._1, true, msg, iniArr, true, false)
    }
    else if(firstMsg._2 == true && secondMsg._2 == false){
      return firstMsg
    }
    else if(firstMsg._2 == false && secondMsg._2 == true){
      return secondMsg
    }
    else{
      return ((firstMsg._1, false, match_set, iniArr, true, false))
    }
  }

  
  def build_DataGraph(sc:SparkContext): Graph[(String, Boolean, Array[Long], Array[String],Boolean,Boolean), String] = {

    val match_set:Array[Long] = Array()
    val match_parent_set:Array[String] = Array()

    // Create an RDD for the vertices
    val vertices_map: RDD[(VertexId,
      (String,          //label
      Boolean,          //if this vertex match any vertex in Query, set it true, otherwise false
      Array[Long],      //protential matched vertices of this vertex in Query graph
      Array[String],    //labels from this vertex's parents
      Boolean,          //the flag for any change occur in match_set
      Boolean)          //the flag for ini
      )] =
            sc.parallelize(Array((1L, ("A", false, match_set, match_parent_set, false, false)),
                (2L,("B",false,match_set,match_parent_set, false, false)),
                (3L,("A",false,match_set,match_parent_set, false, false)),
                (4L, ("B",false,match_set,match_parent_set, false, false)),
                (5L, ("C",false,match_set,match_parent_set, false, false))))
    
    // Create an RDD for edges
    var edges_map: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),
                       Edge(2, 3, ""), Edge(3, 4, ""), Edge(4, 5, "")))

    // Build the initial Graph
    var data = Graph(vertices_map, edges_map)
    
    return data
  }

  def build_QueryGraph(sc:SparkContext): Graph[String, String] = {

    // Create an RDD for the vertices
    val vertices_map: RDD[(VertexId, String)] =
      sc.parallelize(Array((1L, "A"), (2L,"B")))

    // Create an RDD for edges
    val edges_map: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, "")))

    // Build the initial Graph
    val query = Graph(vertices_map, edges_map)

    println("Create Query graph")

    return query
  }

}
