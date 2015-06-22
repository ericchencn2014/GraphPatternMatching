/**
 * @author Chao Chen

 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphPatternMatching {

  val sc = new SparkContext("local", "GraphPatternMatching")
  val QueryGraph : Graph[String,String] = build_QueryGraph(sc)
  val DataGraph : Graph[(String, Boolean,Array[Long], Array[String],Boolean),String] = build_DataGraph(sc)

  val QueryLabels :  Array[(String)] = QueryGraph.vertices.map{ case (id,label) => label}.collect.distinct
  val QueryVerticesArray = QueryGraph.vertices.toArray()
  val QueryTripletsArray = QueryGraph.triplets.toArray()

  val QuerySortMap : Map[String,Array[Long]] = extract_array()

  QuerySortMap.foreach((x) => println("label: "+ x._1 + "  array: "+ x._2.mkString(" ")))
  //println("map: "+sort_map.mkString("\n"))

  val DataTripletsArray = DataGraph.triplets.toArray()

  def main(args: Array[String]) {

    println("Query Labels: " + QueryLabels.deep.mkString(" , "))
    graph_simulation(DataGraph)

    //strict_simulation

  }


  
  def strict_simulation() = {

    
    //graph_simulation(DataGraph, QueryGraph.QueryGraph)
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
  
  def graph_simulation(data:Graph[(String, Boolean,Array[Long], Array[String],Boolean),String]) : Boolean = {

    val iniArrayChild : Array[Long] = Array()
    val iniArrayParent : Array[String] = Array("A")

    println("Start Pregel")

    val result_graph = data.pregel(("", false, iniArrayChild, iniArrayParent, false), 10, EdgeDirection.Either )(vprog,

      triplet => {
        println("--------------DstID:->"+triplet.dstId+"  SrcId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3.mkString(""))
        if (triplet.dstAttr._5 == true) {
          println("DstID:->"+triplet.dstId+"  SrcId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3.mkString(""))
          Iterator((triplet.srcId, (triplet.dstAttr._1, triplet.dstAttr._2, triplet.dstAttr._3, triplet.dstAttr._4, false)))
          //Iterator((triplet.dstId, (triplet.srcAttr._1, true, triplet.srcAttr._3, triplet.srcAttr._4)))
        } else {
          Iterator.empty
        }
      },
      mergeMsg)
    
    return true
  }


  
  def init_nodes( dist: (String, Boolean, Array[Long], Array[String],Boolean)) :
  (String, Boolean, Array[Long], Array[String],Boolean) = {

    val iniArrayChild : Array[Long] = Array()
    val iniArrayParent : Array[String] = Array("")
    var newVertices = ("", false, iniArrayChild, iniArrayParent, false)

    if(QueryLabels contains(dist._1)){
      var temMatch_child: Array[Long] = QuerySortMap(dist._1)

      newVertices = (dist._1, true, temMatch_child, dist._4, true)
    } else{newVertices = (dist._1, false, dist._3, dist._4, false)}

    println("Init match set: " + newVertices._3.mkString("\n"))

    return newVertices
  }

  def remove(num:Long, dest:Array[Long]):Array[Long]={
    val tmp:Array[Long] = Array(num)
    dest.diff(tmp)
  }


  //If the node(id) has children but node in query doesn't, return false.
  //If the node(id) doesn't have children but node in query has't, return false. Otherwise true
  def check_children(id:VertexId, match_set : Array[Long]):Boolean ={

    //the node in query has children?
    var b_check_query = QueryTripletsArray.map{case(tri) => if(match_set.contains(tri.srcId)) true else false }

    var b_check = DataTripletsArray.map { case (tri) =>  if(tri.srcId == id) true  else false  }

    if(b_check.contains(true) && b_check_query.contains(true)){
      true
    }
    else if( !b_check.contains(true) && !b_check_query.contains(true) ){
      true
    }
    else{
      false
    }
  }

  def check_parent(id:VertexId, match_set : Array[Long]):Boolean ={

    //the node in query has children?
    var b_check_query = QueryTripletsArray.map{case(tri) => if(match_set.contains(tri.dstId)) true else false }

    var b_check = DataTripletsArray.map { case (tri) =>  if(tri.dstId == id) true  else false  }

    if(b_check.contains(true) && b_check_query.contains(true)){
      true
    }
    else if( !b_check.contains(true) && !b_check_query.contains(true) ){
      true
    }
    else{
      false
    }
  }


  def vprog(id:VertexId, dist: (String, Boolean, Array[Long], Array[String],Boolean),
            newDist:(String, Boolean, Array[Long], Array[String],Boolean))
  :(String, Boolean, Array[Long], Array[String],Boolean)={

    if (newDist._2 == false  && newDist._1 == ""){
      return init_nodes(dist)
    }
    else{

      var match_parent_array : Array[String] = Array("")
      var match_array: Array[Long] = Array()
      var change = false

      //If the current vertex is false
      if(dist._2 == false){
        return (dist._1, false, match_array, match_parent_array, true)
      }

      //If the message from the vertex is false
      if(newDist._2 == false){
        if(check_children(id, dist._3) == false){
          return (dist._1, false, match_array, match_parent_array, true)
        }

        if(check_parent(id, dist._3) == false){
          return (dist._1, false, match_array, match_parent_array, true)
        }

        return (dist._1, true, match_array, match_parent_array, false)
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

        if (temp_match.contains(true)){change = false}
        else{
          match_array= remove(node, dist._3)
          change = true
        }
      }

      if(match_array.length>0 ){
        (dist._1, true, match_array, match_parent_array, change)
      }
      else{
        (dist._1, false, match_array, match_parent_array, change)
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


  def mergeMsg(firstMsg : (String, Boolean, Array[Long], Array[String],Boolean),
               secondMsg : (String, Boolean, Array[Long], Array[String],Boolean)):
  (String, Boolean, Array[Long], Array[String],Boolean) ={

    var msg = firstMsg._3 ++ secondMsg._3
    var iniArr : Array[String] = Array("A")
    return ("", true, msg, iniArr, firstMsg._5)
  }

  
  def build_DataGraph(sc:SparkContext): Graph[(String, Boolean, Array[Long], Array[String],Boolean), String] = {

    val match_set = new Array[Long](0)
    val match_parent_set = new Array[String](0)

    // Create an RDD for the vertices
    val vertices_map: RDD[(VertexId,
      (String,          //label
      Boolean,          //if this vertex match any vertex in Query, set it true, otherwise false
      Array[Long],      //protential matched vertices of this vertex
      Array[String],    //labels from this vertex's parents
      Boolean)          //the flag for every change in match_set
      )] =
            sc.parallelize(Array((1L, ("A", false, match_set, match_parent_set, false)),
                (2L,("B",false,match_set,match_parent_set, false)),
                (3L,("A",false,match_set,match_parent_set, false)),
                (4L, ("B",false,match_set,match_parent_set, false)),
                (5L, ("C",false,match_set,match_parent_set, false))))
    
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
