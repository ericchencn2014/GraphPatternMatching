/**
 * @author Chao Chen

 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphPatternMatching {

  val sc = new SparkContext("local", "GraphPatternMatching")
  val QueryGraph : Graph[String,String] = build_QueryGraph(sc)
  val DataGraph : Graph[(String, Boolean,Array[Long], Array[String]),String] = build_DataGraph(sc)

  val QueryLabels :  Array[(String)] = QueryGraph.vertices.map{ case (id,label) => label}.collect.distinct
  val QueryVerticesArray = QueryGraph.vertices.toArray()
  val QueryTripletsArray = QueryGraph.triplets.toArray()


  def main(args: Array[String]) {

    println("Query Labels: " + QueryLabels.deep.mkString(" , "))
    graph_simulation(DataGraph)

    //strict_simulation

  }


  
  def strict_simulation() = {


    
    //graph_simulation(DataGraph, QueryGraph.QueryGraph)
  }
  
  def graph_simulation(data:Graph[(String, Boolean,Array[Long], Array[String]),String]) : Boolean = {

    val iniArrayChild : Array[Long] = Array(1)
    val iniArrayParent : Array[String] = Array("A")

    println("Start Pregel")

    val result_graph = data.pregel(("A", false, iniArrayChild, iniArrayParent), 1, EdgeDirection.Either )(vprog,

      triplet => {
        println("--------------DstID:->"+triplet.dstId+"  SrcId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3)
        if (triplet.dstAttr._2 == true) {
          println("DstID:->"+triplet.dstId+"  SrcId:->"+triplet.srcId+"  array: " + triplet.dstAttr._3)
          //Iterator((triplet.srcId, (triplet.dstAttr._1, true, triplet.dstAttr._3, triplet.dstAttr._4)))
          Iterator((triplet.dstId, (triplet.srcAttr._1, true, triplet.srcAttr._3, triplet.srcAttr._4)))
        } else {
          Iterator.empty
        }
      },
      mergeMsg)
    
    return true
  }


  
  def init_nodes( dist: (String, Boolean, Array[Long], Array[String])) :
  (String, Boolean, Array[Long], Array[String]) = {

    var sort_map : Map[String,Array[Long]] = Map()

    for(ele <- QueryLabels)
      {

        var tmp_array : Array[Long] = Array()

        for(lab <- QueryVerticesArray){
          if(lab._2 == ele){
            tmp_array = tmp_array :+ lab._1
          }
        }
        sort_map+= (ele -> tmp_array)
      }

    val iniArrayChild : Array[Long] = Array()
    val iniArrayParent : Array[String] = Array("")
    var newVertices = ("", false, iniArrayChild, iniArrayParent)

    if(QueryLabels contains(dist._1)){
      var temMatch_child2: Array[Long] = sort_map(dist._1)

      newVertices = (dist._1, true, temMatch_child2, dist._4)
    } else{newVertices = (dist._1, false, dist._3, dist._4)}


    println("Init newVertices: " + newVertices._3.mkString("\n"))

    return newVertices
  }

  def remove(num:Long, dest:Array[Long]):Array[Long]={
    val tmp:Array[Long] = Array(num)
    dest.diff(tmp)
  }

  def vprog(id:VertexId, dist: (String, Boolean, Array[Long], Array[String]), newDist:(String, Boolean, Array[Long], Array[String]))
  :(String, Boolean, Array[Long], Array[String])={

    if (newDist._2 == false){
      init_nodes(dist)
    }
    else{

      var iniArr : Array[String] = Array("A")
      var match_array: Array[Long] = Array()
      for(ele <- dist._3){


        var temp1 = QueryTripletsArray.map{case(tri) =>
          if(tri.srcId == ele)
            if (newDist._3 contains(tri.dstId)){
              true
            }
            else{false}
          else{false}
        }
        if (temp1 contains(true)){}
        else{
          match_array= remove(ele, dist._3)
        }

      }

      if(match_array.length>0){
        (dist._1, true, match_array, iniArr)
      }
      else{
        (dist._1, false, match_array, iniArr)
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


  def mergeMsg(firstMsg : (String, Boolean, Array[Long], Array[String]), secondMsg : (String, Boolean, Array[Long], Array[String])):
  (String, Boolean, Array[Long], Array[String]) ={

    var msg = firstMsg._3 ++ secondMsg._3
    var iniArr : Array[String] = Array("A")
    return ("", true, msg, iniArr)
  }

  
  def build_DataGraph(sc:SparkContext): Graph[(String, Boolean, Array[Long], Array[String]), String] = {

    val match_child_set = new Array[Long](0)
    val match_parent_set = new Array[String](0)

    // Create an RDD for the vertices
    val vertices_map: RDD[(VertexId, (String, Boolean, Array[Long], Array[String]))] =
            sc.parallelize(Array((1L, ("A", false, match_child_set, match_parent_set)), 
                (2L,("B",false,match_child_set,match_parent_set)), 
                (3L,("A",false,match_child_set,match_parent_set)), 
                (4L, ("B",false,match_child_set,match_parent_set)), 
                (5L, ("C",false,match_child_set,match_parent_set))))
    
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
