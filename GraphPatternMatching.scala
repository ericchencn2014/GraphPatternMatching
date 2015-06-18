/**
 * @author Chao Chen

 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



object GraphPatternMatching {
  def main(args: Array[String]) {

    strict_simulation

  }

  
  def strict_simulation() = {
    
    val sc = new SparkContext("local", "GraphPatternMatching")
   
    var data_graph : Graph[(String, Boolean,Array[Long], Array[String]),String] = build_data_graph(sc)
    
    var query_graph : Graph[String,String] = build_query_graph(sc)
    
    dual_simulation(data_graph, query_graph)
  }
  
  def dual_simulation(data:Graph[(String, Boolean,Array[Long], Array[String]),String], query:Graph[String,String]) : Boolean = {
    
    var newData =  init_nodes(data, query)

    val result_graph = newData.pregel()
    
    return true
  }

  
  
  def init_nodes(data:Graph[(String, Boolean,Array[Long], Array[String]),String], query:Graph[String,String]) :
          Graph[(String, Boolean,Array[Long], Array[String]),String] = {
        
    var ver_labels :  Array[(String)] = query.vertices.map{ case (id,label) => label}.collect.distinct

    println(ver_labels.deep.mkString(" , "))


    var sort_map : Map[String,Array[Long]] = Map()

    for(ele <- ver_labels)
      {
        var tmp_1 = query.vertices.toArray()
        var tmp_array : Array[Long] = Array()

        for(lab <- tmp_1){
          if(lab._2 == ele){
            tmp_array = tmp_array :+ lab._1
          }
        }
        sort_map+= (ele -> tmp_array)
      }

    var newVertices = data.mapVertices((id, attr) =>  if(ver_labels contains(attr._1)){

      var temMatch_child2: Array[Long] = sort_map(attr._1)
      println("id: "+id+"array: "+temMatch_child2.mkString(" "))

      (attr._1, true, temMatch_child2, attr._4)
    } else{(attr._1, false, attr._3, attr._4)})

    println(newVertices.vertices.take(5).mkString("\n"))


    return newVertices
  }


  def vprog(id:VertexId, dist: Array[AnyVal], newDist:Array[AnyVal])={
    println(id, dist, newDist)
    math.min(dist, newDist)
  }
  /*


    def sendMsg(triplet: edgetriplet[Double, Double]) :Iterator[(VertexId, Array[edge[Double]])]={

      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.srcId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }

    }
  */


  
  def build_data_graph(sc:SparkContext): Graph[(String, Boolean, Array[Long], Array[String]), String] = {

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
  
  def build_query_graph(sc:SparkContext): Graph[String, String] = {
    
    // Create an RDD for the vertices
    var vertices_map: RDD[(VertexId, String)] =
            sc.parallelize(Array((1L, "A"), (2L,"B")))
    
    // Create an RDD for edges
    var edges_map: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, "")))

    // Build the initial Graph
    var query = Graph(vertices_map, edges_map)
    
    return query
  }  
}
