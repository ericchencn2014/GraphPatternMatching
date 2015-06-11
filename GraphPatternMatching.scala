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
    
    val sc = new SparkContext("spark://sparkmaster:7077", "GraphPatternMatching")  
   
    var data_graph : Graph[(String, Boolean,Array[Int], Array[String]),String] = build_data_graph(sc)
    
    var query_graph : Graph[String,String] = build_query_graph(sc)
    
    dual_simulation(data_graph, query_graph)
  }
  
  def dual_simulation(data:Graph[(String, Boolean,Array[Int], Array[String]),String], query:Graph[String,String]) : Boolean = {
    
    var newData =  init_nodes(data, query)
      


        
    
    return true
  }
  
  def init_nodes(data:Graph[(String, Boolean,Array[Int], Array[String]),String], query:Graph[String,String]) : 
          Graph[Any, String] = {
        
    var ver_labels :  Array[(String)] = query.vertices.map{ case (id,label) => label}.collect.distinct
    
    println("query labels:"+ver_labels) 
  
    
    val newVertices  = data.mapVertices{ case(id, attr) => 
      
      if(ver_labels contains(attr._1)) 
        {
          var temMatch_child: Array[AnyVal] = query.vertices.map{ case(id, label) => 
            if ((label) == (attr._1.toString))
            { 
              id.toInt
            }
            else{}
            }.collect
            
          var temMatch_parent: Array[AnyVal] = query.vertices.map{ case(id, label) => 
            if ((label) == (attr._1.toString))
            { 
              id
            }
            else{}
            }.collect
            
          (id, (attr._1, true, temMatch_child, attr._4))
        }
      else{}}
        
   
    return newVertices
  }

  
  def build_data_graph(sc:SparkContext): Graph[(String, Boolean, Array[Int], Array[String]), String] = {
    
    var match_child_set = new Array[Int](0)
    var match_parent_set = new Array[String](0)
    
    // Create an RDD for the vertices
    var vertices_map: RDD[(VertexId, (String, Boolean, Array[Int], Array[String]))] =
            sc.parallelize(Array((1L, ("A", false, match_child_set, match_parent_set)), 
                (2L,("B",false,match_child_set,match_parent_set)), 
                (3L,("A",false,match_child_set,match_parent_set)), 
                (4L, ("B",false,match_child_set,match_parent_set)), 
                (5L, ("C",false,match_child_set,match_parent_set))))
    
    // Create an RDD for edges
    var edges_map: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),
                       Edge(2, 3, ""), Edge(3, 4, ""), Edge(4, 5, "")))

    // Build the initial Graph
    var graph = Graph(vertices_map, edges_map)
    
    return graph
  }  
  
  def build_query_graph(sc:SparkContext): Graph[String, String] = {
    
    // Create an RDD for the vertices
    var vertices_map: RDD[(VertexId, String)] =
            sc.parallelize(Array((1L, "A"), (2L,"B")))
    
    // Create an RDD for edges
    var edges_map: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, "")))

    // Build the initial Graph
    var graph = Graph(vertices_map, edges_map)
    
    return graph
  }  
}
