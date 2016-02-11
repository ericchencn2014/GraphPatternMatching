package GraphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import GraphTypes.SPMap

object GraphTest {

  var sc:SparkContext = null

  def init_sc(pass_sc:SparkContext)={
    sc = pass_sc
  }

  val children_match_set : Map[Long,Array[Long]] = Map()
  val parents_match_set : Map[Long,Array[Long]] = Map()

  val match_set_test:Array[Long] = Array()
  val match_parent_set_test:Array[Long] = Array()
  val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()

  val vertices_map_Query_t3: RDD[(VertexId, String)] =
    sc.parallelize(Array((1L, "A"), (2L,"B"), (3L,"C")))

  val edges_map_Query_t3: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),Edge(2, 3, "")))

  val vertices_map_Data_t3: RDD[(VertexId,SPMap)] =
    sc.parallelize(Array(
      (1L,Map(1L->("A", false, match_set_test, children_match_set , false, 1 , parents_match_set,tmp_triplets))),
      (2L,Map(2L->("B",false,match_set_test,children_match_set , false,1 , parents_match_set,tmp_triplets))),
      (3L,Map(3L->("C",false,match_set_test,children_match_set , false, 1 , parents_match_set,tmp_triplets))),
      (4L,Map(4L->("B",false,match_set_test,children_match_set , false,1 , parents_match_set,tmp_triplets))),
      (5L,Map(5L->("A",false,match_set_test,children_match_set , false,1 , parents_match_set,tmp_triplets))),
      (6L,Map(6L->("B",false,match_set_test,children_match_set , false,1 , parents_match_set,tmp_triplets))),
      (7L,Map(7L->("A",false,match_set_test,children_match_set , false, 1 , parents_match_set,tmp_triplets))),
      (8L,Map(8L->("C",false,match_set_test,children_match_set , false,1 , parents_match_set,tmp_triplets))),
      (9L,Map(9L->("D",false,match_set_test,children_match_set , false, 1 , parents_match_set,tmp_triplets)))
    ))

  val edges_map_Data_t3: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),Edge(2, 3, ""),
    Edge(1, 7, ""),    Edge(1, 5, ""),Edge(2, 6, ""),
    Edge(4, 3, ""),    Edge(3, 9, ""),Edge(7, 4, ""),
    Edge(4, 5, ""),    Edge(9, 5, ""),Edge(5, 6, ""),
    Edge(6, 7, ""),    Edge(6, 8, "")
  ))

//
//  val vertices_map_Query_t1: RDD[(VertexId, String)] =
//    sc.parallelize(Array((1L, "A"), (2L,"B")))
//
//  // Create an RDD for edges
//  val edges_map_Query_t1: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, "")))
//
//
//  val vertices_map_Data_t2: RDD[(VertexId,
//    (String,          //label
//      Boolean,          //if this vertex match any vertex in Query, set it true, otherwise false
//      Array[Long],      //protential matched vertices of this vertex in Query graph
//      Array[Long],    //labels from this vertex's parents
//      Boolean,          //the flag for any change occur in match_set
//      Boolean,Boolean)          //the flag for ini
//    )] =
//    sc.parallelize(Array((1L, ("A", false, match_set_test, match_parent_set_test, false,false, false)),
//      (2L,("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (3L,("A",false,match_set_test,match_parent_set_test, false,false, false)),
//      (4L, ("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (5L, ("C",false,match_set_test,match_parent_set_test, false,false, false))))
//
////////////////////////////////////////////////////////////////////////////////////////
//
//
//  val tmp1 : SPMap = Map(1L ->("A", false, match_set_test, children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp2 : SPMap = Map(2L->("B", false, match_set_test, children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp3 : SPMap = Map(3L->("A", false, match_set_test, children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp4 : SPMap = Map(4L->("B", false, match_set_test, children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp5 : SPMap = Map(5L ->("C", false, match_set_test, children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val vertices_map_Data_t7: RDD[(VertexId,SPMap)] =
//    sc.parallelize(Array((1L, tmp1),      (2L,tmp2),      (3L,tmp3),      (4L,tmp4),      (5L,tmp5)))
//
//  var edges_map_Data_t7: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),
//    Edge(2, 3, ""), Edge(3, 4, ""), Edge(4, 5, "")))
//
//
//  //////////////////////////////////////////////////////////////////////////////////////
//
//
//
//  var edges_map_Data_t2: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),
//    Edge(2, 3, ""), Edge(3, 4, ""), Edge(4, 5, "")))
//
//
//  val vertices_map_Data_t4: RDD[(VertexId,
//    (String,          //label
//      Boolean,          //if this vertex match any vertex in Query, set it true, otherwise false
//      Array[Long],      //protential matched vertices of this vertex in Query graph
//      Array[Long],    //labels from this vertex's parents
//      Boolean,          //the flag for any change occur in match_set
//      Boolean,Boolean           //the flag for ini
//      //true is the parent, false is the child
//      )
//    )] =
//    sc.parallelize(Array(
//      (1L, ("A", false, match_set_test, match_parent_set_test, false, false,false)),
//      (2L,("B",false,match_set_test,match_parent_set_test, false, false,false)),
//      (3L,("C",false,match_set_test,match_parent_set_test, false,false, false)),
//      (4L, ("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (5L, ("A",false,match_set_test,match_parent_set_test, false,false, false)),
//      (6L, ("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (7L, ("C",false,match_set_test,match_parent_set_test, false, false,false)),
//      (8L, ("C",false,match_set_test,match_parent_set_test, false, false,false)),
//      (9L, ("A",false,match_set_test,match_parent_set_test, false, false,false)),
//      (10L, ("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (11L, ("A",false,match_set_test,match_parent_set_test, false,false, false)),
//      (12L, ("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (13L, ("A",false,match_set_test,match_parent_set_test, false,false, false)),
//      (14L, ("B",false,match_set_test,match_parent_set_test, false,false, false)),
//      (15L, ("C",false,match_set_test,match_parent_set_test, false,false, false))
//    ))
//
//  //Create an RDD for edges
//  var edges_map_Data_t4: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),
//    Edge(2, 3, ""), Edge(1, 4, ""), Edge(1, 11, ""), Edge(1, 13, "")
//    , Edge(4, 5, ""), Edge(4, 7, ""), Edge(5, 6, ""), Edge(6, 8, ""), Edge(6, 1, ""), Edge(9, 10, ""), Edge(9, 1, "")
//    , Edge(10, 1, ""), Edge(10, 8, ""),Edge(11, 12, ""), Edge(12, 13, ""), Edge(12, 15, ""), Edge(13, 14, "")
//    , Edge(14, 15, ""), Edge(14, 9, "")
//  ))
//
//
//
//  val vertices_map_Data_t5: RDD[(VertexId,
//    (String,          //label
//      Boolean,          //if this vertex match any vertex in Query, set it true, otherwise false
//      Array[Long],      //protential matched vertices of this vertex in Query graph
//      Array[Long],    //labels from this vertex's parents
//      Boolean,          //the flag for any change occur in match_set
//      Boolean,Boolean           //the flag for ini
//      //true is the parent, false is the child
//      )
//    )] =
//    sc.parallelize(Array((1L, ("A", false, match_set_test, match_parent_set_test, false,false, false)),
//      (2L,("B",false,match_set_test,match_parent_set_test, false, false, false)),
//      (3L,("C",false,match_set_test,match_parent_set_test, false, false, false)),
//      (4L, ("B",false,match_set_test,match_parent_set_test, false, false, false)),
//      (5L, ("A",false,match_set_test,match_parent_set_test, false, false, false)),
//      (6L, ("B",false,match_set_test,match_parent_set_test, false, false, false)),
//      (7L, ("A",false,match_set_test,match_parent_set_test, false, false, false)),
//      (8L, ("C",false,match_set_test,match_parent_set_test, false, false, false)),
//      (9L, ("D",false,match_set_test,match_parent_set_test, false, false, false))))
//
//  val edges_map_Data_t5: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),Edge(2, 3, ""),
//    Edge(1, 7, ""),    Edge(1, 5, ""),Edge(2, 6, ""),
//    Edge(4, 3, ""),    Edge(3, 9, ""),Edge(7, 4, ""),
//    Edge(4, 5, ""),    Edge(9, 5, ""),Edge(5, 6, ""),
//    Edge(6, 7, ""),    Edge(6, 8, "")
//  ))
//
//
//
//
////////////////////////////////////////////////////////////////////////////////////////////////////////
//  val vertices_map_Query_t6: RDD[(VertexId, String)] =
//    sc.parallelize(Array(
//      (55L,"3"),
//      (214L,"87"),
//      (290L,"36"),
//      (546L,"13")
//    ))
//
//  // Create an RDD for edges
//  val edges_map_Query_t6: RDD[Edge[String]] =sc.parallelize(Array(
//    Edge(55,214,""),
//    Edge(290,214,""),
//    Edge(546,55,""),
//    Edge(546,214,""),
//    Edge(546,290,"")
//  ))
//
//  val tmp36 : SPMap = Map(36L->("3",false,match_set_test,children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp55 : SPMap = Map(55L->("3",false,match_set_test,children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp81 : SPMap = Map(81L->("87",false,match_set_test,children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp214 : SPMap = Map(214L->("87",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp290 : SPMap = Map(290L->("36",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp308 : SPMap = Map(308L->("3",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp319 : SPMap = Map(319L->("13",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp405 : SPMap = Map(405L->("3",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp546 : SPMap = Map(546L->("13",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp663 : SPMap = Map(663L->("3",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp793 : SPMap = Map(793L->("3",false,match_set_test,children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp795 : SPMap = Map(795L->("13",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp935 : SPMap = Map(935L->("87",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp1305 : SPMap = Map(1305L->("3",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp1754 : SPMap = Map(1754L->("36",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp1953 : SPMap = Map(1953L->("3",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp1963 : SPMap = Map(1963L->("87",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp2133 : SPMap = Map(2133L->("3",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp2476 : SPMap = Map(2476L->("13",false,match_set_test,children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val tmp3408 : SPMap = Map(3408L->("36",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp5295 : SPMap = Map(5295L->("13",false,match_set_test,children_match_set,false,1 , parents_match_set,tmp_triplets))
//  val tmp6715 : SPMap = Map(6715L->("87",false,match_set_test,children_match_set,false, 1 , parents_match_set,tmp_triplets))
//  val vertices_map_Data_t6: RDD[(VertexId,SPMap    )] =
//    sc.parallelize(Array(
//      (36L,tmp36),
//      (55L,tmp55),
//      (81L,tmp81),
//      (214L,tmp214),
//      (290L,tmp290),
//      (308L,tmp308),
//      (319L,tmp319),
//      (405L,tmp405),
//      (546L,tmp546),
//      (663L,tmp663),
//      (793L,tmp793),
//      (795L,tmp795),
//      (935L,tmp935),
//      (1305L,tmp1305),
//      (1754L,tmp1754),
//      (1953L,tmp1953),
//      (1963L,tmp1963),
//      (2133L,tmp2133),
//      (2476L,tmp2476),
//      (3408L,tmp3408),
//      (5295L,tmp5295),
//      (6715L,tmp6715)
//    ))
//
//
//
//
//
//
//  val edges_map_Data_t6: RDD[Edge[String]] =sc.parallelize(Array(
//    Edge(36,55,""),
//    Edge(36,308,""),
//    Edge(36,663,""),
//    Edge(55,36,""),
//    Edge(55,214,""),
//    Edge(55,308,""),
//    Edge(55,793,""),
//    Edge(55,795,""),
//    Edge(81,55,""),
//    Edge(81,214,""),
//    Edge(290,214,""),
//    Edge(290,1305,""),
//    Edge(290,1754,""),
//    Edge(290,3408,""),
//    Edge(308,36,""),
//    Edge(308,663,""),
//    Edge(319,214,""),
//    Edge(319,290,""),
//    Edge(319,405,""),
//    Edge(319,6715,""),
//    Edge(405,290,""),
//    Edge(546,55,""),
//    Edge(546,214,""),
//    Edge(546,290,""),
//    Edge(546,1953,""),
//    Edge(546,1963,""),
//    Edge(546,5295,""),
//    Edge(546,6715,""),
//    Edge(663,36,""),
//    Edge(663,214,""),
//    Edge(663,290,""),
//    Edge(663,1754,""),
//    Edge(795,290,""),
//    Edge(935,290,""),
//    Edge(1305,214,""),
//    Edge(1305,290,""),
//    Edge(1305,1754,""),
//    Edge(1305,3408,""),
//    Edge(2133,214,""),
//    Edge(2476,290,""),
//    Edge(2476,1754,"")
//  ))

}
