/** @author  Chao Chen
  *  @version 1.0
  *  @date    Thu Aug 17 2015
  *  @see     LICENSE (MIT style license file).
  */

package GraphX


//import java.io.{File, PrintWriter}

import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import GraphTypes._
import CommonUse._

import scala.Array._
import org.apache.spark.broadcast.Broadcast

import scala.util.control.Breaks




//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** This `DualSimulation` object provides an implementation for Distributed Dual Simulation
  *  the algorithm refer to http://cobweb.cs.uga.edu/~ar/papers/IJBD_final.pdf
  *  @param data  the data graph
  *  @param query  the query graph, this parameter is not necessary
  *  @return the graph after dual simulation filter
  *
  *  @warnning this class should "extends java.io.Serializable", Otherwise it's unserializable
  */
class  DualSimulation(data: Graph[SPMap,Int], query: Graph[String,Int],sc:SparkContext, query_tri : Array[(Long, Long)]) extends java.io.Serializable {

  val for_test :Array[Int]= Array()
//  val for_test :Array[Int]= Array(667 , 11 , 1 , 450 , 58 , 292 , 834 , 664 , 467 , 740 , 7 ,
//    734 , 267 , 4 , 152 , 683 , 415 , 74 , 224 , 750 , 121 , 446 , 163 ,
//    171 , 977 , 332 , 676 , 411 , 556 , 647 , 963 , 760 , 457 , 440 , 876 ,
//    319 , 941 , 418 , 90 , 20 , 614 , 391 , 607 , 426 , 242 , 108 , 700 , 468 ,
//    986 , 659 , 605 , 367 , 331 , 420 , 731 , 363 , 92 , 84 , 430 , 181 , 984 ,
//    438 , 164 , 8 , 439 , 650 , 111 , 730 , 462 , 53 , 190 , 511 , 477 , 286 ,
//    294 , 43 , 921 , 240 , 295 , 433 , 130 , 309 , 544 , 783 , 336 , 669 , 134 ,
//    202 , 41 , 395 , 617 , 877 , 85 , 982 , 570 , 648 , 416 , 724 , 880 , 384)

//  val DataTripletsWhole = data.triplets.map{ tri =>  ((tri.srcId,tri.srcAttr.head._2._1), (tri.dstId,tri.dstAttr.head._2._1))}.collect
//
//  val DataTri = sc.broadcast(DataTripletsWhole)


  //collect all labels in Query graph
  private val QueryLabelsVal:Array[String] = query.vertices.map{ case (id,label) => label}.collect.distinct

  //extract Query graph information
  private val QueryVerticesArray = query.vertices.collect()

//  private val QueryTripletsArray = query.triplets.map{ tri =>  ((tri.srcId), (tri.dstId))}.collect
  private val QueryTripletsArray = query_tri

  private val QueryMapLabels = extract_array()

  private val bool_ouput = false

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** apply dual simulation to the data graph and query graph according to the algorithm
    * @param model model=1 : computing by using new method
    * @param compare whether compare the result with pattern, compare=1 : run comparison
    *  @return the graph after dual simulation filter
    */
//  def apply_dual_simulation(model : Int, compare : Int) :  Graph[SPMap,Int]= {
//
//    val iniArray : Array[Long] = Array()
//    val ini_match_set : Map[Long,Array[Long]] = Map()
//
//    val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
//
//    out_put_log("Start Pregel")
//
//    val ini_spmap : SPMap = Map(1L ->(" ", false, iniArray, ini_match_set,false, 1,ini_match_set,tmp_triplets ))
////      val ini_spmap : SPMap = Map(1L ->(" ", false, iniArray, ini_match_set,false, Int.MaxValue,ini_match_set,tmp_triplets ))
//
//    val start_sec:Double = get_current_second()
//
//    val result_graph = data.pregel(ini_spmap,1, EdgeDirection.Either )(
//      if(model==1){
//        vprog_test
//      }else{
//        vprog_original
//      }
////    vprog_sssp
//      ,
//
//      triplet => {
//        if (( triplet.dstAttr.head._2._5 == true || triplet.srcAttr.head._2._5 == true)
//        ) {
//
//          val it1 = Iterator((triplet.srcId, Map(triplet.dstId ->( triplet.dstAttr.head._2._1, triplet.dstAttr.head._2._2, triplet.dstAttr.head._2._3,  ini_match_set, false,
//            triplet.dstAttr.head._2._6, ini_match_set,tmp_triplets))))
//          val it2 = Iterator((triplet.dstId, Map(triplet.srcId ->(triplet.srcAttr.head._2._1, triplet.srcAttr.head._2._2, triplet.srcAttr.head._2._3,  ini_match_set,false,
//            triplet.srcAttr.head._2._6, ini_match_set,tmp_triplets))))
//
//          it1++it2
//        }
//        else {
//          Iterator.empty
//        }
//      }
//      ,  (a, b) => a++b
//    )
//
//
//    val mid_dual_sec:Double = get_current_second()
//    out_put_log("Dual process cost(seconds): "+ (mid_dual_sec-start_sec))
//
//    val subgraph = result_graph.subgraph(vpred = (v_id, attr) => attr.head._2._2 == true )
//
//    if(compare ==1){
//
//      out_put_log("dual graph**************************************************************************:")
//
//      val result_vertices:Array[Long] = subgraph.vertices.map{ case(v,_) => v}.collect()
//      val given_vertices :Array[Long] = QueryVerticesArray.map{case(v,_) => v}
//      check_result(result_vertices,given_vertices)
//
//    }
//
//    return subgraph
////    return result_graph
//  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** apply dual simulation to the data graph and query graph according to the algorithm
    * @param model model=1 : computing by using new method
    * @param compare whether compare the result with pattern, compare=1 : run comparison
    *  @return the graph after dual simulation filter
    */
  def apply_dual_simulation(model : Int, compare : Int) :  Graph[SPMap,Int]= {

    val iniArray : Array[Long] = Array()
    val ini_match_set : Map[Long,Array[Long]] = Map()

    val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()

    out_put_log("iteration : "+model)
    out_put_log("Start Pregel")

    val ini_spmap : SPMap = Map(1L ->(" ", false, iniArray, ini_match_set,false, 1,ini_match_set,tmp_triplets ))

    val start_sec:Double = get_current_second()


    val result_graph = data.pregel(ini_spmap, model, EdgeDirection.Either )(
      vprog_test
        ,
      triplet => {
        if (( triplet.dstAttr.head._2._5 == true || triplet.srcAttr.head._2._5 == true)
        ) {
          val it1 = Iterator((triplet.srcId, Map(triplet.dstId ->( triplet.dstAttr.head._2._1, triplet.dstAttr.head._2._2, triplet.dstAttr.head._2._3,  ini_match_set, false,
            triplet.dstAttr.head._2._6, ini_match_set,tmp_triplets))))
          val it2 = Iterator((triplet.dstId, Map(triplet.srcId ->(triplet.srcAttr.head._2._1, triplet.srcAttr.head._2._2, triplet.srcAttr.head._2._3,  ini_match_set,false,
            triplet.srcAttr.head._2._6, ini_match_set,tmp_triplets))))
          it1++it2
        }
        else {
          Iterator.empty
        }
      }
      ,  (a, b) => a++b
    )


    val mid_dual_sec:Double = get_current_second()
    out_put_log("Dual process cost(seconds): "+ (mid_dual_sec-start_sec))

    val subgraph = result_graph.subgraph(vpred = (v_id, attr) => attr.head._2._2 == true )

    if(compare ==1){

      out_put_log("dual graph**************************************************************************:")

      val result_vertices:Array[Long] = subgraph.vertices.map{ case(v,_) => v}.collect()
      val given_vertices :Array[Long] = QueryVerticesArray.map{case(v,_) => v}
      check_result(result_vertices,given_vertices)

    }

    return subgraph
  }


  def sendMessage(triplet: EdgeTriplet[SPMap, Int]): Iterator[(VertexId, SPMap)] ={

    val ini_match_set : Map[Long,Array[Long]] = Map()

    val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()

    if (( triplet.dstAttr.head._2._5 == true || triplet.srcAttr.head._2._5 == true)
    ) {
      val it1 = Iterator((triplet.srcId, Map(triplet.dstId ->( triplet.dstAttr.head._2._1, triplet.dstAttr.head._2._2, triplet.dstAttr.head._2._3,  ini_match_set, false,
        triplet.dstAttr.head._2._6, ini_match_set,tmp_triplets))))
      val it2 = Iterator((triplet.dstId, Map(triplet.srcId ->(triplet.srcAttr.head._2._1, triplet.srcAttr.head._2._2, triplet.srcAttr.head._2._3,  ini_match_set,false,
        triplet.srcAttr.head._2._6, ini_match_set,tmp_triplets))))

      it1++it2
    }
    else {
      Iterator.empty
    }

  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** a new idea,remove edges, to apply dual simulation to the data graph and query graph, but low efficient now,wait for improving
    *  @return the graph after dual simulation filter
    */
  def vprog_new(id:VertexId, dist: SPMap,  newDist:SPMap)  :SPMap={

    if (dist.head._2._6== 1){
      return init_nodes_new(id, dist)
    }
    else{
      val superstep : Int = dist.head._2._6 +1

      //If the current vertex is false, vote to halt
      if(dist.head._2._2 == false || dist.head._2._3.length<=0){

//        out_put_log("dist.head._2._2 == false  id: " + id)

        //If the current vertex is invalided, reset itself, doesn't send any message
        val tmp_array : Array[Long] = Array()
        val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
        val tmp_match_set : Map[Long,Array[Long]] = Map()

        val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, false, tmp_array, tmp_match_set, false,superstep, tmp_match_set, tmp_triplets))

        return tmp_result
      }

      if(newDist.isEmpty){

        out_put_log("newDist is Empty!")
        val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, true, dist.head._2._3, dist.head._2._4, false, superstep, dist.head._2._7, dist.head._2._8))
        return tmp_result
      }

      //at the end of the loop, these two variable contain the lastest match set of their relatives
      if(bool_debug) {
        out_put_log("dist.head._2._4: ", dist.head._2._4)
        out_put_log("dist.head._2._7: ", dist.head._2._7)
      }

      val tmp_child_parent_map = extract_map_information(newDist, id,dist.head._2._4,dist.head._2._7,dist.head._2._8)

      if(bool_debug) {
//        out_put_log("children_match_set_map: "+tmp_child_parent_map._1.keySet.toArray.mkString(" , "))
//        out_put_log("parents_match_set_map: "+tmp_child_parent_map._2.keySet.toArray.mkString(" , "))
      }

      val tmp_child_parent_array = extract_array_from_map(tmp_child_parent_map._1, tmp_child_parent_map._2)

      if(bool_debug) {
        out_put_log("match_set_from_children: "+tmp_child_parent_array._1.mkString(" , "))
        out_put_log("match_set_from_parents: "+tmp_child_parent_array._2.mkString(" , "))
      }


      var match_array: Array[Long] = dist.head._2._3
      var tmp_change = false
      var tmp_valided = true


      for(x<-dist.head._2._3){

//        val tmp_labels : Map[String,Int] = calculate_num(QueryTripletsWhole)
//
//        val tmp_tri = tmp_child_parent_map._3.map{tri => if(tri._1._2 == true && tri._2._2 == true)
//          ((tri._1._1,tri._1._2),(tri._2._1,tri._2._2))}
//
//        val tmp_labels_data : Map[String,Int] = calculate_num(tmp_tri)
//
//        for(con <- tmp_labels_data){
//          if( tmp_labels(con._1) > con._2  ){
//
//            match_array = remove(x,match_array)
//            tmp_change = true
//          }
//
//        }

      }

      if(match_array.length<= 0 ){
        out_put_log("nodes tmp_set.length <=0    id: " + id)
        //        update_data_array(id, false)
        tmp_valided = false
      }

      val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, tmp_valided, match_array, tmp_child_parent_map._1, tmp_change, superstep,tmp_child_parent_map._2,tmp_child_parent_map._3))

      out_put_log("vertex id:"+id+"   value:"+ tmp_result.head._2._2+"   change: " + tmp_result.head._2._5
        +"    match_set: "+tmp_result.head._2._3.mkString(" , "))
      return tmp_result
    }
  }


  private def calculate_num(triplet : Array[((Long,String),(Long,String))]):Map[String,Int] ={

    val tmp_labels : Map[String,Int] = Map()

    for(ele<-triplet){

//      if(ele._1._1 == x){
//        if(!tmp_labels.keySet.contains(ele._2._2)) {
//          tmp_labels += (ele._2_2 -> 1)
//        }
//        else {
//          val num = tmp_labels(ele._2._2)+1
//          tmp_labels += (ele._2_2 -> num)
//        }
//      }
//
//      if(ele._2._1 == x){
//        if(!tmp_labels.keySet.contains(ele._1._2)) {
//          tmp_labels += (ele._1_2 -> 1)
//        }
//        else {
//          val num = tmp_labels(ele._1._2)+1
//          tmp_labels += (ele._1_2 -> num)
//        }
//      }
    }

    return tmp_labels
  }



  private def vprog_test_shuffle(id:VertexId, dist: SPMap,  newDist:SPMap)  :SPMap={
    if (dist.head._2._6== 1){
      if(QueryLabelsVal.contains(dist.head._2._1)){
        return Map(id ->(dist.head._2._1, true, QueryMapLabels(dist.head._2._1), dist.head._2._4, true, 2, dist.head._2._7,dist.head._2._8))
      }
      else{
        return Map(id ->(dist.head._2._1, false, dist.head._2._3, dist.head._2._4, true, 2, dist.head._2._7, dist.head._2._8))
      }
    }
    else {

      val superstep = dist.head._2._6+1
      if(dist.head._2._2 == false || dist.head._2._3.length<=0){
        val tmp_array : Array[Long] = Array()
        val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
        val tmp_match_set : Map[Long,Array[Long]] = Map()
        return Map(dist.head._1 ->(dist.head._2._1, false, tmp_array, tmp_match_set, false,superstep, tmp_match_set, tmp_triplets))
      }

      if(newDist.isEmpty){
        return Map(dist.head._1 ->(dist.head._2._1, true, dist.head._2._3, dist.head._2._4, false, superstep, dist.head._2._7, dist.head._2._8))
      }

      val invalided = newDist.map{e => if(e._2._2 == false){ e._1 } }.toArray

      val valided = newDist.map{e =>  if(e._2._2 == false){  e._1 } }.toArray

      val tmp_struct = dist.head._2._8.map{s => if( invalided.contains(s._1._1 ) || invalided.contains(s._2._1 )){
        ((s._1._1, false, ""),(s._2._1, false, ""))} else{s} }

      val valided_new = newDist.filter{v => v._2._2 != false }

      val rece_map: Map[Long,Array[Long]] = valided_new.map{ x=> (x._1 -> x._2._3) }

      val children = rece_map.filter(m=>  dist.head._2._8.map{x => x._2._1}.contains(m._1))++ dist.head._2._4.filter{m =>(!invalided.contains(m._1 ) && !valided.contains(m._1 ))}

      val parents = rece_map.filter(m=>  dist.head._2._8.map{x => x._1._1}.contains(m._1))++ dist.head._2._7.filter{m =>(!invalided.contains(m._1 ) && !valided.contains(m._1 ))}

      val match_set_from_children : Array[Long] = children.map{x=> x._2}.flatten.toArray.distinct

      val match_set_from_parents : Array[Long] = parents.map{m => m._2}.flatten.toArray.distinct

      var tmp_change = false
      var tmp_valided = true


      var match_array :Array[Long] = Array()
      for(x <- dist.head._2._3){
        val  temp_from_children = query_contains_all_children(x, match_set_from_children)
        val  temp_from_parent = query_contains_all_parents(x,match_set_from_parents)

        if(   (temp_from_children && temp_from_parent) ){
          match_array :+= x
        }
      }



      if(match_array.length != dist.head._2._3.length){
        tmp_change = true
      }

      if(match_array.length<= 0 ){
        tmp_valided = false
      }

      return Map(dist.head._1 ->(dist.head._2._1, tmp_valided, match_array, children, tmp_change, superstep,parents,tmp_struct))
    }
  }


  private def vprog_test(id:VertexId, dist: SPMap,  newDist:SPMap)  :SPMap={
    if (dist.head._2._6== 1){
//      if(QueryLabelsVal.contains(dist.head._2._1)){


      val for_test :Array[Long]= Array(667 , 11 , 1 , 450 , 58 , 292 , 834 , 664 , 467 , 740 , 7 ,
        734 , 267 , 4 , 152 , 683 , 415 , 74 , 224 , 750 , 121 , 446 , 163)


    return Map(id ->(dist.head._2._1, true, for_test, dist.head._2._4, true, 2, dist.head._2._7,dist.head._2._8))
//      }
//      else{
//        return Map(id ->(dist.head._2._1, true, dist.head._2._3, dist.head._2._4, true, 2, dist.head._2._7, dist.head._2._8))
//      }
    }
    else {

      val superstep = dist.head._2._6+1
//      if(dist.head._2._2 == false || dist.head._2._3.length<=0){
//        val tmp_array : Array[Long] = Array()
//        val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
//        val tmp_match_set : Map[Long,Array[Long]] = Map()
//        return Map(dist.head._1 ->(dist.head._2._1, true, tmp_array, tmp_match_set, true,superstep, tmp_match_set, tmp_triplets))
//      }
//
//      if(newDist.isEmpty){
//        return Map(dist.head._1 ->(dist.head._2._1, true, dist.head._2._3, dist.head._2._4, true, superstep, dist.head._2._7, dist.head._2._8))
//      }

      val invalided = newDist.map{e => if(e._2._2 == false){ e._1 } }.toArray

      val valided = newDist.map{e =>  if(e._2._2 == false){  e._1 } }.toArray

      val tmp_struct = dist.head._2._8.map{s => if( invalided.contains(s._1._1 ) || invalided.contains(s._2._1 )){
          ((s._1._1, false, ""),(s._2._1, false, ""))} else{s} }

      val valided_new = newDist.filter{v => v._2._2 != false }

      val rece_map: Map[Long,Array[Long]] = valided_new.map{ x=> (x._1 -> x._2._3) }

      val children = rece_map.filter(m=>  dist.head._2._8.map{x => x._2._1}.contains(m._1))++ dist.head._2._4.filter{m =>(!invalided.contains(m._1 ) && !valided.contains(m._1 ))}

      val parents = rece_map.filter(m=>  dist.head._2._8.map{x => x._1._1}.contains(m._1))++ dist.head._2._7.filter{m =>(!invalided.contains(m._1 ) && !valided.contains(m._1 ))}

//      val match_set_from_children : Array[Long] = children.map{x=> x._2}.flatten.toArray.distinct
//
//
//      val match_set_from_parents : Array[Long] = parents.map{m => m._2}.flatten.toArray.distinct
//
//      var tmp_change = false
//      var tmp_valided = true
//
//      val match_array = dist.head._2._3.filter{ x=>
//        (query_contains_all_children(x, match_set_from_children) && query_contains_all_parents(x,match_set_from_parents))
//      }
//
//
//
//      if(match_array.length != dist.head._2._3.length){
//        tmp_change = true
//      }
//
//      if(match_array.length<= 0 ){
//        tmp_valided = false
//      }

      return Map(dist.head._1 ->(dist.head._2._1, true, dist.head._2._3, children, true, superstep,parents,tmp_struct))
    }
//    else{
//      return dist
//    }
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** apply dual simulation to the data graph and query graph according to the algorithm
    *  @param id : current vertex id
    *  @param dist: the properties of current vertex
    *  @param newDist: the message received from other vertices
    *  @return the computed properties of current vertex
    */
  private def vprog(id:VertexId, dist: SPMap,  newDist:SPMap)  :SPMap={

    if (dist.head._2._6== 1){

      if(QueryLabelsVal.contains(dist.head._2._1)){

//        val tmp_triplets = DataTri.value.filter{tri =>
//          tri._1._1 == id || tri._2._1 == id
//        }.map{v =>
//          ((v._1._1,true,v._1._2),(v._2._1,true,v._2._2))
//        }

              var tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
        //      val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = temp.map{v =>
        //        ((v._1._1,true,v._1._2),(v._2._1,true,v._2._2))
        //      }



//              for(x<-DataTri.value){
//                if(x._1._1 == id || x._2._1 == id){
//                  tmp_triplets=tmp_triplets:+((x._1._1,true,x._1._2),(x._2._1,true,x._2._2))
//                }
//              }
        //      val temp : Array[Long] = QueryMapLabels(dist.head._2._1)

        return Map(id ->(dist.head._2._1, true, QueryMapLabels(dist.head._2._1), dist.head._2._4, true, 2, dist.head._2._7,tmp_triplets))
//        return Map(id ->(dist.head._2._1, true, QueryMapLabels(dist.head._2._1), dist.head._2._4, true, 2, dist.head._2._7,dist.head._2._8))
      }
      else{
        return Map(id ->(dist.head._2._1, false, dist.head._2._3, dist.head._2._4, true, 2, dist.head._2._7, dist.head._2._8))
      }




    }
    else{
      if(bool_debug && for_test.contains(id) ) {
        out_put_log("start to calculate id : "+ id)
        out_put_log("dist.head._2._4: ", dist.head._2._4)
        out_put_log("dist.head._2._7: ", dist.head._2._7)
      }

      val superstep : Int = dist.head._2._6 +1

      //If the current vertex is false, vote to halt
      if(dist.head._2._2 == false || dist.head._2._3.length<=0){

        if(for_test.contains(id) && bool_debug){
            out_put_log("dist.head._2._2 == false  id: " + id)
        }

        //If the current vertex is invalided, reset itself, doesn't send any message
        val tmp_array : Array[Long] = Array()
        val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
        val tmp_match_set : Map[Long,Array[Long]] = Map()

//        update_data_array( id, false)

        val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, false, tmp_array, tmp_match_set, false,superstep, tmp_match_set, tmp_triplets))

        return tmp_result
      }

      if(newDist.isEmpty){

//        out_put_log("newDist is Empty!")
        val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, true, dist.head._2._3, dist.head._2._4, false, superstep, dist.head._2._7, dist.head._2._8))
        return tmp_result
      }

      //at the end of the loop, these two variable contain the lastest match set of their relatives


//      val tmp_child_parent_array = extract_infor(newDist, id,dist.head._2._4,dist.head._2._7,dist.head._2._8)

      val tmp_child_parent_map = extract_map_information(newDist, id,dist.head._2._4,dist.head._2._7,dist.head._2._8)

//      if(bool_debug && for_test.contains(id)) {
//        out_put_log("children_match_set_map: "+tmp_child_parent_map._1.keySet.toArray.mkString(" , "))
//        out_put_log("parents_match_set_map: "+tmp_child_parent_map._2.keySet.toArray.mkString(" , "))
//      }

      val tmp_child_parent_array = extract_array_from_map(tmp_child_parent_map._1, tmp_child_parent_map._2)

      if(bool_debug && for_test.contains(id)) {
        out_put_log("match_set_from_children: "+tmp_child_parent_array._1.mkString(" , "))
        out_put_log("match_set_from_parents: "+tmp_child_parent_array._2.mkString(" , "))
      }


      var match_array: Array[Long] = dist.head._2._3
      var tmp_change = false
      var tmp_valided = true

      for(x<-dist.head._2._3){

        val tmp = check_children_and_parents(id, x, tmp_child_parent_map._3)
        var temp_from_children: Boolean = true
        var temp_from_parent: Boolean = true

        if(tmp._1 == false){

//          out_put_log("tmp._1 == false,  remove   id: " + x)
          if(for_test.contains(id) && bool_debug){
            out_put_log("tmp._1 == false,  remove   id: " + x)
          }
          match_array = remove(x,match_array)
          tmp_change = true
        }
        else{

          if(tmp._2){
//            temp_from_children = check_nodes(x, tmp_child_parent_array._1, true, id)
            temp_from_children = query_contains_all_children(x, tmp_child_parent_array._1)
          }

          if(tmp._3){
//            temp_from_parent = check_nodes(x, tmp_child_parent_array._2, false, id)
            temp_from_parent = query_contains_all_parents(x,tmp_child_parent_array._2)
          }

          if(   !(temp_from_children && temp_from_parent) ){
            match_array = remove(x,match_array)
            tmp_change = true

            if(for_test.contains(id) && bool_debug){
              out_put_log("temp_from_parent  " + temp_from_parent)
              out_put_log("temp_from_children   " + temp_from_children)
              out_put_log("!(temp_from_children && temp_from_parent)  remove   id: " + x)
            }
          }

        }

      }

      if(match_array.length<= 0 ){
//        out_put_log("nodes tmp_set.length <=0    id: " + id)
//        update_data_array(id, false)

        if(for_test.contains(id) && bool_debug){
          out_put_log("nodes tmp_set.length <=0    id: " + id)
        }

        tmp_valided = false
      }

      val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, tmp_valided, match_array, tmp_child_parent_map._1, tmp_change, superstep,tmp_child_parent_map._2,tmp_child_parent_map._3))

      if(for_test.contains(id) && bool_debug){
        out_put_log("vertex id:"+id+"   value:"+ tmp_result.head._2._2+"   change: " + tmp_result.head._2._5
          +"    match_set: "+tmp_result.head._2._3.mkString(" , "))
      }


      return tmp_result
    }
  }

  private def vprog_sssp(id:VertexId, dist: SPMap,  newDist:SPMap)  :SPMap={
//    (id, dist, newDist) => math.min(dist, newDist)

    var min : Int = newDist.head._2._6

    for(ele <- newDist){
      if(min > ele._2._6){
        min = ele._2._6
      }
    }

    val superstep : Int = math.min(dist.head._2._6, min)

    val tmp_array : Array[Long] = Array()
    val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))]= Array()
    val tmp_match_set : Map[Long,Array[Long]] = Map()

    var tmp_result : SPMap = Map()

    if(dist.head._2._6 > min ){
      var tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, true, tmp_array, tmp_match_set, true,superstep, tmp_match_set, tmp_triplets))
    }
    else{
      var tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, true, tmp_array, tmp_match_set, false,superstep, tmp_match_set, tmp_triplets))
    }

    return tmp_result
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** apply dual simulation to the data graph and query graph according to the algorithm
    *  @param id : current vertex id
    *  @param dist: the properties of current vertex
    *  @param newDist: the message received from other vertices
    *  @return the computed properties of current vertex
    */
  private def vprog_original(id:VertexId, dist: SPMap,  newDist:SPMap)  :SPMap={

    if (dist.head._2._6== 1){
      return init_nodes(id, dist)
    }
    else{
      val superstep : Int = dist.head._2._6 +1

      //If the current vertex is false, vote to halt
      if(dist.head._2._2 == false || dist.head._2._3.length<=0){

//        out_put_log("dist.head._2._2 == false  id: " + id)

        //If the current vertex is invalided, reset itself, doesn't send any message
        val tmp_array : Array[Long] = Array()
        val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))]= Array()
        val tmp_match_set : Map[Long,Array[Long]] = Map()

        //        update_data_array( id, false)

        val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, false, tmp_array, tmp_match_set, false,superstep, tmp_match_set, tmp_triplets))

        return tmp_result
      }

      if(newDist.isEmpty){

//        out_put_log("newDist is Empty!")
        val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, true, dist.head._2._3, dist.head._2._4, false, superstep, dist.head._2._7, dist.head._2._8))
        return tmp_result
      }

      //at the end of the loop, these two variable contain the lastest match set of their relatives
      if(bool_debug) {
        out_put_log("dist.head._2._3: ", dist.head._2._3)
        out_put_log("dist.head._2._4: ", dist.head._2._4)
        out_put_log("dist.head._2._7: ", dist.head._2._7)
      }

      val tmp_child_parent_map = extract_map_information(newDist, id,dist.head._2._4,dist.head._2._7,dist.head._2._8)

      if(bool_debug) {
//        out_put_log("children_match_set_map: "+tmp_child_parent_map._1.keySet.toArray.mkString(" , "))
//        out_put_log("parents_match_set_map: "+tmp_child_parent_map._2.keySet.toArray.mkString(" , "))
      }

      val tmp_child_parent_array = extract_array_from_map(tmp_child_parent_map._1, tmp_child_parent_map._2)

      if(bool_debug) {
//        out_put_log("match_set_from_children: "+tmp_child_parent_array._1.mkString(" , "))
//        out_put_log("match_set_from_parents: "+tmp_child_parent_array._2.mkString(" , "))
      }


      var match_array: Array[Long] = dist.head._2._3
      var tmp_change = false
      var tmp_valided = true


      for(x<-dist.head._2._3){

        if(  !(query_contains_all_children(x, tmp_child_parent_array._1) && query_contains_all_parents(x,tmp_child_parent_array._2)) ){
          tmp_change = true
          match_array = remove(x,match_array)
        }
      }

      if(match_array.length<= 0 ){
//        out_put_log("nodes tmp_set.length <=0    id: " + id)
        tmp_valided = false
      }

      val tmp_result : SPMap = Map(dist.head._1 ->(dist.head._2._1, tmp_valided, match_array, tmp_child_parent_map._1, tmp_change, superstep,tmp_child_parent_map._2,tmp_child_parent_map._3))

//      out_put_log("vertex id:"+id+"   value:"+ tmp_result.head._2._2+"   change: " + tmp_result.head._2._5
//        +"    match_set: "+tmp_result.head._2._3.mkString(" , "))

      return tmp_result
    }
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** the vertex id of Query, its children all contains in array
    *  @param id : vertex id of Query
    *  @param array: children match set
    *  @return true:contains all children, otherwise flase
    */
  private def query_contains_all_children(id:VertexId, array : Array[VertexId]):Boolean={

    val temp: Array[Long] = QueryTripletsArray.filter(x => x._1 ==id).map(m => m._2)

    if(temp.isEmpty ){
      return true
    }

    if( array.isEmpty){
      return false
    }

    val result = temp.filter(x=> !array.contains(x))
    if(result.length > 0){
      return false
    }

//    if(!temp.isEmpty && !array.isEmpty ){
//      for(x<-temp){
//        if(!array.contains(x) ){
//          return false
//        }
//      }
//    }

    return true
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** the vertex id of Query, its parents all contains in array
    *  @param id : vertex id of Query
    *  @param array: parents match set
    *  @return true:contains all parents, otherwise flase
    */
  private def query_contains_all_parents(id:VertexId, array : Array[VertexId]):Boolean={

    val temp: Array[Long] = QueryTripletsArray.filter(x => x._2 ==id).map(m => m._1)

    if(temp.isEmpty ){
      return true
    }

    if( array.isEmpty){
      return false
    }


    val result = temp.filter(x=> !array.contains(x))
    if(result.length > 0){
      return false
    }

//    if(!temp.isEmpty && !array.isEmpty ){
//      for(x<-temp){
//        if(!array.contains(x)){
//          return false
//        }
//      }
//    }

    return true
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** apply dual simulation to the data graph and query graph according to the algorithm
    *  @param firstMsg : first message
    *  @param secondMsg: second message
    *  @return the merged message
    */
  private def mergeMsg(firstMsg :SPMap, secondMsg : SPMap):  SPMap ={

    return firstMsg++secondMsg
  }

  private def init_nodes_new( id:VertexId, dist:SPMap) :SPMap= {

    var newVertices: SPMap = Map()

//    if(QueryLabels.value.contains(dist.head._2._1)){
//
//      var tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()


//      for (p <- DataTripletsWhole) {
//        val idx = p.index
//        val partRdd = rddData.mapPartitionsWithIndex((a,b) => if (a._1 == idx) a._2 else Iterator(), true)
//        //The second argument is true to avoid rdd reshuffling
//        val data = partRdd.collect //data contains all values from a single partition
//        //in the form of array
//        //Now you can do with the data whatever you want: iterate, save to a file, etc.
//      }

//
//      for(x<-DataTripletsWhole.value){
//        if(x._1._1 == id || x._2._1 == id){
//          tmp_triplets=tmp_triplets:+((x._1._1,true,x._1._2),(x._2._1,true,x._2._2))
//        }
//      }

//      val map1 : SPMap = Map(id ->(dist.head._2._1, true, QueryMapLabels(dist.head._2._1), dist.head._2._4, true, 2, dist.head._2._7,tmp_triplets))
//
//      newVertices = map1
//    }
//    else{
//      val map1 : SPMap = Map(id ->(dist.head._2._1, false, dist.head._2._3, dist.head._2._4, true, 2, dist.head._2._7, dist.head._2._8))
//
//      newVertices = map1
//    }

    return newVertices
  }


//  private var bInit : Boolean  = false

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** initialize the vertex
    *  @param id : current vertex id
    *  @param dist: current properties
    *  @return the computed properties
    */
  private def init_nodes( id:VertexId, dist:SPMap) :SPMap= {

//    var newVertices: SPMap = Map()

    if(QueryLabelsVal.contains(dist.head._2._1)){


//      val tmp_triplets = DataTripletsWhole.filter{tri =>
//        tri._1._1 == id || tri._2._1 == id
//      }.map{v =>
//        ((v._1._1,true,v._1._2),(v._2._1,true,v._2._2))
//      }

//      var tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
//      val tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = temp.map{v =>
//        ((v._1._1,true,v._1._2),(v._2._1,true,v._2._2))
//      }



//      for(x<-DataTripletsWhole){
//        if(x._1._1 == id || x._2._1 == id){
//          tmp_triplets=tmp_triplets:+((x._1._1,true,x._1._2),(x._2._1,true,x._2._2))
//        }
//      }
//      val temp : Array[Long] = QueryMapLabels(dist.head._2._1)

//      return Map(id ->(dist.head._2._1, true, QueryMapLabels(dist.head._2._1), dist.head._2._4, true, 2, dist.head._2._7,tmp_triplets))
      return Map(id ->(dist.head._2._1, true, QueryMapLabels(dist.head._2._1), dist.head._2._4, true, 2, dist.head._2._7,dist.head._2._8))

    }
    else{
      return Map(id ->(dist.head._2._1, false, dist.head._2._3, dist.head._2._4, true, 2, dist.head._2._7, dist.head._2._8))

    }

//    return newVertices
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** update the triplets of data graph which means updating invalided vertex to data graph
    *  @param id : current vertex id
    *  @param flag: flase indicates the current vertex is invalided
    */
  private def update_data_array( id:VertexId, flag:Boolean)={
//    println("DataTripletsConvert"+DataTripletsConvert.contains(true))
//
//    var data_array = DataTripletsConvert
//
//    for (ele <- 0 until data_array.length) {
//      if (data_array(ele)._1._1 == id){
//        data_array.update(ele, ((data_array(ele)._1._1, flag), (data_array(ele)._2._1, data_array(ele)._2._2)))
//      }
//
//      if(data_array(ele)._2._1 == id){
//        data_array.update(ele, ((data_array(ele)._1._1, data_array(ele)._1._2), (data_array(ele)._2._1, flag)))
//      }
//    }
//    DataTripletsConvert = data_array
  }

  private def remove_edge_from_data(srcId:VertexId, dstId:VertexId)={

//    var data_array = DataTripletsConvert
//    for (ele <- 0 until data_array.length) {
//      if (data_array(ele)._1._1 == srcId && data_array(ele)._2._1 == dstId){
//        data_array.update(ele, ((data_array(ele)._1._1, false), (data_array(ele)._2._1, false)))
//      }
//    }
//    DataTripletsConvert = data_array
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** update the triplets of data graph which means updating invalided vertex to data graph
    *  @param struct : the triplet table of current vertex
    *  @param checkId: the vertex need to be check
    *  @return  true: if the checkId is child of srcId
    *  @return  true: if the checkId is parent of srcId
    */
  private def check_relative(struct:Array[((Long,Boolean,String), (Long,Boolean,String))], checkId:Long):(Boolean,Boolean)={
    var b_children = false
    var b_parent = false

    val loop = new Breaks;

    loop.breakable {
      for(x<-struct){
        if(x._2._1 == checkId){
          b_children = true
          loop.break()
        }
      }
    }

    loop.breakable {
      for(x<-struct){
        if(x._1._1 == checkId){
          b_parent = true
          loop.break()
        }
      }
    }

    return (b_children,b_parent)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** check the matched vertex is resonable or not
    *  @param id : current vertex id
    *  @param matched_vertex: the vertex which matched in query
    *  @param struct : the triplet table of current vertex id
    *  @return  flase: If the node(id) doesn't have children but matched node in query has, or if the node(id) doesn't have parent but matched node in query has
    *  @return  true: this vertex in query has children
    *  @return  true: this vertex in query has parents
    */
  private def check_children_and_parents(id:VertexId, matched_vertex : VertexId,struct:Array[((Long,Boolean,String), (Long,Boolean,String))])
  :(Boolean, Boolean, Boolean) ={

    var b_check_child : Boolean = false

    var b_check_parent : Boolean = false

    var bool_valied:Boolean = true

    val loop = new Breaks;

    loop.breakable {
      for(x <- QueryTripletsArray){
        if(x._1 == matched_vertex){
          b_check_child = true
        }

        if(x._2 == matched_vertex){
          b_check_parent = true
        }

        if(b_check_child && b_check_parent){
          loop.break()
        }

      }


    }



//    out_put_log("id: "+id+"    == matched_vertex:"+  matched_vertex)
//
//    out_put_log("children: "+ b_check_child)
//    out_put_log("parent: "+ b_check_parent)


    //If the current vertex has children, this array contains true
    val b_check_data_child = struct.map { case (tri) =>  if(tri._1._1 == id && tri._2._2 == true) true  else false  }

    //If the current vertex has parent, this array contains true
    val b_check_data_parent = struct.map { case (tri) =>  if(tri._2._1 == id  &&  tri._1._2 == true) true  else false  }

//    out_put_log("b_check_data_child: "+ b_check_data_child.contains(true))
//    out_put_log("b_check_data_parent: "+ b_check_data_parent.contains(true))

    //If this vertex has no parents and children, vote to halt
    if( !b_check_data_child.contains(true) && !b_check_data_parent.contains(true) ){
//      out_put_log("vertex has no relatives, remove id:"+id)
      return (false, b_check_child, b_check_parent)
    }

    //the remove criterion is the current vertex has no relatives but matched vertex in Query graph has
    if( !b_check_data_child.contains(true) && b_check_child ){
      bool_valied = false
    }

    if(!b_check_data_parent.contains(true) && b_check_parent){
      bool_valied = false
    }

    return (bool_valied, b_check_child, b_check_parent)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** check the matched vertex is resonable or not
    *  @param relative : true:the input message from children; false:the input message from parents
    *  @param vertexId: current vertex id
    *  @param input_message : match set
    *  @return  if the array contains true, no need to remove any elements in match_set
    */
  private def nodes_in_query(relative : Boolean, vertexId : Long, input_message: Array[Long]):Array[Boolean]={

    if(relative){
      val temp_match = QueryTripletsArray.map{  case(tri) =>
        if(tri._1 == vertexId){                    //Find the matched node in Query
          if (input_message.contains(tri._2)){    //Compare with the new match_child_set.
            true
          }
          else{false}
        }
        else{false}
      }

      temp_match
    }
    else{
      val temp_match = QueryTripletsArray.map{  case(tri) =>
        if(tri._2 == vertexId){                    //Find the matched node in Query
          if (input_message.contains(tri._1)){    //Compare with the new match_child_set.
            true
          }
          else{false}
        }
        else{false}
      }

      temp_match
    }
  }

  //return:
  //1 @Boolean: the vertex is invalided or not
  //2 @Array:  the match_set
  //3 @Boolean:  vertex send message or not in next round

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** check the matched vertex is resonable or not
    *  @param relative : true:the input message from children; false:the input message from parents
    *  @param vertexId: current vertex id
    *  @param input_message : match set
    *  @param matched_vertex : matched vertex
    *  @return  this matched vertex is reasonable or not
    */
  private def check_nodes(matched_vertex : Long, input_message: Array[Long], relative : Boolean, vertexId : Long)
  : Boolean ={

    var tmp_bool : Boolean = true

//    out_put_log("id: "+vertexId+"   matched_vertex: "+matched_vertex
//      +"   input_message: "+input_message.mkString(",")+"   relative: "+relative)

    if(input_message.length <=0){
      return tmp_bool
    }


    val temp_match = nodes_in_query(relative, matched_vertex, input_message )

    if(!temp_match.contains(true)){
      tmp_bool = false
    }

    tmp_bool
  }



  private def check_nodes_new(matched_vertex : Long, input_message: Map[Long,Array[Long]], relative : Boolean, vertexId : Long)
  : Boolean ={

    var tmp_bool : Boolean = true

//    if(input_message.length <=0){
//      return tmp_bool
//    }

//    val temp_match = nodes_in_query(relative, matched_vertex, input_message )
//
//    if(!temp_match.contains(true)){
//      tmp_bool = false
//    }





    tmp_bool
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** update the triplet table of vertex
    *  @param id : the vertex which need to be changed
    *  @param bool_valid: the flag of vertex which need to be changed
    *  @param struct : the triplet table of vertex
    *  @return  the new triplet table
    */
  private def update_struct(id:Long,bool_valid:Boolean, struct:Array[((Long,Boolean,String), (Long,Boolean,String))])
  :Array[((Long,Boolean,String), (Long,Boolean,String))]={

    var tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()

    for(x<-struct){
      if(x._1._1 == id || x._2._1 == id){
        tmp_triplets=tmp_triplets:+((x._1._1,bool_valid,x._1._3),(x._2._1,bool_valid,x._2._3))
      }
      else{
        tmp_triplets=tmp_triplets:+((x._1._1,x._1._2, x._1._3),(x._2._1,x._2._2, x._2._3))
      }
    }

    tmp_triplets
  }



  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** update the recorded list from parents and children
    *  @param newDist : the input message
    *  @param id: current vertex id
    *  @param children_map : current map for children
    *  @param parents_map : current map for children
    *  @param struct : the triplet table of vertex
    *  @return  the new map for children
    *  @return  the new map for parents
    *  @return  the triplet table
    */
  private def extract_map_information(newDist:SPMap, id:Long, children_map:Map[Long,Array[Long]],
                                      parents_map:Map[Long,Array[Long]], struct:Array[((Long,Boolean,String), (Long,Boolean,String))])
  :(Map[Long,Array[Long]],Map[Long,Array[Long]],Array[((Long,Boolean,String), (Long,Boolean,String))])={

    var children = children_map
    var parents = parents_map

    var tmp_struct = struct

    for(ele <- newDist){

      val tmp_relative = check_relative(struct, ele._1)

      //so far, didn't check self loop
      if(ele._1 != id  && !ele._2._1.isEmpty ){

        if(ele._2._2 == false){

          tmp_struct = update_struct(ele._1,false, tmp_struct)

          if(children.contains(ele._1) && tmp_relative._1){
            children =children.-(ele._1)
          }
          if(parents.contains(ele._1) && tmp_relative._2){
            parents =parents.-(ele._1)
          }

        }
        else{

          if(tmp_relative._1){
            if(children.contains(ele._1)){
              children.updated(ele._1,ele._2._3)
            }
            else{
              children+=(ele._1 -> ele._2._3)
            }
          }

          if(tmp_relative._2){
            if(parents.contains(ele._1)){
              parents.updated(ele._1,ele._2._3)
            }
            else{
              parents+=(ele._1 -> ele._2._3)
            }
          }

        }
      }


    }
    (children, parents, tmp_struct)
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** extract match set from map of its recorded
    *  @param children : current map for children
    *  @param parents : current map for children
    *  @return  the new array for children
    *  @return  the new array for parents
    */
  private def extract_array_from_map(children:Map[Long,Array[Long]], parents: Map[Long,Array[Long]]):(Array[Long],Array[Long])={
    var match_set_from_children : Array[Long] = Array()
    var match_set_from_parents : Array[Long] = Array()

    for(x<-children.values.toArray){
      match_set_from_children  = concat(match_set_from_children, x)
    }

    for(x<-parents.values.toArray){
      match_set_from_parents = concat(match_set_from_parents, x)
    }

    (match_set_from_children.distinct, match_set_from_parents.distinct)
  }


  private def extract_tri(): Array[(Long, Long)]={
    return query.triplets.map{ tri =>  ((tri.srcId), (tri.dstId))}.collect
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** extract map of labels for query graph
    *  @return  map of labels for query graph
    */
  private def extract_array() : Map[String,Array[Long]] = {

    var map : Map[String,Array[Long]] = Map()

    for(ele <- QueryLabelsVal)
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

//  private def get_data_triplets(id:VertexId):(Array[Boolean],Array[Boolean])={
//    //If the current vertex has children, this array contains true
//    val b_check_data_child = DataTripletsEx.map { case (tri) =>  if(tri._1 == id) true  else false  }
//
//    //If the current vertex has parent, this array contains true
//    val b_check_data_parent = DataTripletsEx.map { case (tri) =>  if(tri._2 == id  ) true  else false  }
//
//    return (b_check_data_child, b_check_data_parent)
//  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** this method works for the new vprog
    * @param  id current vertex id
    * @param  matched_vertex match set
    * @return  this vertex will sent message in the next round or not
    *  @return  computed match set
    */
  private def check_children_and_parents_array(id:VertexId, matched_vertex : Array[VertexId]):(Boolean, Array[VertexId]) ={

    var b_check_child : Array[(Long)] = Array()
    var b_check_parent : Array[(Long)] = Array()

    var change:Boolean = false

    var result_array : Array[(Long)] = matched_vertex
    var match_array : Array[(Long)] = matched_vertex

    for(x <- QueryTripletsArray){
      if( matched_vertex.contains(x._1)){
        b_check_child=b_check_child:+x._1
      }

      if(matched_vertex.contains(x._2)){
        b_check_parent = b_check_parent:+x._2
      }
    }

    b_check_child= b_check_child.distinct
    b_check_parent= b_check_parent.distinct
    out_put_log("id: "+id+"    == matched_vertex:"+  matched_vertex.mkString(" , "))

    out_put_log("children: "+ b_check_child.mkString(" , "))
    out_put_log("parent: "+ b_check_parent.mkString(" , "))


//    val b_check_data = get_data_triplets(id)
//
//    out_put_log("b_check_data._1: "+ b_check_data._1.contains(true))
//    out_put_log("b_check_data._2: "+ b_check_data._2.contains(true))
//
//
//    //If this vertex has no parents and children, vote to halt
//    if( !b_check_data._1.contains(true) && !b_check_data._2.contains(true) ){
//      out_put_log("vertex has no relatives, remove id:"+id)
//      val tmp : Array[(Long)] = Array()
//      return (true, tmp)
//    }
//
//    //the remove criterion is the current vertex has no relatives but matched vertex in Query graph has
//    if( !b_check_data._1.contains(true) && b_check_child.length>0 ){
//      for(x<-match_array){
//        if(b_check_child.contains(x) ) {
//          result_array=remove(x, result_array)
//          change = true
//        }
//      }
//    }
//    match_array = result_array
//    if(!b_check_data._2.contains(true) && b_check_parent.length>0 && result_array.length > 0){
//      for(x<-match_array){
//        if(b_check_parent.contains(x)) {
//          result_array=remove(x, result_array)
//          change = true
//        }
//      }
//    }

    return (change, result_array)
  }

  private def init_Vertices_tri(input_graph: Graph[SPMap,String])= {

//    val triWhole = input_graph.triplets.map{ tri =>  ((tri.srcId,tri.srcAttr.head._2._1), (tri.dstId,tri.dstAttr.head._2._1))}.collect
//
    var tmp_vertex: Array[(VertexId,SPMap)]= Array()
//
//    val data_ver = input_graph.vertices.collect

//    for(ver <- data_ver){
//
//      var tmp_triplets : Array[((Long,Boolean,String), (Long,Boolean,String))] = Array()
//
//      for(x<-triWhole){
//        if(x._1._1 == ver._1 || x._2._1 == ver._1){
//          tmp_triplets=tmp_triplets:+((x._1._1,true,x._1._2),(x._2._1,true,x._2._2))
//        }
//      }
//
//      val map1 : SPMap = Map(ver._1 ->(ver._2.head._2._1, ver._2.head._2._2, ver._2.head._2._3, ver._2.head._2._4, true, 2, ver._2.head._2._7,tmp_triplets))
//
//      tmp_vertex = tmp_vertex :+ (ver._1 ,map1)
//    }

//    val edges_map: RDD[Edge[String]] =input_graph.edges
//
//    val vertices_map:RDD[(VertexId,SPMap)]  =sc.parallelize(tmp_vertex)
//
//    val g = Graph(vertices_map, edges_map)
//
//    return g

  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** this method works for the new vprog
    * @param  match_set match set of current vertex
    * @param  input_message match set from neighbor
    * @param  relative true:message from children
    * @param  vertexId current vertex id
    * @param  input_id the if of received message
    * @param struct : the triplet table of vertex
    *  @return  thie vertex is valied or not
    *  @return  match set
    *  @return  vertex send message or not in next round
    */
  private def check_nodes_in_query(match_set : Array[Long], input_message: Array[Long], relative : Boolean,
                                   vertexId : Long, input_id:Long, struct:Array[((Long,Boolean), (Long,Boolean))])
  : ( Boolean, Array[Long],Boolean) ={

    var change = false
    var tmp_status:Array[Boolean] = Array()

    if(input_message.length <=0){
      return (true, match_set, false)
    }


    out_put_log("id: "+vertexId+"   matched_vertex: "+match_set.mkString(" , ")
      +"   input_message: "+input_message.mkString(",")+"   relative: "+relative)


    if(bool_debug) {
      for(x<-QueryTripletsArray){
        if(relative){
          if( match_set.contains(x._1)){
            out_put_log("node: "+x._1+"   in query from children: "+ x._2)
          }
        }
        else{
          if(match_set.contains(x._2)){
            out_put_log("node: "+x._2+"   in query from parents: "+ x._1 )
          }
        }
      }
    }


    if(relative){
      val tmp_send = struct.map{ case(tri)=>
        if(tri._1._1 == vertexId && tri._2._1 == input_id){
          if(tri._1._2 == true && tri._2._2 == true){
            true
          }else{
            false
          }
        }
      }
      if(!tmp_send.contains(true)){
        return (true, match_set, false)
      }
    }
    else{
      val tmp_send = struct.map{ case(tri)=>
        if(tri._1._1 == input_id && tri._2._1 ==vertexId ){
          if(tri._1._2 == true && tri._2._2 == true){
            true
          }else{
            false
          }
        }
      }
      if(!tmp_send.contains(true)){
        return (true, match_set, false)
      }
    }

    for(node <- match_set){             // Iterate each node in match_set
    val temp_match = nodes_in_query(relative, node, input_message )

      if(!temp_match.contains(true)){
        tmp_status = tmp_status:+true
      }
      else{
        tmp_status = tmp_status:+false
      }
    }


    if(!tmp_status.contains(false)){
      out_put_log("input_id: "+ input_id)
      if(relative){
        remove_edge_from_data(vertexId, input_id)
      }
      else{
        remove_edge_from_data(input_id, vertexId)
      }
      change = true
    }

    val tmp = check_children_and_parents_array(vertexId, match_set)

    if(tmp._2.length <= 0){
      out_put_log("check_nodes_in_query tmp._2.length <=0    id: " + vertexId)
      update_data_array(vertexId, false)
      return (false, tmp._2, true)
    }
    else{
      return (true, tmp._2, change||tmp._1)
    }

  }

}



object Test_Dual_local{

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

    val sc = new SparkContext(conf)
//    val str:String = "/home/eric/1_BigData/1_graph_pattern_matching/src/GraphX/Pattern_Matching/dataset/test_6_2.txt"
//    val str:String = "/home/eric/1_BigData/1_graph_pattern_matching/src/GraphX/Pattern_Matching/dataset/Slashdot0811.txt"
//      val str:String = "/home/eric/SparkBench/Wiki-Vote_csv.csv"
//    val str:String = "/home/eric/SparkBench/test_6_2.csv"
    val str:String = "/home/eric/SparkBench/s14.csv"
//    var name : String = "Log-"+out_put_time + ".txt"
//    var pw : PrintWriter = new PrintWriter(name)
//    CommonUse.init(pw)

//    println("Mem(M) : "+ (Runtime.getRuntime().maxMemory()/1048576 ))




    val data = GraphConstruct.load_data_graph(str, 200,sc)
    val query = GraphConstruct.extract_graph(data, 100,sc, 1000)

    {
      println("new")
      val tmp = new DualSimulation(data, query._1,sc, query._3)
      val temp2 = tmp.apply_dual_simulation(1,1)
    }

//    {
//      println("original")
//      val tmp = new DualSimulation(data, query._1,sc)
//      val temp2 = tmp.apply_dual_simulation(0,1)
//    }

//    val children_match_set : Map[Long,Array[Long]] = Map()
//    val parents_match_set : Map[Long,Array[Long]] = Map()
//
//    val match_set_test:Array[Long] = Array()
//    val match_parent_set_test:Array[Long] = Array()
//    val tmp_triplets : Array[((Long,Boolean), (Long,Boolean))] = Array()
//
//    val tmp1 : SPMap = Map(1L ->("A", false, match_set_test, children_match_set,false, 1 , parents_match_set,tmp_triplets))
//    val tmp2 : SPMap = Map(2L->("B", false, match_set_test, children_match_set,false, 1 , parents_match_set,tmp_triplets))
//    val tmp3 : SPMap = Map(3L->("A", false, match_set_test, children_match_set,false, 1 , parents_match_set,tmp_triplets))
//    val tmp4 : SPMap = Map(4L->("B", false, match_set_test, children_match_set,false,1 , parents_match_set,tmp_triplets))
//    val tmp5 : SPMap = Map(5L ->("C", false, match_set_test, children_match_set,false,1 , parents_match_set,tmp_triplets))
//    val vertices_map_Data_t7: RDD[(VertexId,SPMap)] =
//      sc.parallelize(Array((1L, tmp1),      (2L,tmp2),      (3L,tmp3),      (4L,tmp4),      (5L,tmp5)))
//
//    var edges_map_Data_t7: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, ""),
//      Edge(2, 3, ""), Edge(3, 4, ""), Edge(4, 5, "")))
//
//    val vertices_map_Query_t1: RDD[(VertexId, String)] =
//      sc.parallelize(Array((1L, "A"), (2L,"B")))
//
//    // Create an RDD for edges
//    val edges_map_Query_t1: RDD[Edge[String]] =sc.parallelize(Array(Edge(1, 2, ""),    Edge(2, 1, "")))
//
//
//
//    val data = GraphConstruct.build_DataGraph(vertices_map_Data_t7,edges_map_Data_t7)
//    val query = GraphConstruct.build_QueryGraph(vertices_map_Query_t1,edges_map_Query_t1)
//    val tmp = new DualSimulation(data, query)



  }

}

object Test_Dual_remote{

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

    println("------------------------------------------------------")
    println("                    Version:1.07")
    println("------------------------------------------------------")

    val conf = new SparkConf()
      .setAppName("GraphPatternMatching")

    val sc = new SparkContext(conf)

//    val data = GraphConstruct.load_data_graph(args(0), args(1).toInt,sc)
//    val query = GraphConstruct.extract_graph(data, args(2).toInt)
//
//    val tmp = new DualSimulation(data, query)
//
//    val tmp2 = tmp.apply_dual_simulation()
  }

}