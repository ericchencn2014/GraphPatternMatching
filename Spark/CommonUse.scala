package GraphX

import java.io.{FileWriter, PrintWriter, File}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph

import scala.compat.Platform.currentTime


/**
 * Created by eric on 17/08/15.
 */
object CommonUse  extends java.io.Serializable{

  /** set for output log information or not
    */
  val bool_ouput = true

  /** set for output debug information or not
    */
  val bool_debug= false

  /** current if start to record to local file it my cause unserializable
    */
  val  bool_write= true

//  var pw : PrintWriter = null

//  val name : String = "Log@"+out_put_time + ".txt"
//  val name : String = "///root/Log-"+out_put_time + ".txt"
//  val pw : PrintWriter = new PrintWriter(new File(name))

//  val file  = new File(name)
//  val file  = new File(name)
//  val fw = new FileWriter(file,true)

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** output the current time
    */
  def out_put_time(): String ={
    val today = Calendar.getInstance().getTime()

    val dayFormat = new SimpleDateFormat("dd")
    val hourFormat = new SimpleDateFormat("HH")
    val minuteFormat = new SimpleDateFormat("mm")
    val sFormat = new SimpleDateFormat("ss")


    val currentday = dayFormat.format(today)
    val currenthour = hourFormat.format(today)
    val currentMinute = minuteFormat.format(today)
    val currentSecond = sFormat.format(today)

    return currentday+"-"+currenthour+":"+currentMinute+":"+currentSecond
  }

  def get_current_second():Double={
//    val today = Calendar.getInstance().getTime()
//    val hourFormat = new SimpleDateFormat("hh")
//    val minuteFormat = new SimpleDateFormat("mm")
//    val sFormat = new SimpleDateFormat("ss")
//    val currenthour = hourFormat.format(today)
//    val currentMinute = minuteFormat.format(today)
//    val currentSecond = sFormat.format(today)

//    val start_time:Double = currenthour.toDouble*3600+currentMinute.toDouble*60+currentSecond.toDouble
    return currentTime/1000

  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** extract same elements from two given array
    *  @param  first  array
    *  @param  second  array
    *  @return  the array which contains same elements
    */
  def extract_same_from_array(first:Array[Long], second:Array[Long]):Array[Long]={
    var result_array : Array[Long] = Array()
    for(x<-first){
      if(second.contains(x)){
        result_array = result_array:+x
      }
    }
    result_array
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** remove an element from the array
    *  @param  element  element
    *  @param  dest  array
    *  @return  the new array
    */
  def remove(element:Long, dest:Array[Long]):Array[Long]={
    val tmp:Array[Long] = Array(element)
    dest.diff(tmp)
  }


  def out_put_log(str:String): Unit ={
    if (bool_ouput == true){
      println(str)
    }
//    if(bool_write){
//      val fw = new FileWriter(file,true)
//      try {
//        fw.append(str)
//        fw.append("\n")
//      }
//      finally fw.close()
//    }
  }

  def out_put_log(name:String,str:String): Unit ={
    if (bool_ouput == true){
      println(name)
      println(str)
    }
//    if(bool_write){
//      pw.write(name)
//      pw.write("\n")
//      pw.write(str)
//      pw.write("\n")
//    }
  }

  def out_put_log(name:String, map:Map[_,Array[_]]): Unit={
    if(bool_ouput == true){
      println(name)
      map.mkString("\n")
      map.map{ case(key, value) => println(" key:"+ key+ "   array:"+ value.mkString(" , "))}
    }
//    if(bool_write){
//      pw.write(name)
//      pw.write("\n")
//      map.map{ case(key, value) => pw.write(" key:"+ key+ "   array:"+ value.mkString(" , ")+"\n")}
//      pw.write("\n")
//    }
  }

  def out_put_log(name:String, arr:Array[_]): Unit={
    if(bool_ouput == true){
      println(name)
      println(arr.mkString("  ,  "))
    }
//    if(bool_write){
//      pw.write(name)
//      pw.write("\n")
//      pw.write(arr.mkString("  ,  "))
//      pw.write("\n")
//    }
  }


  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** extract map of labels for query graph
    *  @param  result computed array
    *  @param  given_array given array
    *  @return  the array which contains additional vertices
    */
  def check_result( result : Array[Long], given_array : Array[Long])={

    val tmp1 = given_array.filter(x=> !result.contains(x)).distinct

    val tmp2 = result.filter(x=> !given_array.contains(x)).distinct

    if(tmp2.length == 0 && tmp1.length == 0){
      out_put_log("perfect result:")
    }
    else{
      if(tmp1.length > 0){
        out_put_log("miss vertex number:"+tmp1.length)
      }
      if(tmp2.length > 0){
        out_put_log("add more vertices number:"+tmp2.length)
      }
    }


//    var check : Array[Long] = Array()
//    for(x<-given_array){
//      if(!result.contains(x)){
//        out_put_log("miss vertex id:"+x)
//      }
//    }

//    for(x<-result){
//      if(!given_array.contains(x)){
//        check=check:+x
//      }
//    }
//    out_put_log("add more vertices number: "+check.length)
  }

  //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
  /** calculate sssp
    *  @param graph
    *  @return result graph
    */
  def shortest_path(graph: Graph[Double, Double]): Graph[Double,Double] = {

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
}


object Test_Comm{

  def main(args: Array[String]): Unit = {

//    val today = Calendar.getInstance().getTime()


    val t = currentTime

    println(t)
  }
}