package gr.ml.analytics.service

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.service.cf.PredictionService

object HybridService extends App{
  // TODO add up rating from both files (if movie exists in both files) or (OPTIONAL!) double it if exists only in one of them

  val userId = 1

  val collaborativeReader = CSVReader.open(String.format(PredictionService.collaborativePredictionsForUserPath, userId.toString))
  val collaborativePredictions = collaborativeReader.all()
  collaborativeReader.close()
  val contentBasedReader = CSVReader.open(String.format(PredictionService.contentBasedPredictionsForUserPath, userId.toString))
  val contentBasedPredictions = contentBasedReader.all()
  contentBasedReader.close()
  val allPredictions= collaborativePredictions ++ contentBasedPredictions

  val list: List[List[Int]] = List(List(0,1), List(0,2), List(1,4))    // ==> List(List(0,3), List(1,4))
  val reducedMap: Map[Int,List[List[Int]]] = Map(0 -> List(List(0,1), List(0,2)), 1 -> List(List(1,4)))
  val map: Map[Int,List[Int]] = Map(0 -> List(1,2), 1 -> List(4))

//  list.groupBy()

    reducedMap.map{case (i:Int,list:List[List[Int]])=>(i, list)}
    reducedMap.map(t => (t._1, 3))

  val m = Map("a" -> 1, "b" -> 2)

//  val incM = m.map((key:String, value:Int) => (key, value + 1))
  val incM = m.map{case (key, value) => (key, value + 1)}
  //  map.reduce((l1,l2)=>{val l3 = l1; l3._2._1 (2)+=l2(2)})

  // TODO if I need to avoid user ID here, that's possible
  val initialList = List(List("1","1683", "11.699"), List("1","1683", "2.2"), List("1","50", "2.2"))
  val testGroupedList = initialList.groupBy(l=>l(1)) // TODO refactor to take by itemId
  val groupedList = List(List("1","1683", "11.699"), List("1","1683", "2.2"))
  val reducedList = groupedList.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2).toDouble+l2(2).toDouble).toString))

  val out = testGroupedList.map(t=>(t._1, t._2.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2).toDouble+l2(2).toDouble).toString))))



  val hybridPredictions = allPredictions.filter(l=>l(0)!="userId").groupBy(l=>l(1))
    .map(t=>(t._1, t._2.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2).toDouble+l2(2).toDouble).toString))))
    .map(t=>t._2)

  //val hybridPredictionsTyped: List[(Int, Int, Double)] = hybridPredictions.toList

  println(23)


}
