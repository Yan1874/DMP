package cn.util

import scala.collection.mutable

object mapUtil {
  def mergeMap(map1:mutable.HashMap[String,Int], map2:mutable.HashMap[String,Int]) ={
    val iterator: Iterator[String] = map2.keysIterator
    while(iterator.hasNext) {
      val key: String = iterator.next()
      if(map1.contains(key)) {
        map1.put(key, map1.get(key).get + map2.get(key).get)
      }else{
        map1.put(key,map2.get(key).get)
      }
    }
    map1
  }

  def main(args: Array[String]): Unit = {
    val map1 = new mutable.HashMap[String,Int]()
    val map2 = new mutable.HashMap[String,Int]()

    map1.put("a",1)
    map1.put("b",1)
    map2.put("a",1)

    println(mergeMap(map1,map2))
  }
}
