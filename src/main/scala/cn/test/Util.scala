package cn.test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

object Util {
  def getBusinessarea(line:String):String = {
    val jSONObject: JSONObject = JSON.parseObject(line)

    val status: Int = jSONObject.getIntValue("status")
    if(status==0) return ""

    val jSONObject1: JSONObject = jSONObject.getJSONObject("regeocode")
    if(jSONObject1==null) return ""

    val jSONArray: JSONArray = jSONObject1.getJSONArray("pois")

    val list: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    for(item <- jSONArray.toArray()) {
      if(item.isInstanceOf[JSONObject]) {
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val businessarea: String = json.getString("businessarea")
        list.append(businessarea)
      }
    }
    list.mkString(",")
  }

  def getType(line:String):String = {
    val jSONObject: JSONObject = JSON.parseObject(line)

    val status: Int = jSONObject.getIntValue("status")
    if(status==0) return ""

    val jSONObject1: JSONObject = jSONObject.getJSONObject("regeocode")
    if(jSONObject1==null) return ""

    val jSONArray: JSONArray = jSONObject1.getJSONArray("pois")

    val list: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    for(item <- jSONArray.toArray()) {
      if(item.isInstanceOf[JSONObject]) {
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val ty: String = json.getString("type")
        list.append(ty)
      }
    }
    list.mkString(",")
  }
}
