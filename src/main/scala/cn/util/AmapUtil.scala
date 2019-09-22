package cn.util

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object AmapUtil {

  def getBusiness(long: String, lat: String) = {
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(String2Type.toDouble(lat), String2Type.toDouble(lat), 5)

    var business = redis_getBusiness(geoHash)

    if (business == null) {
     business = getBusinessFromGaoDe(long, lat)
      if(business!=null && business.length>0) {
        redis_insertBusiness(geoHash,business)
      }
    }
    business
  }

  /**
    * 从高德开发者平台逆地理解析商圈信息
    *
    * @param long
    * @param lat
    */

  def getBusinessFromGaoDe(long: String, lat: String):String = {
    val url = "https://restapi.amap.com/v3/geocode/regeo?location=" + long + "," + lat + "&key=d277f98c4db4c42366fa0ee82347b032&extensions=all"

    val jsonStr: String = HttpUtil.get(url)

    val jSONObject = JSON.parseObject(jsonStr)

    val status: Int = jSONObject.getIntValue("status")
    if(status == 0) return ""
    val jSONObject1 = jSONObject.getJSONObject("regeocode")
    if(jSONObject1 == null) return ""
    val jSONObject2 = jSONObject1.getJSONObject("addressComponent")
    if(jSONObject2 == null) return ""
    val jSONArray: JSONArray = jSONObject2.getJSONArray("businessAreas")
    if(jSONArray ==null) return ""

    val list: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    for(item <- jSONArray.toArray()) {
      if(item.isInstanceOf[JSONObject]) {
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val name: String = json.getString("name")
        list.append(name)
      }
    }
    list.mkString(",")
  }

  /**
    * 从redis获取缓存的商圈信息
    * @param str
    * @return
    */
  def redis_getBusiness(str: String) = {
    val jedis: Jedis = JedisConnectionPool.getJedis()
    val business: String = jedis.get(str)
    jedis.close()
    business
  }

  def redis_insertBusiness(geoHash: String, business: String) = {
    val jedis: Jedis = JedisConnectionPool.getJedis()
    jedis.set(geoHash,business)
    jedis.close()
  }




}
