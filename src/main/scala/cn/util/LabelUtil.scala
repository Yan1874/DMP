package cn.util

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

object LabelUtil {
  def businessLb(long: String, lat: String, map: mutable.HashMap[String, Int]) = {
    if(String2Type.toDouble(long)>=73
      && String2Type.toDouble(long)<=136
      && String2Type.toDouble(lat)>=3
      && String2Type.toDouble(long)<=53){
      val business: String = AmapUtil.getBusiness(long,lat)
      if(business!=null) {
        val arr: Array[String] = business.split(",")
        for(key <-arr) {
          map.put(key,1)
        }
      }
    }

  }

  def pricityLb(provincename: String, cityname: String, map: mutable.HashMap[String, Int]) = {
    if(provincename!=null) {
      val key = "ZP"+provincename
      map.put(key,1)
    }
    if(cityname!=null) {
      val key = "ZC" + cityname
      map.put(key,1)
    }
  }

  def keywordsLb(keywords: String, map: mutable.HashMap[String, Int]) = {
    if(keywords!=null) {
      val words: Array[String] = keywords.split("\\|",-1)
      for(word <- words) {
        if(word.length>=3 && word.length<=8) {
          val key = "K"+word
          map.put(key,1)
        }
      }
    }
  }

  def ispnameLb(ispname: String, map: mutable.HashMap[String, Int]) = {
    if(ispname!=null) {
      val key = ispname match {
        case "移动" => "D00030001"
        case "联通" => "D00030002"
        case "电信" => "D00030003"
        case _ => "D00030004"
      }
      map.put(key,1)
    }
  }

  def networkmannernameLb(networkmannername: String, map: mutable.HashMap[String, Int])= {
    if(networkmannername!=null) {
      val key = networkmannername match {
        case "Wifi" => "D00020001"
        case "4G" => "D00020002"
        case "3G" => "D00020003"
        case "2G" => "D00020004"
        case _ => "D00020005"
      }
      map.put(key,1)

    }
  }

  def clientLb(client: Int, map: mutable.HashMap[String, Int])= {
    if(client!=0) {
     val key = client match{
        case 1 => "D00010001"
        case 2 => "D00010002"
        case 3 => "D00010003"
        case _ => "D00010004"
     }
      map.put(key,1)
    }
  }

  def adplatformprovideridLb(adplatformproviderid: Int, map: mutable.HashMap[String, Int])= {
    if(adplatformproviderid!=0) {
      val key = "CN"+adplatformproviderid
      map.put(key,1)
    }
  }


  def adspacetypeLb(adspacetype:Int,map:mutable.HashMap[String,Int]) ={
    if(adspacetype!=0) {
      var key = ""
      if (adspacetype < 10) {
        key = "LC0" + adspacetype
      } else {
        key = "LC" + adspacetype
      }
      map.put(key,1)
    }
  }

  def adspacetypenameLb(adspacetypename:String,map:mutable.HashMap[String,Int]) = {
    if(adspacetypename!=null) {
      val key =  "LN" + adspacetypename
      map.put(key,1)
    }
  }

  def appnameLb(appid: String, appname: String, broadcastVar: Broadcast[Map[String, String]],
              map: mutable.HashMap[String, Int]) ={

    if(appname=="未知"||appname==null) {
      if(broadcastVar.value.contains(appid)) {
        val key = "APP" + broadcastVar.value.get(appid)
        map.put(key,1)
      }
    }else {
      val key = "APP"+appname
      map.put(key,1)
    }

  }





}
