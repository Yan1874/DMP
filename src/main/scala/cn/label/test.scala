package cn.label

import cn.util.AmapUtil

object test {
  def main(args: Array[String]): Unit = {
    val str: String = AmapUtil.getBusiness("116.310003","39.991957")
  println(str)
  }
}
