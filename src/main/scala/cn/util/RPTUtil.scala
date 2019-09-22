package cn.util

object RPTUtil {
  def reqPt(requestmode: Int, processnode: Int) = {
    if (requestmode == 1 && processnode == 1) {
      (1, 0, 0)
    } else if (requestmode == 1 && processnode == 2) {
      (1, 1, 0)
    } else if (requestmode == 1 && processnode == 2) {
      (1, 1, 1)
    } else {
      (0, 0, 0)
    }
  }


  def showClick(requestmode: Int, iseffective: Int) = {
    if (requestmode == 2 && iseffective == 1) {
      (1, 0)
    } else if (requestmode == 3 && iseffective == 1) {
      (0, 1)
    } else {
      (0, 0)
    }
  }


  def adRTB(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
            adorderid: Int, winprice: Double, adpayment: Double) = {
    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0) {
        (1, 1, winprice / 1000.0, adpayment / 1000.0)
      } else {
        (1, 0, 0.0, 0.0)
      }
    } else {
      (0, 0, 0.0, 0.0)
    }
  }
}