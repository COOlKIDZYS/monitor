package cn.sheep.utils

import java.text.SimpleDateFormat

object Utils {
  def calculateRqt(startTime:String,endTime:String):Long= {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSSS")
    val st = dateFormat.parse(startTime.substring(0, 17)).getTime
    val et=dateFormat.parse(endTime).getTime
    et-st
  }

}
