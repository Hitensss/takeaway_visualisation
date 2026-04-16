package cleaning
import scala.util.Try

object FieldParser {

  //字段解析器
  // 解析月售："月售1000+" -> 1000
  def parseMonthlySales(s: String): Int = {
    if (s == null || s.isEmpty) return 0
    val digits = s.replaceAll("[^0-9]", "")
    if (digits.isEmpty) 0 else digits.toInt
  }

  // 解析距离："420m" -> 420, "4.5km" -> 4500
  def parseDistance(s: String): Int = {
    if (s == null || s.isEmpty) return 0
    val cleaned = s.trim.toLowerCase
    if (cleaned.contains("km")) {
      val num = cleaned.replace("km", "").toDouble
      (num * 1000).toInt
    } else {
      cleaned.replace("m", "").toInt
    }
  }

  // 解析人均："人均 ¥22" -> 22.0
  def parseAvgPrice(s: String): Double = {
    if (s == null || s.isEmpty) return 0.0
    val digits = s.replaceAll("[^0-9.]", "")
    if (digits.isEmpty) 0.0 else digits.toDouble
  }

  // 解析评分
  def parseRating(s: String): Double = {
    if (s == null || s.isEmpty) return 0.0
    s.toDouble
  }

  // 解析送达时间：支持 "39分钟", "1.5小时", "45分", "2时", "30" 等多种格式
  def parseDeliveryTime(s: String): Int = {
    if (s == null || s.isEmpty) return 0

    Try {
      val cleaned = s.trim

      // 提取数字（支持小数）
      val numPattern = """(\d+(?:\.\d+)?)""".r
      val num = numPattern.findFirstMatchIn(cleaned).map(_.group(1).toDouble).getOrElse(0.0)

      // 判断单位
      if (cleaned.contains("小时") || cleaned.contains("时")) {
        (num * 60).toInt  // 小时转分钟
      } else {
        num.toInt  // 分钟或默认
      }
    }.getOrElse(0)
  }

  // 解析配送费
  def parseDeliveryFee(s: String): Double = {
    if (s == null || s.isEmpty) return 0.0
    if (s.contains("免") || s.contains("免费")) return 0.0
    val digits = s.replaceAll("[^0-9.]", "")
    if (digits.isEmpty) 0.0 else digits.toDouble
  }

  // 解析起送价
  def parseMinPrice(s: String): Double = {
    if (s == null || s.isEmpty) return 0.0
    val digits = s.replaceAll("[^0-9.]", "")
    if (digits.isEmpty) 0.0 else digits.toDouble
  }
}