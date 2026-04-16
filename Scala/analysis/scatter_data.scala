package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.MySQLUtils

//散点图数据
object ScatterData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ScatterDataAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)

      // 选择散点图需要的字段，过滤无效数据
      val result = cleanedDF
        .select(
          col("distance"),
          col("monthly_sales"),
          col("avg_price"),
          col("shop_name")
        )
        .filter(col("distance") > 0 && col("monthly_sales") > 0)

      println("========== 散点图数据结果 ==========")
      println(s"数据量: ${result.count()}")
      result.show(20)

      // 写入MySQL
      result.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "scatter_data", MySQLUtils.getProperties())

      println(" 散点图数据已写入 scatter_data 表")

    } finally {
      spark.stop()
    }
  }
}