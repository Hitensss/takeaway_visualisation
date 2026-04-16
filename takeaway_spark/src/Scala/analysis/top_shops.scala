package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.MySQLUtils

//Top10店铺
object TopShops {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TopShopsAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)

      // 取月售前10的店铺
      val result = cleanedDF
        .orderBy(desc("monthly_sales"))
        .select(
          col("shop_name"),
          col("category_clean").alias("category"),
          col("monthly_sales"),
          col("avg_price"),
          col("rating"),
          col("distance")
        )
        .limit(10)

      println("========== Top10店铺结果 ==========")
      result.show()

      // 写入MySQL
      result.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "top_shops", MySQLUtils.getProperties())

      println(" Top10店铺已写入 top_shops 表")

    } finally {
      spark.stop()
    }
  }
}