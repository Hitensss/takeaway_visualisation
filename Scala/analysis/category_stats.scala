package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.MySQLUtils

//品类特征统计
object CategoryStats {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CategoryStatsAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)

      // 按品类聚合统计
      val result = cleanedDF.groupBy("category_clean")
        .agg(
          count("shop_name").alias("shop_count"),
          round(avg("monthly_sales"),2).alias("avg_sales"),
          round(avg("avg_price"),2).alias("avg_price"),
          round(avg("rating"),2).alias("avg_rating")
        )
        .withColumnRenamed("category_clean", "category")
        .orderBy(desc("avg_sales"))

      println("========== 品类特征统计结果 ==========")
      result.show()

      // 写入MySQL
      result.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_stats", MySQLUtils.getProperties())

      println(" 品类特征统计已写入 category_stats 表")

    } finally {
      spark.stop()
    }
  }
}
