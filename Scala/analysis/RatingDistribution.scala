package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RatingDistribution {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RatingDistributionAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 2. 创建评分区间（0-5分，每0.5分一个区间）
      val ratingDistribution = cleanedDF
        .withColumn("rating_bucket",
          when(col("rating") < 0.5, "0-0.5")
            .when(col("rating") < 1.0, "0.5-1.0")
            .when(col("rating") < 1.5, "1.0-1.5")
            .when(col("rating") < 2.0, "1.5-2.0")
            .when(col("rating") < 2.5, "2.0-2.5")
            .when(col("rating") < 3.0, "2.5-3.0")
            .when(col("rating") < 3.5, "3.0-3.5")
            .when(col("rating") < 4.0, "3.5-4.0")
            .when(col("rating") < 4.5, "4.0-4.5")
            .otherwise("4.5-5.0")
        )
        .groupBy("rating_bucket")
        .agg(count("shop_name").alias("shop_count"))
        .withColumn("percentage",
          round(col("shop_count") / sum("shop_count").over() * 100, 2))
        .orderBy("rating_bucket")

      // 3. 显示结果
      println("========== 评分分布结果 ==========")
      ratingDistribution.show()

      // 4. 写入MySQL
      ratingDistribution.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "rating_distribution", MySQLUtils.getProperties())

      println("评分分布已写入 rating_distribution 表")

      // 5. 输出统计摘要
      println("\n========== 评分统计摘要 ==========")
      cleanedDF.select(
        avg("rating").alias("平均评分"),
        min("rating").alias("最低评分"),
        max("rating").alias("最高评分"),
        stddev("rating").alias("评分标准差")
      ).show()

    } finally {
      spark.stop()
    }
  }
}