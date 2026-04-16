package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.MySQLUtils

object DistanceStats {

  def main(args: Array[String]): Unit = {
    // 1. 创建SparkSession
    val spark = SparkSession.builder()
      .appName("DistanceStatsAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 2. 读取并清洗数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 3. 添加距离分组
      val dfWithGroup = cleanedDF.withColumn("distance_group",
        when(col("distance") < 300, "0-300m")
          .when(col("distance") < 800, "300-800m")
          .when(col("distance") < 1500, "800-1500m")
          .otherwise("1500m+")
      )

      // 4. 聚合统计
      val result = dfWithGroup.groupBy("distance_group")
        .agg(
          count("shop_name").alias("shop_count"),
          round(avg("monthly_sales"), 2).alias("avg_sales"),
          round(avg("avg_price"), 2).alias("avg_price"),
          round(avg("rating"), 2).alias("avg_rating")
        )
        .orderBy(
          when(col("distance_group") === "0-300m", 1)
            .when(col("distance_group") === "300-800m", 2)
            .when(col("distance_group") === "800-1500m", 3)
            .otherwise(4)
        )

      // 5. 显示结果
      println("========== 距离区间统计结果 ==========")
      result.show()

      // 6. 写入MySQL
      result.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "distance_stats", MySQLUtils.getProperties())

      println(" 距离区间统计已写入 distance_stats 表")

    } finally {
      // 7. 关闭SparkSession
      spark.stop()
    }
  }
}