package analysis
import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.MySQLUtils

object DistanceStats {

  /**
   * 供RunAllAnalysis调用的分析方法
   */
  def analyze(spark: SparkSession, cleanedDF: DataFrame): Unit = {
    println("开始执行距离区间统计...")

    // 添加距离分组
    val dfWithGroup = cleanedDF.withColumn("distance_group",
      when(col("distance") < 300, "0-300m")
        .when(col("distance") < 800, "300-800m")
        .when(col("distance") < 1500, "800-1500m")
        .otherwise("1500m+")
    )

    // 聚合统计
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

    // 显示结果
    println("========== 距离区间统计结果 ==========")
    result.show()

    // 写入MySQL
    result.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "distance_stats", MySQLUtils.getProperties())

    println("距离区间统计已写入 distance_stats 表")
  }

  /**
   * 独立运行时的main方法
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DistanceStatsAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      val cleanedDF = DataCleaner.getCleanedData(spark)
      analyze(spark, cleanedDF)
    } finally {
      spark.stop()
    }
  }
}