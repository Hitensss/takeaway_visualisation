package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CategoryRatingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CategoryRatingAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（评分>0）
      val validDF = cleanedDF.filter(col("rating") > 0)

      // ========== 分析1：品类评分统计 ==========
      println("\n========== 品类评分统计 ==========")

      val categoryStats = validDF
        .groupBy("category_clean")
        .agg(
          count("shop_name").alias("shop_count"),
          avg("rating").alias("avg_rating"),
          min("rating").alias("min_rating"),
          max("rating").alias("max_rating"),
          stddev("rating").alias("rating_stddev"),
          sum(when(col("rating") >= 4.5, 1).otherwise(0)).alias("high_rating_count")
        )
        .withColumn("high_rating_ratio",
          round(col("high_rating_count") / col("shop_count") * 100, 2))
        .select(
          col("category_clean").alias("category"),
          col("shop_count"),
          round(col("avg_rating"), 2).alias("avg_rating"),
          col("min_rating"),
          col("max_rating"),
          col("rating_stddev"),
          col("high_rating_ratio")
        )
        .orderBy(desc("avg_rating"))

      println("品类评分排名（按平均评分降序）:")
      categoryStats.show(20)

      // 写入品类评分统计表
      categoryStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_rating_stats", MySQLUtils.getProperties())

      println(" 品类评分统计已写入 category_rating_stats 表")

      // ========== 分析2：评分最高和最低的品类 ==========
      println("\n========== 评分最高的品类（Top5） ==========")
      categoryStats
        .filter(col("shop_count") >= 10)  // 至少10家店铺
        .orderBy(desc("avg_rating"))
        .select("category", "avg_rating", "shop_count", "high_rating_ratio")
        .show(5)

      println("\n========== 评分最低的品类（Bottom5） ==========")
      categoryStats
        .filter(col("shop_count") >= 10)
        .orderBy("avg_rating")
        .select("category", "avg_rating", "shop_count", "high_rating_ratio")
        .show(5)

      // ========== 分析3：品类评分分布（用于堆叠柱状图） ==========
      println("\n========== 品类评分分布 ==========")

      val ratingDistribution = validDF
        .withColumn("rating_bucket",
          when(col("rating") < 3.0, "3.0分以下")
            .when(col("rating") < 3.5, "3.0-3.5分")
            .when(col("rating") < 4.0, "3.5-4.0分")
            .when(col("rating") < 4.5, "4.0-4.5分")
            .otherwise("4.5分以上")
        )
        .groupBy("category_clean", "rating_bucket")
        .agg(count("shop_name").alias("shop_count"))
        .withColumnRenamed("category_clean", "category")
        .orderBy("category", "rating_bucket")

      // 计算每个品类内的占比
      val categoryTotal = ratingDistribution
        .groupBy("category")
        .agg(sum("shop_count").alias("total"))

      val distributionWithPercent = ratingDistribution
        .join(categoryTotal, "category")
        .withColumn("percentage", round(col("shop_count") / col("total") * 100, 2))
        .select("category", "rating_bucket", "shop_count", "percentage")
        .orderBy("category", "rating_bucket")

      println("各品类的评分分布:")
      distributionWithPercent.show(50)

      // 写入品类评分分布表
      distributionWithPercent.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_rating_distribution", MySQLUtils.getProperties())

      println(" 品类评分分布已写入 category_rating_distribution 表")

      // ========== 分析4：词云数据生成 ==========
      println("\n========== 词云数据 ==========")

      // 词云需要：品类名称 + 权重（评分越高权重越大）
      val wordCloudData = categoryStats
        .filter(col("shop_count") >= 5)  // 过滤样本太少的品类
        .select(
          col("category"),
          // 权重公式：评分 × 店铺数对数，让评分高且店铺多的品类字更大
          round(col("avg_rating") * log(col("shop_count") + 1), 2).alias("weight")
        )
        .orderBy(desc("weight"))

      println("词云数据（品类 -> 权重）:")
      wordCloudData.show(20)

      // 可选：将词云数据也写入MySQL（方便前端直接读取）
      wordCloudData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_wordcloud", MySQLUtils.getProperties())

      println(" 词云数据已写入 category_wordcloud 表")

      // ========== 分析5：品类评分箱线图数据 ==========
      println("\n========== 品类评分箱线图数据 ==========")

      // 计算每个品类的四分位数
      val boxplotData = validDF
        .groupBy("category_clean")
        .agg(
          count("rating").alias("shop_count"),
          expr("percentile_approx(rating, 0.00)").alias("min_rating"),
          expr("percentile_approx(rating, 0.25)").alias("q1_rating"),
          expr("percentile_approx(rating, 0.50)").alias("median_rating"),
          expr("percentile_approx(rating, 0.75)").alias("q3_rating"),
          expr("percentile_approx(rating, 1.00)").alias("max_rating"),
          avg("rating").alias("mean_rating")
        )
        .filter(col("shop_count") >= 10)
        .withColumnRenamed("category_clean", "category")
        .orderBy(desc("mean_rating"))

      println("品类箱线图数据:")
      boxplotData.show(20)

      // 写入箱线图数据表
      boxplotData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_rating_boxplot", MySQLUtils.getProperties())

      println(" 品类箱线图数据已写入 category_rating_boxplot 表")

      // ========== 分析6：高评分品类特征 ==========
      println("\n========== 高评分品类特征分析 ==========")

      val highRatingCategories = categoryStats
        .filter(col("avg_rating") >= 4.5 && col("shop_count") >= 10)
        .orderBy(desc("avg_rating"))

      println("高评分品类（平均评分≥4.5）:")
      highRatingCategories.show()

      // ========== 分析7：低评分品类特征 ==========
      println("\n========== 低评分品类特征分析 ==========")

      val lowRatingCategories = categoryStats
        .filter(col("avg_rating") < 4.0 && col("shop_count") >= 10)
        .orderBy("avg_rating")

      println("低评分品类（平均评分<4.0）:")
      lowRatingCategories.show()

      // ========== 分析8：品类评分与店铺数量的关系 ==========
      println("\n========== 品类评分与店铺数量关系 ==========")

      val correlation = categoryStats.stat.corr("shop_count", "avg_rating")
      println(s"品类店铺数量与平均评分的相关系数: ${f"$correlation%.4f"}")

      if (correlation > 0.2) {
        println("结论：店铺数量多的品类，评分普遍较高 ")
      } else if (correlation > -0.2) {
        println("结论：品类店铺数量与评分关系不明显")
      } else {
        println("结论：店铺数量多的品类，评分反而较低 ")
      }

    } finally {
      spark.stop()
    }
  }
}
