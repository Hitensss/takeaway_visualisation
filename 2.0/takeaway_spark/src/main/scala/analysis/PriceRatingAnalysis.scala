package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.MySQLUtils

object PriceRatingAnalysis {

  /**
   * 供RunAllAnalysis调用的分析方法
   */
  def analyze(spark: SparkSession, cleanedDF: DataFrame): Unit = {
    println("开始执行人均评分分析...")

    // 过滤有效数据（人均>0且评分>0）
    val validDF = cleanedDF
      .filter(col("avg_price") > 0 && col("rating") > 0)

    println(s"有效数据量: ${validDF.count()}")

    // ========== 分析1：人均与评分的整体相关性 ==========
    println("\n========== 人均与评分相关性分析 ==========")

    val correlation = validDF.stat.corr("avg_price", "rating")
    println(s"人均与评分的皮尔逊相关系数: ${f"$correlation%.4f"}")

    println("\n相关性解读:")
    if (correlation > 0.3) {
      println("  正相关：人均越高，评分越高 ")
    } else if (correlation > 0) {
      println("  弱正相关：人均与评分有一定正向关系")
    } else if (correlation > -0.3) {
      println("  弱负相关：人均越高，评分反而越低 ")
    } else {
      println("  负相关：人均越高，评分越低 ")
    }

    // ========== 分析2：散点图数据 ==========
    println("\n========== 散点图数据 ==========")

    val scatterData = validDF
      .select(
        col("avg_price"),
        col("rating"),
        col("shop_name"),
        col("category_clean").alias("category")
      )
      .orderBy(col("avg_price"))

    println(s"散点图数据量: ${scatterData.count()}")
    scatterData.show(20)

    scatterData.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "price_rating_scatter", MySQLUtils.getProperties())

    println("散点图数据已写入 price_rating_scatter 表")

    // ========== 分析3：人均分组统计 ==========
    println("\n========== 人均分组统计 ==========")

    val percentiles = validDF
      .selectExpr(
        "percentile_approx(avg_price, 0.25) as p25",
        "percentile_approx(avg_price, 0.50) as p50",
        "percentile_approx(avg_price, 0.75) as p75"
      )
      .collect()(0)

    val p25 = percentiles.getDouble(0)
    val p50 = percentiles.getDouble(1)
    val p75 = percentiles.getDouble(2)

    println(s"人均消费分位数:")
    println(s"  25%分位数: ${f"$p25%.0f"}元")
    println(s"  50%分位数: ${f"$p50%.0f"}元")
    println(s"  75%分位数: ${f"$p75%.0f"}元")

    val dfWithGroup = validDF
      .withColumn("price_group",
        when(col("avg_price") <= 15, "低价位(≤15元)")
          .when(col("avg_price") <= 25, "中低价位(15-25元)")
          .when(col("avg_price") <= 40, "中高价位(25-40元)")
          .otherwise("高价位(>40元)")
      )

    val groupStats = dfWithGroup.groupBy("price_group")
      .agg(
        count("shop_name").alias("shop_count"),
        min("avg_price").alias("min_price"),
        max("avg_price").alias("max_price"),
        avg("rating").alias("avg_rating"),
        expr("percentile_approx(rating, 0.50)").alias("median_rating"),
        stddev("rating").alias("stddev_rating")
      )
      .orderBy(
        when(col("price_group") === "低价位(≤15元)", 1)
          .when(col("price_group") === "中低价位(15-25元)", 2)
          .when(col("price_group") === "中高价位(25-40元)", 3)
          .otherwise(4)
      )

    println("\n分组统计结果:")
    groupStats.show()

    groupStats.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "price_rating_group", MySQLUtils.getProperties())

    println("分组统计已写入 price_rating_group 表")

    // ========== 分析4：高人均 vs 低人均对比 ==========
    println("\n========== 高人均 vs 低人均对比 ==========")

    val lowPriceDF = validDF.filter(col("avg_price") <= 25)
    val highPriceDF = validDF.filter(col("avg_price") > 40)

    val lowPriceAvgRating = lowPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
    val highPriceAvgRating = highPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]

    println(s"低价位商家（≤25元）平均评分: ${f"$lowPriceAvgRating%.2f"}")
    println(s"高价位商家（>40元）平均评分: ${f"$highPriceAvgRating%.2f"}")

    val diff = highPriceAvgRating - lowPriceAvgRating

    if (diff > 0.3) {
      println(f"结论：高价位商家评分显著高于低价位商家  (+${diff}%.2f)")
    } else if (diff > 0) {
      println(f"结论：高价位商家评分略高于低价位商家 (+${diff}%.2f)")
    } else {
      println(f"结论：高价位商家评分并未高于低价位商家  (${diff}%.2f)")
    }

    // ========== 分析5：不同价格区间的评分分布 ==========
    println("\n========== 不同价格区间的评分分布 ==========")

    val priceRatingDistribution = dfWithGroup
      .withColumn("rating_bucket",
        when(col("rating") < 3.5, "3.5分以下")
          .when(col("rating") < 4.0, "3.5-4.0分")
          .when(col("rating") < 4.5, "4.0-4.5分")
          .otherwise("4.5分以上")
      )
      .groupBy("price_group", "rating_bucket")
      .agg(count("shop_name").alias("shop_count"))
      .orderBy("price_group", "rating_bucket")

    println("各价格区间的评分分布:")
    priceRatingDistribution.show(50)

    // ========== 分析6：按品类细分 ==========
    println("\n========== 不同品类的价格-评分关系 ==========")

    val categoryPriceRating = validDF
      .groupBy("category_clean")
      .agg(
        avg("avg_price").alias("avg_price"),
        avg("rating").alias("avg_rating"),
        count("shop_name").alias("shop_count"),
        corr("avg_price", "rating").alias("correlation")
      )
      .filter(col("shop_count") >= 10)
      .orderBy(desc("correlation"))

    println("各品类的人均-评分相关系数:")
    categoryPriceRating.show(20)

    // ========== 分析7：价格与评分的最佳区间 ==========
    println("\n========== 价格与评分的最佳区间 ==========")

    val bestPriceRange = groupStats
      .orderBy(desc("avg_rating"))
      .select("price_group", "avg_rating", "shop_count")
      .first()

    println(s"评分最高的价格区间: ${bestPriceRange.getString(0)}")
    println(s"  平均评分: ${bestPriceRange.getDouble(1)}")
    println(s"  店铺数量: ${bestPriceRange.getLong(2)}")

    // ========== 分析8：异常值分析 ==========
    println("\n========== 异常值分析 ==========")

    val lowRatingHighPrice = validDF
      .filter(col("rating") < 3.5 && col("avg_price") > 50)
      .select("shop_name", "category_clean", "avg_price", "rating")
      .orderBy(desc("avg_price"))

    println("低评分高人均的店铺（可能名不副实）:")
    lowRatingHighPrice.show(10)

    val highRatingLowPrice = validDF
      .filter(col("rating") >= 4.5 && col("avg_price") <= 20)
      .select("shop_name", "category_clean", "avg_price", "rating")
      .orderBy(desc("rating"))

    println("\n高评分低人均的店铺（性价比之王）:")
    highRatingLowPrice.show(10)

    println("人均评分分析完成")
  }

  /**
   * 独立运行时的main方法
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PriceRatingAnalysis")
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