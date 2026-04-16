package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object MinPriceRatingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MinPriceRatingAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（起送价>=0且评分>0）
      val validDF = cleanedDF
        .filter(col("min_price") >= 0)
        .filter(col("rating") > 0)

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：起送价与评分的整体相关性 ==========
      println("\n========== 起送价与评分相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("min_price", "rating")
      println(s"起送价与评分的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.2) {
        println("  负相关：起送价越低，评分越高 ")
      } else if (correlation > 0.2) {
        println("  正相关：起送价越高，评分越高 ")
      } else if (correlation < 0) {
        println("  弱负相关：起送价与评分有一定负向关系")
      } else {
        println("  弱相关：起送价与评分关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("min_price"),
          col("rating"),
          col("shop_name"),
          col("category_clean").alias("category")
        )
        .orderBy(col("min_price"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "min_price_rating_scatter", MySQLUtils.getProperties())

      println("散点图数据已写入 min_price_rating_scatter 表")

      // ========== 分析3：起送价分组统计 ==========
      println("\n========== 起送价分组统计 ==========")

      // 计算起送价的分位数
      val percentiles = validDF
        .selectExpr(
          "percentile_approx(min_price, 0.25) as p25",
          "percentile_approx(min_price, 0.50) as p50",
          "percentile_approx(min_price, 0.75) as p75"
        )
        .collect()(0)

      val p25 = percentiles.getDouble(0)
      val p50 = percentiles.getDouble(1)
      val p75 = percentiles.getDouble(2)

      println(s"起送价分位数:")
      println(s"  25%分位数: ${f"$p25%.0f"}元")
      println(s"  50%分位数: ${f"$p50%.0f"}元")
      println(s"  75%分位数: ${f"$p75%.0f"}元")

      // 定义起送价分组
      val dfWithGroup = validDF
        .withColumn("price_group",
          when(col("min_price") === 0, "0元（免起送）")
            .when(col("min_price") <= 15, "≤15元")
            .when(col("min_price") <= 20, "15-20元")
            .when(col("min_price") <= 25, "20-25元")
            .when(col("min_price") <= 30, "25-30元")
            .otherwise(">30元")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("price_group")
        .agg(
          count("shop_name").alias("shop_count"),
          min("min_price").alias("min_price"),
          max("min_price").alias("max_price"),
          avg("rating").alias("avg_rating"),
          expr("percentile_approx(rating, 0.50)").alias("median_rating"),
          sum(when(col("rating") >= 4.5, 1).otherwise(0)).alias("high_rating_count")
        )
        .withColumn("high_rating_ratio",
          round(col("high_rating_count") / col("shop_count") * 100, 2))
        .select(
          col("price_group"),
          col("min_price"),
          col("max_price"),
          col("shop_count"),
          round(col("avg_rating"), 2).alias("avg_rating"),
          col("median_rating"),
          col("high_rating_ratio")
        )
        .orderBy(
          when(col("price_group") === "0元（免起送）", 1)
            .when(col("price_group") === "≤15元", 2)
            .when(col("price_group") === "15-20元", 3)
            .when(col("price_group") === "20-25元", 4)
            .when(col("price_group") === "25-30元", 5)
            .otherwise(6)
        )

      println("\n分组统计结果:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "min_price_rating_group", MySQLUtils.getProperties())

      println(" 分组统计已写入 min_price_rating_group 表")

      // ========== 分析4：低起送价 vs 高起送价对比 ==========
      println("\n========== 低起送价 vs 高起送价对比 ==========")

      val lowMinPriceDF = validDF.filter(col("min_price") <= 15)
      val highMinPriceDF = validDF.filter(col("min_price") > 25)

      val lowAvgRating = lowMinPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
      val highAvgRating = highMinPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]

      println(s"低起送价商家（≤15元）数量: ${lowMinPriceDF.count()}")
      println(s"低起送价商家平均评分: ${f"$lowAvgRating%.2f"}")
      println(s"\n高起送价商家（>25元）数量: ${highMinPriceDF.count()}")
      println(s"高起送价商家平均评分: ${f"$highAvgRating%.2f"}")

      val diff = lowAvgRating - highAvgRating

      if (diff > 0.15) {
        println(f"\n结论：起送价越低的商家评分显著越高  (+$diff%.2f)")
        } else if (diff > 0) {
          println(f"\n结论：起送价越低的商家评分略高 (+$diff%.2f)")
        } else if (diff < -0.15) {
          println(f"\n结论：起送价越低的商家评分反而更低  ($diff%.2f)")
        } else if (diff < 0) {
          println(f"\n结论：起送价越低的商家评分略低 ($diff%.2f)")
        } else {
          println(s"\n结论：起送价与评分无明显关系")
      }

      // ========== 分析5：免起送商家分析 ==========
      println("\n========== 免起送商家分析 ==========")

      val freeMinPriceDF = validDF.filter(col("min_price") === 0)
      val paidMinPriceDF = validDF.filter(col("min_price") > 0)

      val freeAvgRating = freeMinPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
      val paidAvgRating = paidMinPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]

      println(s"免起送商家数量: ${freeMinPriceDF.count()}")
      println(s"免起送商家平均评分: ${f"$freeAvgRating%.2f"}")
      println(s"\n有起送价商家数量: ${paidMinPriceDF.count()}")
      println(s"有起送价商家平均评分: ${f"$paidAvgRating%.2f"}")

      val freeDiff = freeAvgRating - paidAvgRating

      if (freeDiff > 0.1) {
        println(f"\n结论：免起送商家的评分显著高于有起送价商家  (+$freeDiff%.2f)")
        } else if (freeDiff > 0) {
          println(f"\n结论：免起送商家的评分略高于有起送价商家 (+$freeDiff%.2f)")
        } else {
          println(s"\n结论：免起送与有起送价商家评分差异不大")
      }

      // ========== 分析6：按品类细分 ==========
      println("\n========== 不同品类的起送价-评分关系 ==========")

      val categoryMinPriceRating = validDF
        .groupBy("category_clean")
        .agg(
          avg("min_price").alias("avg_min_price"),
          avg("rating").alias("avg_rating"),
          count("shop_name").alias("shop_count"),
          corr("min_price", "rating").alias("correlation")
        )
        .filter(col("shop_count") >= 10)
        .orderBy("correlation")

      println("各品类的起送价-评分相关系数:")
      categoryMinPriceRating.show(20)

      // 找出起送价影响最明显的品类
      println("\n起送价对评分影响最明显的品类（负相关最强）:")
      categoryMinPriceRating
        .filter(col("correlation") < -0.1)
        .select("category_clean", "avg_min_price", "avg_rating", "correlation")
        .show(10)

      // ========== 分析7：高评分商家的起送价特征 ==========
      println("\n========== 高评分商家的起送价特征 ==========")

      val highRatingDF = validDF.filter(col("rating") >= 4.5)
      val lowRatingDF = validDF.filter(col("rating") < 4.0)

      val highPriceStats = highRatingDF.select(
        avg("min_price").alias("avg_price"),
        expr("percentile_approx(min_price, 0.5)").alias("median_price")
      ).collect()(0)

      val lowPriceStats = lowRatingDF.select(
        avg("min_price").alias("avg_price"),
        expr("percentile_approx(min_price, 0.5)").alias("median_price")
      ).collect()(0)

      val highAvgPrice = highPriceStats.getAs[Double]("avg_price")
      val highMedianPrice = highPriceStats.getAs[Double]("median_price")
      val lowAvgPrice = lowPriceStats.getAs[Double]("avg_price")
      val lowMedianPrice = lowPriceStats.getAs[Double]("median_price")

      println(s"高评分商家（≥4.5分）平均起送价: ${f"$highAvgPrice%.1f"}元")
      println(s"高评分商家（≥4.5分）起送价中位数: ${f"$highMedianPrice%.1f"}元")
      println(s"\n低评分商家（<4.0分）平均起送价: ${f"$lowAvgPrice%.1f"}元")
      println(s"低评分商家（<4.0分）起送价中位数: ${f"$lowMedianPrice%.1f"}元")

      // ========== 分析8：起送价分布 ==========
      println("\n========== 起送价分布 ==========")

      val priceDistribution = validDF
        .withColumn("price_bucket",
          when(col("min_price") === 0, "0元")
            .when(col("min_price") <= 10, "1-10元")
            .when(col("min_price") <= 15, "10-15元")
            .when(col("min_price") <= 20, "15-20元")
            .when(col("min_price") <= 25, "20-25元")
            .when(col("min_price") <= 30, "25-30元")
            .otherwise(">30元")
        )
        .groupBy("price_bucket")
        .agg(count("shop_name").alias("shop_count"))
        .orderBy("price_bucket")

      println("起送价分布:")
      priceDistribution.show()

      // ========== 分析9：起送价与评分的关系曲线 ==========
      println("\n========== 起送价-评分趋势 ==========")

      // 按5元间隔计算平均评分
      val trendData = validDF
        .withColumn("price_interval",
          when(col("min_price") === 0, 0)
            .when(col("min_price") <= 10, 10)
            .when(col("min_price") <= 15, 15)
            .when(col("min_price") <= 20, 20)
            .when(col("min_price") <= 25, 25)
            .when(col("min_price") <= 30, 30)
            .otherwise(35)
        )
        .groupBy("price_interval")
        .agg(
          avg("rating").alias("avg_rating"),
          count("shop_name").alias("shop_count")
        )
        .orderBy("price_interval")

      println("起送价-评分趋势:")
      trendData.show()

    } finally {
      spark.stop()
    }
  }
}
