package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DeliveryFeeRatingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeliveryFeeRatingAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据
      val validDF = cleanedDF
        .filter(col("rating") > 0)
        .filter(col("delivery_fee") >= 0)  // 配送费可能为0（免配送费）

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：配送费与评分的整体相关性 ==========
      println("\n========== 配送费与评分相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("delivery_fee", "rating")
      println(s"配送费与评分的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.2) {
        println("  负相关：配送费越低，评分越高 ")
      } else if (correlation > 0.2) {
        println("  正相关：配送费越高，评分越高 ")
      } else {
        println("  弱相关：配送费与评分关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("delivery_fee"),
          col("rating"),
          col("shop_name"),
          col("category_clean").alias("category")
        )
        .orderBy(col("delivery_fee"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "delivery_fee_rating_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 delivery_fee_rating_scatter 表")

      // ========== 分析3：配送费分组统计（用于柱状图） ==========
      println("\n========== 配送费分组统计 ==========")

      // 计算配送费的分位数，用于确定分组边界
      val percentiles = validDF
        .selectExpr(
          "percentile_approx(delivery_fee, 0.25) as p25",
          "percentile_approx(delivery_fee, 0.50) as p50",
          "percentile_approx(delivery_fee, 0.75) as p75"
        )
        .collect()(0)

      val p25 = percentiles.getDouble(0)
      val p50 = percentiles.getDouble(1)
      val p75 = percentiles.getDouble(2)

      println(s"配送费分位数:")
      println(s"  25%分位数: ${f"$p25%.2f"}元")
      println(s"  50%分位数: ${f"$p50%.2f"}元")
      println(s"  75%分位数: ${f"$p75%.2f"}元")

      // 定义配送费分组
      val dfWithGroup = validDF
        .withColumn("fee_group",
          when(col("delivery_fee") === 0, "免配送费")
            .when(col("delivery_fee") <= 1.0, "0-1元")
            .when(col("delivery_fee") <= 2.0, "1-2元")
            .when(col("delivery_fee") <= 3.0, "2-3元")
            .when(col("delivery_fee") <= 5.0, "3-5元")
            .otherwise("5元以上")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("fee_group")
        .agg(
          count("shop_name").alias("shop_count"),
          min("delivery_fee").alias("min_fee"),
          max("delivery_fee").alias("max_fee"),
          avg("rating").alias("avg_rating"),
          expr("percentile_approx(rating, 0.50)").alias("median_rating"),
          sum(when(col("rating") >= 4.5, 1).otherwise(0)).alias("high_rating_count")
        )
        .withColumn("high_rating_ratio",
          round(col("high_rating_count") / col("shop_count") * 100, 2))
        .select(
          col("fee_group"),
          col("min_fee"),
          col("max_fee"),
          col("shop_count"),
          round(col("avg_rating"), 2).alias("avg_rating"),
          col("median_rating"),
          col("high_rating_ratio")
        )
        .orderBy(
          when(col("fee_group") === "免配送费", 1)
            .when(col("fee_group") === "0-1元", 2)
            .when(col("fee_group") === "1-2元", 3)
            .when(col("fee_group") === "2-3元", 4)
            .when(col("fee_group") === "3-5元", 5)
            .otherwise(6)
        )

      println("\n分组统计结果:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "delivery_fee_rating_group", MySQLUtils.getProperties())

      println(" 分组统计已写入 delivery_fee_rating_group 表")

      // ========== 分析4：免配送费 vs 收费配送对比 ==========
      println("\n========== 免配送费 vs 收费配送对比 ==========")

      val freeDeliveryDF = validDF.filter(col("delivery_fee") === 0)
      val paidDeliveryDF = validDF.filter(col("delivery_fee") > 0)

      val freeAvgRating = freeDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
      val paidAvgRating = paidDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]

      println(s"免配送费商家数量: ${freeDeliveryDF.count()}")
      println(s"免配送费商家平均评分: ${f"$freeAvgRating%.2f"}")
      println(s"\n收费配送商家数量: ${paidDeliveryDF.count()}")
      println(s"收费配送商家平均评分: ${f"$paidAvgRating%.2f"}")

      val diff = freeAvgRating.asInstanceOf[Double] - paidAvgRating.asInstanceOf[Double]

      if (diff > 0.1) {
        println(f"\n结论：免配送费商家的评分显著高于收费配送商家  (+$diff%.2f})")
        } else if (diff > 0) {
          println(f"\n结论：免配送费商家的评分略高于收费配送商家 (+$diff%.2f})")
        } else {
          println(f"\n结论：免配送费商家的评分并未高于收费配送商家 ")
      }

      // ========== 分析5：不同配送费区间的评分分布 ==========
      println("\n========== 不同配送费区间的评分分布 ==========")

      val feeRatingDistribution = dfWithGroup
        .withColumn("rating_bucket",
          when(col("rating") < 3.5, "3.5分以下")
            .when(col("rating") < 4.0, "3.5-4.0分")
            .when(col("rating") < 4.5, "4.0-4.5分")
            .otherwise("4.5分以上")
        )
        .groupBy("fee_group", "rating_bucket")
        .agg(count("shop_name").alias("shop_count"))
        .orderBy("fee_group", "rating_bucket")

      println("各配送费区间的评分分布:")
      feeRatingDistribution.show(50)

      // ========== 分析6：按品类细分 ==========
      println("\n========== 不同品类的配送费-评分关系 ==========")

      val categoryFeeRating = validDF
        .groupBy("category_clean")
        .agg(
          avg("delivery_fee").alias("avg_delivery_fee"),
          avg("rating").alias("avg_rating"),
          count("shop_name").alias("shop_count"),
          corr("delivery_fee", "rating").alias("correlation")
        )
        .filter(col("shop_count") >= 10)
        .orderBy("correlation")

      println("各品类的配送费-评分相关系数:")
      categoryFeeRating.show(20)

      // 找出配送费影响最明显的品类
      println("\n配送费对评分影响最明显的品类（负相关最强）:")
      categoryFeeRating
        .filter(col("correlation") < -0.2)
        .select("category_clean", "avg_delivery_fee", "avg_rating", "correlation")
        .show(10)

      // ========== 分析7：高评分商家的配送费特征 ==========
      println("\n========== 高评分商家的配送费特征 ==========")

      val highRatingDF = validDF.filter(col("rating") >= 4.5)
      val lowRatingDF = validDF.filter(col("rating") < 4.0)

      val highFeeStats = highRatingDF.select(
        avg("delivery_fee").alias("avg_fee"),
        expr("percentile_approx(delivery_fee, 0.5)").alias("median_fee")
      ).collect()(0)

      val lowFeeStats = lowRatingDF.select(
        avg("delivery_fee").alias("avg_fee"),
        expr("percentile_approx(delivery_fee, 0.5)").alias("median_fee")
      ).collect()(0)

      println(s"高评分商家（≥4.5分）平均配送费: ${f"${highFeeStats.getDouble(0)}%.2f"}元")
      println(s"高评分商家（≥4.5分）配送费中位数: ${f"${highFeeStats.getDouble(1)}%.2f"}元")
      println(s"\n低评分商家（<4.0分）平均配送费: ${f"${lowFeeStats.getDouble(0)}%.2f"}元")
      println(s"低评分商家（<4.0分）配送费中位数: ${f"${lowFeeStats.getDouble(1)}%.2f"}元")

      // ========== 分析8：免配送费商家的评分分布 ==========
      println("\n========== 免配送费商家评分分布 ==========")

      val freeDeliveryRatingDist = freeDeliveryDF
        .withColumn("rating_bucket",
          when(col("rating") < 4.0, "4.0分以下")
            .when(col("rating") < 4.5, "4.0-4.5分")
            .otherwise("4.5分以上")
        )
        .groupBy("rating_bucket")
        .agg(count("shop_name").alias("shop_count"))
        .withColumn("percentage",
          round(col("shop_count") / sum("shop_count").over() * 100, 2))

      println("免配送费商家的评分分布:")
      freeDeliveryRatingDist.show()

    } finally {
      spark.stop()
    }
  }
}
