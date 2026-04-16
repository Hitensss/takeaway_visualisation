package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DeliveryFeeTimeAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FeeTimeAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（配送费>=0，送达时间>0）
      val validDF = cleanedDF
        .filter(col("delivery_fee") >= 0)
        .filter(col("delivery_time") > 0)

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：配送费与送达时间的整体相关性 ==========
      println("\n========== 配送费与送达时间相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("delivery_fee", "delivery_time")
      println(s"配送费与送达时间的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.3) {
        println("  强负相关：配送费越高，送达时间越短 ")
      } else if (correlation < -0.15) {
        println("  中等负相关：高配送费确实送得更快")
      } else if (correlation < 0) {
        println("  弱负相关：配送费与送达时间有微弱负向关系")
      } else if (correlation > 0) {
        println("  正相关：配送费越高，送达时间越长 ")
      } else {
        println("  弱相关：配送费与送达时间关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("delivery_fee"),
          col("delivery_time"),
          col("shop_name"),
          col("category_clean").alias("category"),
          col("rating"),
          col("monthly_sales")
        )
        .orderBy(col("delivery_fee"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "fee_time_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 fee_time_scatter 表")

      // ========== 分析3：配送费分组送达时间统计 ==========
      println("\n========== 配送费分组送达时间统计 ==========")

      // 计算配送费的分位数
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
            .when(col("delivery_fee") <= 1.0, "低配送费(≤1元)")
            .when(col("delivery_fee") <= 2.0, "中低配送费(1-2元)")
            .when(col("delivery_fee") <= 3.0, "中配送费(2-3元)")
            .when(col("delivery_fee") <= 5.0, "中高配送费(3-5元)")
            .otherwise("高配送费(>5元)")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("fee_group")
        .agg(
          count("shop_name").alias("shop_count"),
          min("delivery_fee").alias("min_fee"),
          max("delivery_fee").alias("max_fee"),
          avg("delivery_time").alias("avg_time"),
          expr("percentile_approx(delivery_time, 0.50)").alias("median_time"),
          min("delivery_time").alias("min_time"),
          max("delivery_time").alias("max_time"),
          stddev("delivery_time").alias("stddev_time")
        )
        .select(
          col("fee_group"),
          col("min_fee"),
          col("max_fee"),
          col("shop_count"),
          round(col("avg_time"), 1).alias("avg_time"),
          col("median_time"),
          col("min_time"),
          col("max_time")
        )
        .orderBy(
          when(col("fee_group") === "免配送费", 1)
            .when(col("fee_group") === "低配送费(≤1元)", 2)
            .when(col("fee_group") === "中低配送费(1-2元)", 3)
            .when(col("fee_group") === "中配送费(2-3元)", 4)
            .when(col("fee_group") === "中高配送费(3-5元)", 5)
            .otherwise(6)
        )

      println("\n配送费分组送达时间统计:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "fee_time_group", MySQLUtils.getProperties())

      println(" 配送费分组送达时间统计已写入 fee_time_group 表")

      // ========== 分析4：高配送费 vs 免配送费对比 ==========
      println("\n========== 高配送费 vs 免配送费送达时间对比 ==========")

      val highFeeDF = validDF.filter(col("delivery_fee") > 3.0)
      val freeFeeDF = validDF.filter(col("delivery_fee") === 0)


      val highAvgTime = highFeeDF.select(avg("monthly_sales")).collect()(0).getDouble(0)
      val freeAvgTime = freeFeeDF.select(avg("monthly_sales")).collect()(0).getDouble(0)
      // 使用 cast 确保返回 Double
      val highMedianTime = highFeeDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      val freeMedianTime = freeFeeDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      println(s"高配送费商家（>3元）数量: ${highFeeDF.count()}")
      println(s"高配送费商家平均送达时间: ${f"$highAvgTime%.1f"}分钟")
      println(s"高配送费商家送达时间中位数: ${f"$highMedianTime%.0f"}分钟")
      println(s"\n免配送费商家数量: ${freeFeeDF.count()}")
      println(s"免配送费商家平均送达时间: ${f"$freeAvgTime%.1f"}分钟")
      println(s"免配送费商家送达时间中位数: ${f"$freeMedianTime%.0f"}分钟")

      val diff = freeAvgTime - highAvgTime

      if (diff > 5) {
        println(s"\n结论：高配送费商家送达时间显著更短  (快${f"$diff%.1f"}分钟)")
        } else if (diff > 0) {
          println(s"\n结论：高配送费商家送达时间略短 (快${f"$diff%.1f"}分钟)")
        } else {
          println(s"\n结论：配送费与送达时间关系不明显")
      }

      // ========== 分析5：配送费与送达时间的趋势线数据 ==========
      println("\n========== 配送费-送达时间趋势 ==========")

      // 按配送费区间计算平均送达时间
      val trendData = validDF
        .withColumn("fee_interval",
          when(col("delivery_fee") === 0, 0)
            .when(col("delivery_fee") <= 1, 1)
            .when(col("delivery_fee") <= 2, 2)
            .when(col("delivery_fee") <= 3, 3)
            .when(col("delivery_fee") <= 4, 4)
            .when(col("delivery_fee") <= 5, 5)
            .otherwise(6)
        )
        .groupBy("fee_interval")
        .agg(
          avg("delivery_time").alias("avg_time"),
          count("shop_name").alias("shop_count")
        )
        .orderBy("fee_interval")

      println("配送费-送达时间趋势:")
      trendData.show()

      // ========== 分析6：不同品类的配送费-送达时间关系 ==========
      println("\n========== 不同品类的配送费-送达时间关系 ==========")

      val categoryFeeTime = validDF
        .groupBy("category_clean")
        .agg(
          avg("delivery_fee").alias("avg_fee"),
          avg("delivery_time").alias("avg_time"),
          count("shop_name").alias("shop_count"),
          corr("delivery_fee", "delivery_time").alias("correlation")
        )
        .filter(col("shop_count") >= 10)
        .orderBy("correlation")

      println("各品类的配送费-送达时间相关系数:")
      categoryFeeTime.show(20)

      // 找出配送费影响最明显的品类
      println("\n配送费对送达时间影响最明显的品类（负相关最强）:")
      categoryFeeTime
        .filter(col("correlation") < -0.2)
        .select("category_clean", "avg_fee", "avg_time", "correlation")
        .show(10)

      // ========== 分析7：快送商家的配送费特征 ==========
      println("\n========== 快送商家的配送费特征 ==========")

      val fastDeliveryDF = validDF.filter(col("delivery_time") <= 25)
      val slowDeliveryDF = validDF.filter(col("delivery_time") > 40)

      val fastAvgFee = fastDeliveryDF.select(avg("delivery_fee")).collect()(0)(0).asInstanceOf[Double]
      val slowAvgFee = slowDeliveryDF.select(avg("delivery_fee")).collect()(0)(0).asInstanceOf[Double]
      val fastHighFeeRatio = fastDeliveryDF.filter(col("delivery_fee") > 2).count().toDouble / fastDeliveryDF.count() * 100
      val slowHighFeeRatio = slowDeliveryDF.filter(col("delivery_fee") > 2).count().toDouble / slowDeliveryDF.count() * 100

      println(s"快送商家（≤25分钟）平均配送费: ${f"$fastAvgFee%.2f"}元")
      println(s"快送商家（≤25分钟）高配送费占比(>2元): ${f"$fastHighFeeRatio%.1f"}%")
        println(s"\n慢送商家（>40分钟）平均配送费: ${f"$slowAvgFee%.2f"}元")
        println(s"慢送商家（>40分钟）高配送费占比(>2元): ${f"$slowHighFeeRatio%.1f"}%")

          // ========== 分析8：不同评分区间的配送费-送达时间关系 ==========
          println("\n========== 不同评分区间的配送费-送达时间关系 ==========")

          val ratingFeeTime = validDF
            .withColumn("rating_group",
              when(col("rating") < 4.0, "低评分(<4.0)")
                .when(col("rating") < 4.5, "中评分(4.0-4.5)")
                .otherwise("高评分(≥4.5)")
            )
            .groupBy("rating_group")
            .agg(
              corr("delivery_fee", "delivery_time").alias("correlation"),
              avg("delivery_fee").alias("avg_fee"),
              avg("delivery_time").alias("avg_time")
            )

          println("不同评分区间的配送费-送达时间相关系数:")
          ratingFeeTime.show()

          // ========== 分析9：配送费与送达时间的效率分析 ==========
          println("\n========== 配送费与送达时间效率分析 ==========")

          // 计算每元配送费节省的时间
          val efficiencyData = validDF
            .withColumn("fee_group",
              when(col("delivery_fee") === 0, "免配送费")
                .when(col("delivery_fee") <= 1, "低配送费")
                .when(col("delivery_fee") <= 2, "中低配送费")
                .when(col("delivery_fee") <= 3, "中配送费")
                .otherwise("高配送费")
            )
            .groupBy("fee_group")
            .agg(
              avg("delivery_time").alias("avg_time"),
              avg("delivery_fee").alias("avg_fee")
            )
            .orderBy("avg_fee")

          val baseline = efficiencyData.filter(col("fee_group") === "免配送费").select("avg_time").collect()(0)(0).asInstanceOf[Double]

          println("配送费与送达时间效率分析:")
          efficiencyData.withColumn("time_saved",
              when(col("avg_fee") > 0, lit(baseline) - col("avg_time"))
                .otherwise(0))
            .withColumn("saved_per_yuan",
              when(col("avg_fee") > 0,
                (lit(baseline) - col("avg_time"))
                  .divide(col("avg_fee")))
                .otherwise(0))
            .show()

          // ========== 分析10：配送费-送达时间箱线图数据（按品类） ==========
          println("\n========== 品类配送费-送达时间箱线图数据 ==========")

          val timeBoxplot = validDF
            .groupBy("category_clean")
            .agg(
              count("delivery_time").alias("shop_count"),
              expr("percentile_approx(delivery_time, 0.00)").alias("min_time"),
              expr("percentile_approx(delivery_time, 0.25)").alias("q1_time"),
              expr("percentile_approx(delivery_time, 0.50)").alias("median_time"),
              expr("percentile_approx(delivery_time, 0.75)").alias("q3_time"),
              expr("percentile_approx(delivery_time, 1.00)").alias("max_time"),
              avg("delivery_time").alias("mean_time"),
              avg("delivery_fee").alias("avg_fee")
            )
            .filter(col("shop_count") >= 10)
            .withColumnRenamed("category_clean", "category")
            .orderBy("median_time")

          println("各品类送达时间箱线图数据（含平均配送费）:")
          timeBoxplot.show(20)

    } finally {
      spark.stop()
    }
  }
}