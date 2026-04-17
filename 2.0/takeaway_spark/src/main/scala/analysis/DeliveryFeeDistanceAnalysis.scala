package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.MySQLUtils

object DeliveryFeeDistanceAnalysis {

  /**
   * 供RunAllAnalysis调用的分析方法
   */
  def analyze(spark: SparkSession, cleanedDF: DataFrame): Unit = {
    println("开始执行配送费距离分析...")

    // 过滤有效数据（配送费>=0，距离>0）
    val validDF = cleanedDF
      .filter(col("delivery_fee") >= 0)
      .filter(col("distance") > 0)

    println(s"有效数据量: ${validDF.count()}")

    // ========== 分析1：配送费与距离的整体相关性 ==========
    println("\n========== 配送费与距离相关性分析 ==========")

    // 计算相关系数
    val correlation = validDF.stat.corr("delivery_fee", "distance")
    println(s"配送费与距离的皮尔逊相关系数: ${f"$correlation%.4f"}")

    // 解释相关性
    println("\n相关性解读:")
    if (correlation > 0.5) {
      println("  强正相关：距离越远，配送费越高 ")
    } else if (correlation > 0.3) {
      println("  中等正相关：距离对配送费有明显正向影响")
    } else if (correlation > 0) {
      println("  弱正相关：距离与配送费有一定正向关系")
    } else if (correlation < 0) {
      println("  负相关：距离越远，配送费越低 ")
    } else {
      println("  弱相关：距离与配送费关系不明显")
    }

    // ========== 分析2：散点图数据 ==========
    println("\n========== 散点图数据 ==========")

    val scatterData = validDF
      .select(
        col("delivery_fee"),
        col("distance"),
        col("shop_name"),
        col("category_clean").alias("category"),
        col("rating"),
        col("monthly_sales")
      )
      .orderBy(col("distance"))

    println(s"散点图数据量: ${scatterData.count()}")
    scatterData.show(20)

    // 写入散点图数据表
    scatterData.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "fee_distance_scatter", MySQLUtils.getProperties())

    println("散点图数据已写入 fee_distance_scatter 表")

    // ========== 分析3：距离分组配送费统计 ==========
    println("\n========== 距离分组配送费统计 ==========")

    // 计算距离的分位数
    val percentiles = validDF
      .selectExpr(
        "percentile_approx(distance, 0.25) as p25",
        "percentile_approx(distance, 0.50) as p50",
        "percentile_approx(distance, 0.75) as p75"
      )
      .collect()(0)

    val p25 = percentiles.getInt(0)
    val p50 = percentiles.getInt(1)
    val p75 = percentiles.getInt(2)

    println(s"距离分位数:")
    println(s"  25%分位数: ${p25}米")
    println(s"  50%分位数: ${p50}米")
    println(s"  75%分位数: ${p75}米")

    // 定义距离分组
    val dfWithGroup = validDF
      .withColumn("distance_group",
        when(col("distance") <= 500, "极近(≤500m)")
          .when(col("distance") <= 1000, "近距(500-1000m)")
          .when(col("distance") <= 1500, "中距(1000-1500m)")
          .when(col("distance") <= 2000, "中远距(1500-2000m)")
          .when(col("distance") <= 3000, "远距(2000-3000m)")
          .otherwise("超远距(>3000m)")
      )

    // 分组统计
    val groupStats = dfWithGroup.groupBy("distance_group")
      .agg(
        count("shop_name").alias("shop_count"),
        min("distance").alias("min_distance"),
        max("distance").alias("max_distance"),
        avg("delivery_fee").alias("avg_fee"),
        expr("percentile_approx(delivery_fee, 0.50)").alias("median_fee"),
        min("delivery_fee").alias("min_fee"),
        max("delivery_fee").alias("max_fee"),
        stddev("delivery_fee").alias("stddev_fee")
      )
      .select(
        col("distance_group"),
        col("min_distance"),
        col("max_distance"),
        col("shop_count"),
        round(col("avg_fee"), 2).alias("avg_fee"),
        col("median_fee"),
        col("min_fee"),
        col("max_fee")
      )
      .orderBy(
        when(col("distance_group") === "极近(≤500m)", 1)
          .when(col("distance_group") === "近距(500-1000m)", 2)
          .when(col("distance_group") === "中距(1000-1500m)", 3)
          .when(col("distance_group") === "中远距(1500-2000m)", 4)
          .when(col("distance_group") === "远距(2000-3000m)", 5)
          .otherwise(6)
      )

    println("\n距离分组配送费统计:")
    groupStats.show()

    // 写入分组统计表
    groupStats.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "distance_fee_group", MySQLUtils.getProperties())

    println("距离分组配送费统计已写入 distance_fee_group 表")

    // ========== 分析4：近距 vs 远距对比 ==========
    println("\n========== 近距 vs 远距配送费对比 ==========")

    val nearDF = validDF.filter(col("distance") <= 1000)
    val farDF = validDF.filter(col("distance") > 2000)

    val nearAvgFee = nearDF.select(avg("delivery_fee")).collect()(0)(0).asInstanceOf[Double]
    val farAvgFee = farDF.select(avg("delivery_fee")).collect()(0)(0).asInstanceOf[Double]
    val nearMedianFee = nearDF.selectExpr("percentile_approx(delivery_fee, 0.5)").collect()(0)(0).asInstanceOf[Double]
    val farMedianFee = farDF.selectExpr("percentile_approx(delivery_fee, 0.5)").collect()(0)(0).asInstanceOf[Double]

    println(s"近距商家（≤1km）数量: ${nearDF.count()}")
    println(s"近距商家平均配送费: ${f"$nearAvgFee%.2f"}元")
    println(s"近距商家配送费中位数: ${f"$nearMedianFee%.2f"}元")
    println(s"\n远距商家（>2km）数量: ${farDF.count()}")
    println(s"远距商家平均配送费: ${f"$farAvgFee%.2f"}元")
    println(s"远距商家配送费中位数: ${f"$farMedianFee%.2f"}元")

    val diff = farAvgFee - nearAvgFee

    if (diff > 1.0) {
      println(s"\n结论：距离越远，配送费越高  (+${f"$diff%.2f"}元)")
    } else if (diff > 0) {
      println(s"\n结论：距离越远，配送费略高 (+${f"$diff%.2f"}元)")
    } else {
      println(s"\n结论：配送费与距离关系不明显")
    }

    // ========== 分析5：配送费随距离的增长率 ==========
    println("\n========== 配送费随距离增长率分析 ==========")

    // 计算每公里配送费增量
    val feeGrowth = validDF
      .withColumn("distance_km", col("distance") / 1000)
      .groupBy(ceil(col("distance_km")).alias("dist_km"))
      .agg(
        avg("delivery_fee").alias("avg_fee"),
        count("shop_name").alias("shop_count")
      )
      .filter(col("dist_km") <= 10)
      .orderBy("dist_km")

    println("每公里区间的平均配送费:")
    feeGrowth.show()

    // 计算增长率
    val baseFee = feeGrowth.filter(col("dist_km") === 1).select("avg_fee").collect()(0)(0).asInstanceOf[Double]
    println(s"\n1km以内平均配送费: ${f"$baseFee%.2f"}元")

    feeGrowth.withColumn("fee_increase",
        when(col("dist_km") > 1, round(col("avg_fee") - baseFee, 2))
          .otherwise(0))
      .withColumn("increase_per_km",
        when(col("dist_km") > 1, round((col("avg_fee") - baseFee) / (col("dist_km") - 1), 2))
          .otherwise(0))
      .show()

    // ========== 分析6：不同品类的距离-配送费关系 ==========
    println("\n========== 不同品类的距离-配送费关系 ==========")

    val categoryFeeDistance = validDF
      .groupBy("category_clean")
      .agg(
        avg("delivery_fee").alias("avg_fee"),
        avg("distance").alias("avg_distance"),
        count("shop_name").alias("shop_count"),
        corr("distance", "delivery_fee").alias("correlation")
      )
      .filter(col("shop_count") >= 10)
      .orderBy(desc("correlation"))

    println("各品类的距离-配送费相关系数:")
    categoryFeeDistance.show(20)

    // 找出距离影响最明显的品类
    println("\n距离对配送费影响最明显的品类（正相关最强）:")
    categoryFeeDistance
      .filter(col("correlation") > 0.3)
      .select("category_clean", "avg_distance", "avg_fee", "correlation")
      .show(10)

    // ========== 分析7：免配送费商家的距离特征 ==========
    println("\n========== 免配送费商家的距离特征 ==========")

    val freeFeeDF = validDF.filter(col("delivery_fee") === 0)
    val paidFeeDF = validDF.filter(col("delivery_fee") > 0)

    val freeAvgDistance = freeFeeDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
    val paidAvgDistance = paidFeeDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
    val freeNearRatio = freeFeeDF.filter(col("distance") <= 1000).count().toDouble / freeFeeDF.count() * 100
    val paidNearRatio = paidFeeDF.filter(col("distance") <= 1000).count().toDouble / paidFeeDF.count() * 100

    println(s"免配送费商家平均距离: ${f"$freeAvgDistance%.0f"}米")
    println(s"免配送费商家近距占比(≤1km): ${f"$freeNearRatio%.1f"}%")
    println(s"\n收费配送商家平均距离: ${f"$paidAvgDistance%.0f"}米")
    println(s"收费配送商家近距占比(≤1km): ${f"$paidNearRatio%.1f"}%")

    // ========== 分析8：配送费-距离趋势线数据 ==========
    println("\n========== 配送费-距离趋势线数据 ==========")

    // 按500米间隔计算平均配送费
    val trendData = validDF
      .withColumn("dist_interval",
        when(col("distance") <= 500, 500)
          .when(col("distance") <= 1000, 1000)
          .when(col("distance") <= 1500, 1500)
          .when(col("distance") <= 2000, 2000)
          .when(col("distance") <= 2500, 2500)
          .when(col("distance") <= 3000, 3000)
          .otherwise(3500)
      )
      .groupBy("dist_interval")
      .agg(
        avg("delivery_fee").alias("avg_fee"),
        count("shop_name").alias("shop_count")
      )
      .orderBy("dist_interval")

    println("配送费-距离趋势（每500米间隔）:")
    trendData.show()

    // ========== 分析9：不同评分区间的距离敏感度 ==========
    println("\n========== 不同评分区间的距离-配送费关系 ==========")

    val ratingFeeDistance = validDF
      .withColumn("rating_group",
        when(col("rating") < 4.0, "低评分(<4.0)")
          .when(col("rating") < 4.5, "中评分(4.0-4.5)")
          .otherwise("高评分(≥4.5)")
      )
      .groupBy("rating_group")
      .agg(
        corr("distance", "delivery_fee").alias("correlation"),
        avg("distance").alias("avg_distance"),
        avg("delivery_fee").alias("avg_fee")
      )

    println("不同评分区间的距离-配送费相关系数:")
    ratingFeeDistance.show()

    // ========== 分析10：距离-配送费分段模型 ==========
    println("\n========== 距离-配送费分段模型 ==========")

    // 计算各距离段的配送费增量
    val segmentModel = validDF
      .withColumn("dist_segment",
        when(col("distance") <= 1000, "0-1km")
          .when(col("distance") <= 2000, "1-2km")
          .when(col("distance") <= 3000, "2-3km")
          .otherwise("3km以上")
      )
      .groupBy("dist_segment")
      .agg(
        avg("delivery_fee").alias("avg_fee"),
        count("shop_name").alias("shop_count"),
        stddev("delivery_fee").alias("fee_stddev")
      )
      .orderBy("dist_segment")

    println("距离分段配送费模型:")
    segmentModel.show()

    // 计算每增加1km的配送费增量
    val segmentData = segmentModel.collect()
    if (segmentData.length >= 2) {
      println("\n配送费增量分析:")
      for (i <- 1 until segmentData.length) {
        val prevFee = segmentData(i-1).getDouble(1)
        val currFee = segmentData(i).getDouble(1)
        val increase = currFee - prevFee
        println(s"  ${segmentData(i-1).getString(0)} → ${segmentData(i).getString(0)}: +${f"$increase%.2f"}元")
      }
    }

    println("配送费距离分析完成")
  }

  /**
   * 独立运行时的main方法
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FeeDistanceAnalysis")
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