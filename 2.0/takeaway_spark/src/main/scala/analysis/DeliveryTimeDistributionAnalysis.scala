package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.MySQLUtils

object DeliveryTimeDistributionAnalysis {

  /**
   * 供RunAllAnalysis调用的分析方法
   */
  def analyze(spark: SparkSession, cleanedDF: DataFrame): Unit = {
    import spark.implicits._
    println("开始执行送达时间分布分析...")

    // 过滤有效数据（送达时间>0）
    val validDF = cleanedDF.filter(col("delivery_time") > 0)
    println(s"有效数据量: ${validDF.count()}")

    // ========== 分析1：送达时间统计摘要 ==========
    println("\n========== 送达时间统计摘要 ==========")

    val timeStats = validDF.select(
      avg("delivery_time").alias("avg_time"),
      stddev("delivery_time").alias("stddev_time"),
      min("delivery_time").alias("min_time"),
      max("delivery_time").alias("max_time"),
      expr("percentile_approx(delivery_time, 0.25)").alias("p25"),
      expr("percentile_approx(delivery_time, 0.50)").alias("median_time"),
      expr("percentile_approx(delivery_time, 0.75)").alias("p75"),
      expr("percentile_approx(delivery_time, 0.90)").alias("p90")
    ).collect()(0)

    val avgTime = timeStats.getDouble(0)
    val stddevTime = timeStats.getDouble(1)
    val minTime = timeStats.getInt(2)
    val maxTime = timeStats.getInt(3)
    val p25 = timeStats.getInt(4).toDouble
    val medianTime = timeStats.getInt(5).toDouble
    val p75 = timeStats.getInt(6).toDouble
    val p90 = timeStats.getInt(7).toDouble

    println(s"店铺总数: ${validDF.count()}")
    println(s"平均送达时间: ${f"$avgTime%.1f"}分钟")
    println(s"送达时间标准差: ${f"$stddevTime%.1f"}分钟")
    println(s"最快送达: ${minTime}分钟")
    println(s"最慢送达: ${maxTime}分钟")
    println(s"25%分位数: ${f"$p25%.0f"}分钟")
    println(s"中位数送达时间: ${f"$medianTime%.0f"}分钟")
    println(s"75%分位数: ${f"$p75%.0f"}分钟")
    println(s"90%分位数: ${f"$p90%.0f"}分钟")

    // 快送商家统计（≤25分钟）
    val fastDeliveryCount = validDF.filter(col("delivery_time") <= 25).count()
    val fastDeliveryRatio = fastDeliveryCount.toDouble / validDF.count() * 100
    println(s"\n快送商家（≤25分钟）数量: ${fastDeliveryCount} (占比 ${f"$fastDeliveryRatio%.1f"}%)")

    // 慢送商家统计（>40分钟）
    val slowDeliveryCount = validDF.filter(col("delivery_time") > 40).count()
    val slowDeliveryRatio = slowDeliveryCount.toDouble / validDF.count() * 100
    println(s"慢送商家（>40分钟）数量: ${slowDeliveryCount} (占比 ${f"$slowDeliveryRatio%.1f"}%)")

    // 写入统计摘要表
    val summaryData = Seq(
      ("店铺总数", validDF.count().toDouble),
      ("平均送达时间", avgTime),
      ("送达时间标准差", stddevTime),
      ("最快送达", minTime.toDouble),
      ("最慢送达", maxTime.toDouble),
      ("25%分位数", p25),
      ("中位数送达时间", medianTime),
      ("75%分位数", p75),
      ("90%分位数", p90),
      ("快送商家占比", fastDeliveryRatio)
    ).toDF("metric_name", "metric_value")

    summaryData.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "delivery_time_summary", MySQLUtils.getProperties())

    println("送达时间统计摘要已写入 delivery_time_summary 表")

    // ========== 分析2：送达时间分布（直方图数据） ==========
    println("\n========== 送达时间分布统计 ==========")

    // 定义送达时间区间
    val dfWithBucket = validDF
      .withColumn("time_bucket",
        when(col("delivery_time") <= 15, "≤15分钟")
          .when(col("delivery_time") <= 20, "15-20分钟")
          .when(col("delivery_time") <= 25, "20-25分钟")
          .when(col("delivery_time") <= 30, "25-30分钟")
          .when(col("delivery_time") <= 35, "30-35分钟")
          .when(col("delivery_time") <= 40, "35-40分钟")
          .when(col("delivery_time") <= 45, "40-45分钟")
          .when(col("delivery_time") <= 50, "45-50分钟")
          .otherwise("50分钟以上")
      )

    // 分组统计
    val distribution = dfWithBucket.groupBy("time_bucket")
      .agg(
        count("shop_name").alias("shop_count"),
        min("delivery_time").alias("min_time"),
        max("delivery_time").alias("max_time")
      )
      .withColumn("percentage",
        round(col("shop_count") / sum("shop_count").over() * 100, 2))
      .withColumn("cumulative_percentage",
        round(sum("percentage").over(Window.orderBy("time_bucket")), 2))
      .orderBy(
        when(col("time_bucket") === "≤15分钟", 1)
          .when(col("time_bucket") === "15-20分钟", 2)
          .when(col("time_bucket") === "20-25分钟", 3)
          .when(col("time_bucket") === "25-30分钟", 4)
          .when(col("time_bucket") === "30-35分钟", 5)
          .when(col("time_bucket") === "35-40分钟", 6)
          .when(col("time_bucket") === "40-45分钟", 7)
          .when(col("time_bucket") === "45-50分钟", 8)
          .otherwise(9)
      )

    println("送达时间分布结果:")
    distribution.show()

    // 写入送达时间分布表
    distribution.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "delivery_time_distribution", MySQLUtils.getProperties())

    println("送达时间分布已写入 delivery_time_distribution 表")

    // ========== 分析3：各品类的送达时间分布 ==========
    println("\n========== 各品类送达时间分布 ==========")

    val categoryTimeDist = validDF
      .withColumn("time_level",
        when(col("delivery_time") <= 25, "快送(≤25min)")
          .when(col("delivery_time") <= 35, "中速(25-35min)")
          .otherwise("慢送(>35min)")
      )
      .groupBy("category_clean", "time_level")
      .agg(count("shop_name").alias("shop_count"))
      .withColumnRenamed("category_clean", "category")
      .orderBy("category", "time_level")

    println("各品类送达时间级别分布:")
    categoryTimeDist.show(50)

    // ========== 分析4：不同距离区间的送达时间分布 ==========
    println("\n========== 不同距离区间的送达时间分布 ==========")

    val distanceTimeDist = validDF
      .withColumn("distance_group",
        when(col("distance") <= 1000, "近距(≤1km)")
          .when(col("distance") <= 2000, "中距(1-2km)")
          .otherwise("远距(>2km)")
      )
      .withColumn("time_level",
        when(col("delivery_time") <= 25, "快送")
          .when(col("delivery_time") <= 35, "中速")
          .otherwise("慢送")
      )
      .groupBy("distance_group", "time_level")
      .agg(count("shop_name").alias("shop_count"))
      .orderBy("distance_group", "time_level")

    println("不同距离区间的送达时间分布:")
    distanceTimeDist.show()

    // ========== 分析5：送达时间与销量的关系 ==========
    println("\n========== 送达时间与销量分析 ==========")

    val timeSalesAnalysis = dfWithBucket
      .groupBy("time_bucket")
      .agg(
        count("shop_name").alias("shop_count"),
        sum("monthly_sales").alias("total_sales"),
        avg("monthly_sales").alias("avg_sales"),
        avg("rating").alias("avg_rating")
      )
      .orderBy("time_bucket")

    println("各送达时间区间的销量与评分:")
    timeSalesAnalysis.show()

    // ========== 分析6：不同品类的平均送达时间排名 ==========
    println("\n========== 各品类平均送达时间排名 ==========")

    val categoryAvgTime = validDF
      .groupBy("category_clean")
      .agg(
        avg("delivery_time").alias("avg_time"),
        count("shop_name").alias("shop_count"),
        stddev("delivery_time").alias("time_stddev")
      )
      .withColumnRenamed("category_clean", "category")
      .orderBy("avg_time")

    println("各品类平均送达时间（从快到慢）:")
    categoryAvgTime.show(20)

    // ========== 分析7：送达时间与距离的关系 ==========
    println("\n========== 送达时间与距离关系 ==========")

    val distanceCorrelation = validDF.stat.corr("distance", "delivery_time")
    println(s"距离与送达时间的相关系数: ${f"$distanceCorrelation%.4f"}")

    if (distanceCorrelation > 0.5) {
      println("结论：距离越远，送达时间越长 ")
    } else if (distanceCorrelation > 0.3) {
      println("结论：距离与送达时间有较强正相关")
    } else if (distanceCorrelation > 0) {
      println("结论：距离与送达时间有弱正相关")
    } else {
      println("结论：距离与送达时间关系不明显")
    }

    // ========== 分析8：快送商家特征 ==========
    println("\n========== 快送商家特征 ==========")

    val fastDeliveryDF = validDF.filter(col("delivery_time") <= 25)
    val slowDeliveryDF = validDF.filter(col("delivery_time") > 40)

    val fastAvgSales = fastDeliveryDF.select(avg("monthly_sales")).collect()(0)(0).asInstanceOf[Double]
    val slowAvgSales = slowDeliveryDF.select(avg("monthly_sales")).collect()(0)(0).asInstanceOf[Double]
    val fastAvgRating = fastDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
    val slowAvgRating = slowDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
    val fastAvgDistance = fastDeliveryDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
    val slowAvgDistance = slowDeliveryDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]

    println(s"快送商家（≤25分钟）数量: ${fastDeliveryDF.count()}")
    println(s"快送商家平均月售: ${f"$fastAvgSales%.0f"}单")
    println(s"快送商家平均评分: ${f"$fastAvgRating%.2f"}分")
    println(s"快送商家平均距离: ${f"$fastAvgDistance%.0f"}米")
    println(s"\n慢送商家（>40分钟）数量: ${slowDeliveryDF.count()}")
    println(s"慢送商家平均月售: ${f"$slowAvgSales%.0f"}单")
    println(s"慢送商家平均评分: ${f"$slowAvgRating%.2f"}分")
    println(s"慢送商家平均距离: ${f"$slowAvgDistance%.0f"}米")

    // ========== 分析9：送达时间金字塔 ==========
    println("\n========== 送达时间金字塔 ==========")

    val timePyramid = validDF
      .withColumn("time_tier",
        when(col("delivery_time") <= 20, "极速层(≤20min)")
          .when(col("delivery_time") <= 25, "快速层(20-25min)")
          .when(col("delivery_time") <= 30, "标准层(25-30min)")
          .when(col("delivery_time") <= 35, "中速层(30-35min)")
          .otherwise("慢速层(>35min)")
      )
      .groupBy("time_tier")
      .agg(
        count("shop_name").alias("shop_count"),
        avg("monthly_sales").alias("avg_sales"),
        avg("rating").alias("avg_rating")
      )
      .orderBy(
        when(col("time_tier") === "极速层(≤20min)", 1)
          .when(col("time_tier") === "快速层(20-25min)", 2)
          .when(col("time_tier") === "标准层(25-30min)", 3)
          .when(col("time_tier") === "中速层(30-35min)", 4)
          .otherwise(5)
      )

    println("送达时间金字塔:")
    timePyramid.show()

    // ========== 分析10：品类送达时间箱线图数据 ==========
    println("\n========== 品类送达时间箱线图数据 ==========")

    val timeBoxplot = validDF
      .groupBy("category_clean")
      .agg(
        count("delivery_time").alias("shop_count"),
        expr("percentile_approx(delivery_time, 0.00)").alias("min_time"),
        expr("percentile_approx(delivery_time, 0.25)").alias("q1_time"),
        expr("percentile_approx(delivery_time, 0.50)").alias("median_time"),
        expr("percentile_approx(delivery_time, 0.75)").alias("q3_time"),
        expr("percentile_approx(delivery_time, 1.00)").alias("max_time"),
        avg("delivery_time").alias("mean_time")
      )
      .filter(col("shop_count") >= 10)
      .withColumnRenamed("category_clean", "category")
      .orderBy("median_time")

    println("各品类送达时间箱线图数据:")
    timeBoxplot.show(20)

    println("送达时间分布分析完成")
  }

  /**
   * 独立运行时的main方法
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeliveryTimeDistributionAnalysis")
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