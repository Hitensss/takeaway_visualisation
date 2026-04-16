package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DeliveryTimeRatingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeliveryTimeRatingAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（送达时间>0且评分>0）
      val validDF = cleanedDF
        .filter(col("delivery_time") > 0)
        .filter(col("rating") > 0)

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：送达时间与评分的整体相关性 ==========
      println("\n========== 送达时间与评分相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("delivery_time", "rating")
      println(s"送达时间与评分的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.3) {
        println("  强负相关：送达时间越短，评分越高 ")
      } else if (correlation < -0.1) {
        println("  中等负相关：送达时间对评分有明显负面影响")
      } else if (correlation < 0) {
        println("  弱负相关：送达时间与评分有一定负向关系")
      } else if (correlation > 0.1) {
        println("  正相关：送达时间越长，评分越高 ")
      } else {
        println("  弱相关：送达时间与评分关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("delivery_time"),
          col("rating"),
          col("shop_name"),
          col("category_clean").alias("category")
        )
        .orderBy(col("delivery_time"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "delivery_time_rating_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 delivery_time_rating_scatter 表")

      // ========== 分析3：送达时间分组统计 ==========
      println("\n========== 送达时间分组统计 ==========")

      // 计算送达时间的分位数
      val percentiles = validDF
        .selectExpr(
          "percentile_approx(delivery_time, 0.25) as p25",
          "percentile_approx(delivery_time, 0.50) as p50",
          "percentile_approx(delivery_time, 0.75) as p75"
        )
        .collect()(0)

      val p25 = percentiles.getInt(0)
      val p50 = percentiles.getInt(1)
      val p75 = percentiles.getInt(2)

      println(s"送达时间分位数:")
      println(s"  25%分位数: ${p25}分钟")
      println(s"  50%分位数: ${p50}分钟")
      println(s"  75%分位数: ${p75}分钟")

      // 定义送达时间分组
      val dfWithGroup = validDF
        .withColumn("time_group",
          when(col("delivery_time") <= 20, "≤20分钟")
            .when(col("delivery_time") <= 25, "20-25分钟")
            .when(col("delivery_time") <= 30, "25-30分钟")
            .when(col("delivery_time") <= 35, "30-35分钟")
            .when(col("delivery_time") <= 40, "35-40分钟")
            .otherwise(">40分钟")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("time_group")
        .agg(
          count("shop_name").alias("shop_count"),
          min("delivery_time").alias("min_time"),
          max("delivery_time").alias("max_time"),
          avg("rating").alias("avg_rating"),
          expr("percentile_approx(rating, 0.50)").alias("median_rating"),
          sum(when(col("rating") >= 4.5, 1).otherwise(0)).alias("high_rating_count")
        )
        .withColumn("high_rating_ratio",
          round(col("high_rating_count") / col("shop_count") * 100, 2))
        .select(
          col("time_group"),
          col("min_time"),
          col("max_time"),
          col("shop_count"),
          round(col("avg_rating"), 2).alias("avg_rating"),
          col("median_rating"),
          col("high_rating_ratio")
        )
        .orderBy(
          when(col("time_group") === "≤20分钟", 1)
            .when(col("time_group") === "20-25分钟", 2)
            .when(col("time_group") === "25-30分钟", 3)
            .when(col("time_group") === "30-35分钟", 4)
            .when(col("time_group") === "35-40分钟", 5)
            .otherwise(6)
        )

      println("\n分组统计结果:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "delivery_time_rating_group", MySQLUtils.getProperties())

      println(" 分组统计已写入 delivery_time_rating_group 表")

      // ========== 分析4：快送 vs 慢送对比 ==========
      println("\n========== 快送 vs 慢送对比 ==========")

      val fastDeliveryDF = validDF.filter(col("delivery_time") <= 25)
      val slowDeliveryDF = validDF.filter(col("delivery_time") > 35)

      val fastAvgRating = fastDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
      val slowAvgRating = slowDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]

      println(s"快送商家（≤25分钟）数量: ${fastDeliveryDF.count()}")
      println(s"快送商家平均评分: ${f"$fastAvgRating%.2f"}")
      println(s"\n慢送商家（>35分钟）数量: ${slowDeliveryDF.count()}")
      println(s"慢送商家平均评分: ${f"$slowAvgRating%.2f"}")

      val diff = fastAvgRating - slowAvgRating

      if (diff > 0.2) {
        println(s"\n结论：送达时间越短的商家评分显著越高  (+${f"$diff%.2f"})")
      } else if (diff > 0) {
        println(s"\n结论：送达时间越短的商家评分略高 (+${f"$diff%.2f"})")
      } else if (diff < -0.2) {
        println(s"\n结论：送达时间越短的商家评分反而更低  (${f"$diff%.2f"})")
      } else if (diff < 0) {
        println(s"\n结论：送达时间越短的商家评分略低 (${f"$diff%.2f"})")
      } else {
        println(s"\n结论：送达时间与评分无明显关系")
      }

      // ========== 分析5：按品类细分 ==========
      println("\n========== 不同品类的送达时间-评分关系 ==========")

      val categoryTimeRating = validDF
        .groupBy("category_clean")
        .agg(
          avg("delivery_time").alias("avg_delivery_time"),
          avg("rating").alias("avg_rating"),
          count("shop_name").alias("shop_count"),
          corr("delivery_time", "rating").alias("correlation")
        )
        .filter(col("shop_count") >= 10)
        .orderBy("correlation")

      println("各品类的送达时间-评分相关系数:")
      categoryTimeRating.show(20)

      // 找出送达时间影响最明显的品类
      println("\n送达时间对评分影响最明显的品类（负相关最强）:")
      categoryTimeRating
        .filter(col("correlation") < -0.2)
        .select("category_clean", "avg_delivery_time", "avg_rating", "correlation")
        .show(10)

      // ========== 分析6：高评分商家的送达时间特征 ==========
      println("\n========== 高评分商家的送达时间特征 ==========")

      // 确保 delivery_time 是 Double 类型
      val validDFWithDouble = validDF.withColumn("delivery_time", col("delivery_time").cast("double"))

      val highRatingDF = validDFWithDouble.filter(col("rating") >= 4.5)
      val lowRatingDF = validDFWithDouble.filter(col("rating") < 4.0)

      val highTimeStats = highRatingDF.select(
        avg("delivery_time").alias("avg_time"),
        expr("percentile_approx(delivery_time, 0.5)").alias("median_time")
      ).collect()(0)

      val lowTimeStats = lowRatingDF.select(
        avg("delivery_time").alias("avg_time"),
        expr("percentile_approx(delivery_time, 0.5)").alias("median_time")
      ).collect()(0)

      println(s"高评分商家（≥4.5分）平均送达时间: ${f"${highTimeStats.getAs[Double](0)}%.1f"}分钟")
      println(s"高评分商家（≥4.5分）送达时间中位数: ${f"${highTimeStats.getAs[Double](1)}%.1f"}分钟")
      println(s"\n低评分商家（<4.0分）平均送达时间: ${f"${lowTimeStats.getAs[Double](0)}%.1f"}分钟")
      println(s"低评分商家（<4.0分）送达时间中位数: ${f"${lowTimeStats.getAs[Double](1)}%.1f"}分钟")

      // ========== 分析7：送达时间分布 ==========
      println("\n========== 送达时间分布 ==========")

      val timeDistribution = validDF
        .withColumn("time_bucket",
          when(col("delivery_time") <= 15, "≤15分钟")
            .when(col("delivery_time") <= 20, "15-20分钟")
            .when(col("delivery_time") <= 25, "20-25分钟")
            .when(col("delivery_time") <= 30, "25-30分钟")
            .when(col("delivery_time") <= 35, "30-35分钟")
            .when(col("delivery_time") <= 40, "35-40分钟")
            .otherwise(">40分钟")
        )
        .groupBy("time_bucket")
        .agg(count("shop_name").alias("shop_count"))
        .orderBy("time_bucket")

      println("送达时间分布:")
      timeDistribution.show()

      // ========== 分析8：送达时间与评分的关系曲线 ==========
      println("\n========== 送达时间-评分趋势 ==========")

      // 按5分钟间隔计算平均评分
      val trendData = validDF
        .withColumn("time_interval",
          when(col("delivery_time") <= 15, 15)
            .when(col("delivery_time") <= 20, 20)
            .when(col("delivery_time") <= 25, 25)
            .when(col("delivery_time") <= 30, 30)
            .when(col("delivery_time") <= 35, 35)
            .when(col("delivery_time") <= 40, 40)
            .otherwise(45)
        )
        .groupBy("time_interval")
        .agg(
          avg("rating").alias("avg_rating"),
          count("shop_name").alias("shop_count")
        )
        .orderBy("time_interval")

      println("送达时间-评分趋势（每5分钟间隔）:")
      trendData.show()

    } finally {
      spark.stop()
    }
  }
}
