package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SalesDeliveryTimeAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SalesDeliveryTimeAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（月售>0，送达时间>0）
      val validDF = cleanedDF
        .filter(col("monthly_sales") > 0)
        .filter(col("delivery_time") > 0)

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：月售与送达时间的整体相关性 ==========
      println("\n========== 月售与送达时间相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("monthly_sales", "delivery_time")
      println(s"月售与送达时间的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.25) {
        println("  负相关：送达时间越短，月售越高 ")
      } else if (correlation > 0.25) {
        println("  正相关：送达时间越长，月售越高 ")
      } else if (correlation < 0) {
        println("  弱负相关：送达时间与月售有一定负向关系")
      } else {
        println("  弱相关：送达时间与月售关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("monthly_sales"),
          col("delivery_time"),
          col("shop_name"),
          col("category_clean").alias("category"),
          col("rating")
        )
        .orderBy(col("delivery_time"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "sales_delivery_time_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 sales_delivery_time_scatter 表")

      // ========== 分析3：送达时间分组月售统计 ==========
      println("\n========== 送达时间分组月售统计 ==========")

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
          sum("monthly_sales").alias("total_sales"),
          avg("monthly_sales").alias("avg_sales"),
          expr("percentile_approx(monthly_sales, 0.50)").alias("median_sales")
        )
        .select(
          col("time_group"),
          col("min_time"),
          col("max_time"),
          col("shop_count"),
          round(col("avg_sales"), 0).alias("avg_sales"),
          col("median_sales"),
          col("total_sales")
        )
        .orderBy(
          when(col("time_group") === "≤20分钟", 1)
            .when(col("time_group") === "20-25分钟", 2)
            .when(col("time_group") === "25-30分钟", 3)
            .when(col("time_group") === "30-35分钟", 4)
            .when(col("time_group") === "35-40分钟", 5)
            .otherwise(6)
        )

      println("\n送达时间分组月售统计:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "delivery_time_sales_group", MySQLUtils.getProperties())

      println(" 送达时间分组月售统计已写入 delivery_time_sales_group 表")

      // ========== 分析4：快送 vs 慢送对比 ==========
      println("\n========== 快送 vs 慢送对比 ==========")

      val fastDeliveryDF = validDF.filter(col("delivery_time") <= 25)
      val slowDeliveryDF = validDF.filter(col("delivery_time") > 35)

      val fastAvgSales = fastDeliveryDF.select(avg("monthly_sales")).collect()(0).getDouble(0)
      val slowAvgSales = slowDeliveryDF.select(avg("monthly_sales")).collect()(0).getDouble(0)

      // 使用 cast 确保返回 Double
      val fastMedianSales = fastDeliveryDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      val slowMedianSales = slowDeliveryDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      println(s"快送商家（≤25分钟）数量: ${fastDeliveryDF.count()}")
      println(s"快送商家平均月售: ${f"$fastAvgSales%.0f"}单")
      println(s"快送商家月售中位数: ${f"$fastMedianSales%.0f"}单")
      println(s"\n慢送商家（>35分钟）数量: ${slowDeliveryDF.count()}")
      println(s"慢送商家平均月售: ${f"$slowAvgSales%.0f"}单")
      println(s"慢送商家月售中位数: ${f"$slowMedianSales%.0f"}单")

      val diff = fastAvgSales - slowAvgSales

      if (diff > 300) {
        println(s"\n结论：送达时间越短的商家月售显著越高  (+${f"$diff%.0f"}单)")
        } else if (diff > 0) {
          println(s"\n结论：送达时间越短的商家月售略高 (+${f"$diff%.0f"}单)")
        } else {
          println(s"\n结论：送达时间与月售关系不明显")
      }

      // ========== 分析5：高销量商家的送达时间特征 ==========
      println("\n========== 高销量商家的送达时间特征 ==========")

      val highSalesDF = validDF.filter(col("monthly_sales") > 1500)
      val lowSalesDF = validDF.filter(col("monthly_sales") <= 300)

      val highSalesAvgTime = highSalesDF.select(avg("delivery_time")).collect()(0)(0).asInstanceOf[Double]
      val lowSalesAvgTime = lowSalesDF.select(avg("delivery_time")).collect()(0)(0).asInstanceOf[Double]
      val highSalesFastRatio = highSalesDF.filter(col("delivery_time") <= 25).count().toDouble / highSalesDF.count() * 100
      val lowSalesFastRatio = lowSalesDF.filter(col("delivery_time") <= 25).count().toDouble / lowSalesDF.count() * 100

      println(s"高销量商家（>1500单）平均送达时间: ${f"$highSalesAvgTime%.1f"}分钟")
      println(s"高销量商家（>1500单）快送占比(≤25min): ${f"$highSalesFastRatio%.1f"}%")
        println(s"\n低销量商家（≤300单）平均送达时间: ${f"$lowSalesAvgTime%.1f"}分钟")
        println(s"低销量商家（≤300单）快送占比(≤25min): ${f"$lowSalesFastRatio%.1f"}%")

          // ========== 分析6：按品类细分 ==========
          println("\n========== 不同品类的送达时间-月售关系 ==========")

          val categoryTimeSales = validDF
            .groupBy("category_clean")
            .agg(
              avg("delivery_time").alias("avg_delivery_time"),
              avg("monthly_sales").alias("avg_sales"),
              count("shop_name").alias("shop_count"),
              corr("delivery_time", "monthly_sales").alias("correlation")
            )
            .filter(col("shop_count") >= 10)
            .orderBy("correlation")

          println("各品类的送达时间-月售相关系数:")
          categoryTimeSales.show(20)

          // 找出送达时间影响最明显的品类
          println("\n送达时间对月售影响最明显的品类（负相关最强）:")
          categoryTimeSales
            .filter(col("correlation") < -0.2)
            .select("category_clean", "avg_delivery_time", "avg_sales", "correlation")
            .show(10)

          // ========== 分析7：送达时间与月售的趋势线数据 ==========
          println("\n========== 送达时间-月售趋势 ==========")

          // 按5分钟间隔计算平均月售
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
              avg("monthly_sales").alias("avg_sales"),
              count("shop_name").alias("shop_count")
            )
            .orderBy("time_interval")

          println("送达时间-月售趋势（每5分钟间隔）:")
          trendData.show()

          // ========== 分析8：不同评分区间的送达时间敏感度 ==========
          println("\n========== 不同评分区间的送达时间敏感度 ==========")

          val ratingTimeSensitivity = validDF
            .withColumn("rating_group",
              when(col("rating") < 4.0, "低评分(<4.0)")
                .when(col("rating") < 4.5, "中评分(4.0-4.5)")
                .otherwise("高评分(≥4.5)")
            )
            .groupBy("rating_group")
            .agg(
              corr("delivery_time", "monthly_sales").alias("correlation"),
              avg("delivery_time").alias("avg_time"),
              avg("monthly_sales").alias("avg_sales")
            )

          println("不同评分区间的送达时间敏感度:")
          ratingTimeSensitivity.show()

          // ========== 分析9：送达时间与评分的关系（交叉验证） ==========
          println("\n========== 送达时间与评分的交叉分析 ==========")

          val timeRatingCross = validDF
            .withColumn("time_group",
              when(col("delivery_time") <= 25, "快送(≤25min)")
                .when(col("delivery_time") <= 35, "中速(25-35min)")
                .otherwise("慢送(>35min)")
            )
            .withColumn("rating_group",
              when(col("rating") < 4.0, "低评分")
                .when(col("rating") < 4.5, "中评分")
                .otherwise("高评分")
            )
            .groupBy("time_group", "rating_group")
            .agg(
              count("shop_name").alias("shop_count"),
              avg("monthly_sales").alias("avg_sales")
            )
            .orderBy("time_group", "rating_group")

          println("送达时间与评分的交叉分析:")
          timeRatingCross.show()

    } finally {
      spark.stop()
    }
  }
}
