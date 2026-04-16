package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SalesDistanceAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SalesDistanceAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（月售>0，距离>0）
      val validDF = cleanedDF
        .filter(col("monthly_sales") > 0)
        .filter(col("distance") > 0)

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：月售与距离的整体相关性 ==========
      println("\n========== 月售与距离相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("monthly_sales", "distance")
      println(s"月售与距离的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.3) {
        println("  强负相关：距离越近，月售越高 ")
      } else if (correlation < -0.15) {
        println("  中等负相关：距离对月售有明显负面影响")
      } else if (correlation < 0) {
        println("  弱负相关：距离与月售有一定负向关系")
      } else if (correlation > 0.15) {
        println("  正相关：距离越远，月售越高 ️")
      } else {
        println("  弱相关：距离与月售关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("monthly_sales"),
          col("distance"),
          col("shop_name"),
          col("category_clean").alias("category"),
          col("rating")
        )
        .orderBy(col("distance"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "sales_distance_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 sales_distance_scatter 表")

      // ========== 分析3：距离分组月售统计 ==========
      println("\n========== 距离分组月售统计 ==========")

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

      // 定义距离分组（根据你的数据特点调整）
      val dfWithGroup = validDF
        .withColumn("distance_group",
          when(col("distance") <= 500, "极近(≤500m)")
            .when(col("distance") <= 1000, "近距(500-1000m)")
            .when(col("distance") <= 1500, "中距(1000-1500m)")
            .when(col("distance") <= 2000, "中远距(1500-2000m)")
            .otherwise("远距(>2000m)")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("distance_group")
        .agg(
          count("shop_name").alias("shop_count"),
          min("distance").alias("min_distance"),
          max("distance").alias("max_distance"),
          sum("monthly_sales").alias("total_sales"),
          avg("monthly_sales").alias("avg_sales"),
          expr("percentile_approx(monthly_sales, 0.50)").alias("median_sales")
        )
        .select(
          col("distance_group"),
          col("min_distance"),
          col("max_distance"),
          col("shop_count"),
          round(col("avg_sales"), 0).alias("avg_sales"),
          col("median_sales"),
          col("total_sales")
        )
        .orderBy(
          when(col("distance_group") === "极近(≤500m)", 1)
            .when(col("distance_group") === "近距(500-1000m)", 2)
            .when(col("distance_group") === "中距(1000-1500m)", 3)
            .when(col("distance_group") === "中远距(1500-2000m)", 4)
            .otherwise(5)
        )

      println("\n距离分组月售统计:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "distance_sales_group", MySQLUtils.getProperties())

      println(" 距离分组月售统计已写入 distance_sales_group 表")

      // ========== 分析4：近距 vs 远距对比 ==========
      println("\n========== 近距 vs 远距对比 ==========")

      val nearDistanceDF = validDF.filter(col("distance") <= 1000)
      val farDistanceDF = validDF.filter(col("distance") > 2000)

      val nearAvgSales = nearDistanceDF.select(avg("monthly_sales")).collect()(0).getDouble(0)
      val farAvgSales = farDistanceDF.select(avg("monthly_sales")).collect()(0).getDouble(0)

      val nearMedianSales = nearDistanceDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      val farMedianSales = farDistanceDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      println(s"近距商家（≤1km）数量: ${nearDistanceDF.count()}")
      println(s"近距商家平均月售: ${f"$nearAvgSales%.0f"}单")
      println(s"近距商家月售中位数: ${f"$nearMedianSales%.0f"}单")
      println(s"\n远距商家（>2km）数量: ${farDistanceDF.count()}")
      println(s"远距商家平均月售: ${f"$farAvgSales%.0f"}单")
      println(s"远距商家月售中位数: ${f"$farMedianSales%.0f"}单")

      val diff = nearAvgSales - farAvgSales

      if (diff > 300) {
        println(s"\n结论：距离学校越近的商家，月售显著越高 ✅ (+${f"$diff%.0f"}单)")
        } else if (diff > 0) {
          println(s"\n结论：距离学校越近的商家，月售略高 (+${f"$diff%.0f"}单)")
        } else {
          println(s"\n结论：距离与月售关系不明显")
      }

      // ========== 分析5：高销量商家的距离特征 ==========
      println("\n========== 高销量商家的距离特征 ==========")

      val highSalesDF = validDF.filter(col("monthly_sales") > 1500)
      val lowSalesDF = validDF.filter(col("monthly_sales") <= 300)

      val highSalesAvgDist = highSalesDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
      val lowSalesAvgDist = lowSalesDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
      val highSalesNearRatio = highSalesDF.filter(col("distance") <= 1000).count().toDouble / highSalesDF.count() * 100
      val lowSalesNearRatio = lowSalesDF.filter(col("distance") <= 1000).count().toDouble / lowSalesDF.count() * 100

      println(s"高销量商家（>1500单）平均距离: ${f"$highSalesAvgDist%.0f"}米")
      println(s"高销量商家（>1500单）近距占比(≤1km): ${f"$highSalesNearRatio%.1f"}%")
        println(s"\n低销量商家（≤300单）平均距离: ${f"$lowSalesAvgDist%.0f"}米")
        println(s"低销量商家（≤300单）近距占比(≤1km): ${f"$lowSalesNearRatio%.1f"}%")

          // ========== 分析6：不同距离区间的商家数量分布 ==========
          println("\n========== 不同距离区间的商家数量分布 ==========")

          val distanceDistribution = validDF
            .withColumn("dist_bucket",
              when(col("distance") <= 500, "0-500m")
                .when(col("distance") <= 1000, "500-1000m")
                .when(col("distance") <= 1500, "1000-1500m")
                .when(col("distance") <= 2000, "1500-2000m")
                .when(col("distance") <= 3000, "2000-3000m")
                .otherwise(">3000m")
            )
            .groupBy("dist_bucket")
            .agg(
              count("shop_name").alias("shop_count"),
              avg("monthly_sales").alias("avg_sales")
            )
            .orderBy("dist_bucket")

          println("距离区间商家分布:")
          distanceDistribution.show()

          // ========== 分析7：按品类细分 ==========
          println("\n========== 不同品类的距离-月售关系 ==========")

          val categoryDistanceSales = validDF
            .groupBy("category_clean")
            .agg(
              avg("distance").alias("avg_distance"),
              avg("monthly_sales").alias("avg_sales"),
              count("shop_name").alias("shop_count"),
              corr("distance", "monthly_sales").alias("correlation")
            )
            .filter(col("shop_count") >= 10)
            .orderBy("correlation")

          println("各品类的距离-月售相关系数:")
          categoryDistanceSales.show(20)

          // 找出距离影响最明显的品类
          println("\n距离对月售影响最明显的品类（负相关最强）:")
          categoryDistanceSales
            .filter(col("correlation") < -0.2)
            .select("category_clean", "avg_distance", "avg_sales", "correlation")
            .show(10)

          // ========== 分析8：距离与月售的趋势线数据 ==========
          println("\n========== 距离-月售趋势 ==========")

          // 按500米间隔计算平均月售
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
              avg("monthly_sales").alias("avg_sales"),
              count("shop_name").alias("shop_count")
            )
            .orderBy("dist_interval")

          println("距离-月售趋势（每500米间隔）:")
          trendData.show()

          // ========== 分析9：不同评分区间的距离敏感度 ==========
          println("\n========== 不同评分区间的距离敏感度 ==========")

          val ratingDistanceSensitivity = validDF
            .withColumn("rating_group",
              when(col("rating") < 4.0, "低评分(<4.0)")
                .when(col("rating") < 4.5, "中评分(4.0-4.5)")
                .otherwise("高评分(≥4.5)")
            )
            .groupBy("rating_group")
            .agg(
              corr("distance", "monthly_sales").alias("correlation"),
              avg("distance").alias("avg_distance"),
              avg("monthly_sales").alias("avg_sales")
            )

          println("不同评分区间的距离敏感度:")
          ratingDistanceSensitivity.show()

          // ========== 分析10：距离衰减效应 ==========
          println("\n========== 距离衰减效应分析 ==========")

          // 计算距离每增加500米，月售下降的比例
          val decayData = validDF
            .withColumn("dist_bucket",
              when(col("distance") <= 500, "0-500m")
                .when(col("distance") <= 1000, "500-1000m")
                .when(col("distance") <= 1500, "1000-1500m")
                .when(col("distance") <= 2000, "1500-2000m")
                .otherwise(">2000m")
            )
            .groupBy("dist_bucket")
            .agg(avg("monthly_sales").alias("avg_sales"))
            .orderBy("dist_bucket")

          val baseline = decayData.filter(col("dist_bucket") === "0-500m").select("avg_sales").collect()(0)(0).asInstanceOf[Double]

          println("距离衰减效应（以0-500m为基准）:")
      val decayResult = decayData.withColumn(
        "decay_rate",
        round((lit(baseline) - col("avg_sales")) / lit(baseline) * 100, 1)
      )

      // .show() 现在可以用了
      decayResult.show()

    } finally {
      spark.stop()
    }
  }
}
