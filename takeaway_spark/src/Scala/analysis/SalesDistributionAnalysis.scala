package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SalesDistributionAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SalesDistributionAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（月售>0）
      val validDF = cleanedDF.filter(col("monthly_sales") > 0)
      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：月售统计摘要 ==========
      println("\n========== 月售统计摘要 ==========")

      val salesStats = validDF.select(
        avg("monthly_sales").alias("avg_sales"),
        stddev("monthly_sales").alias("stddev_sales"),
        min("monthly_sales").alias("min_sales"),
        max("monthly_sales").alias("max_sales"),
        expr("percentile_approx(monthly_sales, 0.25)").alias("p25"),
        expr("percentile_approx(monthly_sales, 0.50)").alias("median_sales"),
        expr("percentile_approx(monthly_sales, 0.75)").alias("p75"),
        expr("percentile_approx(monthly_sales, 0.90)").alias("p90")
      ).collect()(0)

      // 安全获取值的方法
      def safeGetDouble(value: Any): Double = value match {
        case d: Double => d
        case i: Int => i.toDouble
        case l: Long => l.toDouble
        case f: Float => f.toDouble
        case _ => 0.0
      }

      def safeGetInt(value: Any): Int = value match {
        case i: Int => i
        case d: Double => d.toInt
        case l: Long => l.toInt
        case _ => 0
      }

      val avgSales = safeGetDouble(salesStats(0))
      val stddevSales = safeGetDouble(salesStats(1))
      val minSales = safeGetInt(salesStats(2))
      val maxSales = safeGetInt(salesStats(3))
      val p25 = safeGetDouble(salesStats(4))
      val medianSales = safeGetDouble(salesStats(5))
      val p75 = safeGetDouble(salesStats(6))
      val p90 = safeGetDouble(salesStats(7))

      println(s"店铺总数: ${validDF.count()}")
      println(s"平均月售: ${f"$avgSales%.0f"}单")
      println(s"月售标准差: ${f"$stddevSales%.0f"}单")
      println(s"最低月售: ${minSales}单")
      println(s"最高月售: ${maxSales}单")
      println(s"25%分位数: ${f"$p25%.0f"}单")
      println(s"中位数月售: ${f"$medianSales%.0f"}单")
      println(s"75%分位数: ${f"$p75%.0f"}单")
      println(s"90%分位数: ${f"$p90%.0f"}单")

      // 写入统计摘要表
      val summaryData = Seq(
        ("店铺总数", validDF.count().toDouble),
        ("平均月售", avgSales),
        ("月售标准差", stddevSales),
        ("最低月售", minSales.toDouble),
        ("最高月售", maxSales.toDouble),
        ("25%分位数", p25),
        ("中位数月售", medianSales),
        ("75%分位数", p75),
        ("90%分位数", p90)
      ).toDF("metric_name", "metric_value")

      summaryData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "sales_summary", MySQLUtils.getProperties())

      println(" 月售统计摘要已写入 sales_summary 表")

      // ========== 分析2：月售分布（直方图数据） ==========
      println("\n========== 月售分布统计 ==========")

      // 定义月售区间（根据数据分布调整）
      val dfWithBucket = validDF
        .withColumn("sales_bucket",
          when(col("monthly_sales") <= 100, "0-100单")
            .when(col("monthly_sales") <= 300, "100-300单")
            .when(col("monthly_sales") <= 500, "300-500单")
            .when(col("monthly_sales") <= 1000, "500-1000单")
            .when(col("monthly_sales") <= 2000, "1000-2000单")
            .when(col("monthly_sales") <= 3000, "2000-3000单")
            .when(col("monthly_sales") <= 5000, "3000-5000单")
            .otherwise("5000单以上")
        )

      // 分组统计
      val distribution = dfWithBucket.groupBy("sales_bucket")
        .agg(
          count("shop_name").alias("shop_count"),
          min("monthly_sales").alias("min_sales"),
          max("monthly_sales").alias("max_sales")
        )
        .withColumn("percentage",
          round(col("shop_count") / sum("shop_count").over() * 100, 2))
        .withColumn("cumulative_percentage",
          round(sum("percentage").over(Window.orderBy("sales_bucket")), 2))
        .orderBy(
          when(col("sales_bucket") === "0-100单", 1)
            .when(col("sales_bucket") === "100-300单", 2)
            .when(col("sales_bucket") === "300-500单", 3)
            .when(col("sales_bucket") === "500-1000单", 4)
            .when(col("sales_bucket") === "1000-2000单", 5)
            .when(col("sales_bucket") === "2000-3000单", 6)
            .when(col("sales_bucket") === "3000-5000单", 7)
            .otherwise(8)
        )

      println("月售分布结果:")
      distribution.show()

      // 写入月售分布表
      distribution.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "sales_distribution", MySQLUtils.getProperties())

      println(" 月售分布已写入 sales_distribution 表")

      // ========== 分析3：各品类的月售分布 ==========
      println("\n========== 各品类月售分布 ==========")

      val categorySalesDist = validDF
        .withColumn("sales_level",
          when(col("monthly_sales") <= 500, "低销量（≤500单）")
            .when(col("monthly_sales") <= 1500, "中销量（500-1500单）")
            .otherwise("高销量（>1500单）")
        )
        .groupBy("category_clean", "sales_level")
        .agg(count("shop_name").alias("shop_count"))
        .withColumnRenamed("category_clean", "category")
        .orderBy("category", "sales_level")

      println("各品类销量级别分布:")
      categorySalesDist.show(50)

      // ========== 分析4：不同距离区间的月售分布 ==========
      println("\n========== 不同距离区间的月售分布 ==========")

      val distanceSalesDist = validDF
        .withColumn("distance_group",
          when(col("distance") <= 1000, "近距(≤1km)")
            .when(col("distance") <= 2000, "中距(1-2km)")
            .otherwise("远距(>2km)")
        )
        .withColumn("sales_level",
          when(col("monthly_sales") <= 500, "低销量")
            .when(col("monthly_sales") <= 1500, "中销量")
            .otherwise("高销量")
        )
        .groupBy("distance_group", "sales_level")
        .agg(count("shop_name").alias("shop_count"))
        .orderBy("distance_group", "sales_level")

      println("不同距离区间的销量分布:")
      distanceSalesDist.show()

      // ========== 分析5：头部商家分析 ==========
      println("\n========== 头部商家分析 ==========")

      // 计算总月售
      val totalSales = validDF.agg(sum("monthly_sales")).collect()(0)(0).asInstanceOf[Long]
      println(s"所有商家总月售: ${totalSales}单")

      // Top 10% 商家的销量占比
      val top10PercentCount = (validDF.count() * 0.1).toInt
      val top10PercentSales = validDF
        .orderBy(desc("monthly_sales"))
        .limit(top10PercentCount)
        .agg(sum("monthly_sales"))
        .collect()(0)(0).asInstanceOf[Long]

      val top10PercentRatio = top10PercentSales.toDouble / totalSales * 100
      println(s"Top 10% 商家（${top10PercentCount}家）销量占比: ${top10PercentRatio.formatted("%.1f")}%")

        // Top 20% 商家的销量占比
        val top20PercentCount = (validDF.count() * 0.2).toInt
        val top20PercentSales = validDF
          .orderBy(desc("monthly_sales"))
          .limit(top20PercentCount)
          .agg(sum("monthly_sales"))
          .collect()(0)(0).asInstanceOf[Long]

        val top20PercentRatio = top20PercentSales.toDouble / totalSales * 100
        println(s"Top 20% 商家（${top20PercentCount}家）销量占比: ${top20PercentRatio.formatted("%.1f")}%")

          // 帕累托分析（二八定律）
          println("\n帕累托分析（累计销量达80%所需的商家比例）:")

          // 按销量降序排序，计算累计占比
          val salesWithCumulative = validDF
            .orderBy(desc("monthly_sales"))
            .select("monthly_sales")
            .withColumn("cumulative_sales", sum("monthly_sales").over(Window.orderBy(desc("monthly_sales"))))
            .withColumn("total_sales", sum("monthly_sales").over())
            .withColumn("cumulative_ratio", col("cumulative_sales") / col("total_sales") * 100)

          // 找到累计占比达到80%的位置
          val targetRow = salesWithCumulative
            .filter(col("cumulative_ratio") >= 80)
            .limit(1)
            .collect()

          if (targetRow.nonEmpty) {
            val rowIndex = targetRow(0).getAs[Int](0)  // 这里需要获取行号，简化处理
            val shopCount = validDF.count()
            val percentage = targetRow(0).getAs[Double](3)  // cumulative_ratio
            println(s"累计销量达到80%时，覆盖了约${percentage}%的商家")
          }

          // ========== 分析6：高销量商家特征 ==========
          println("\n========== 高销量商家特征 ==========")

          val highSalesDF = validDF.filter(col("monthly_sales") > 1500)
          val lowSalesDF = validDF.filter(col("monthly_sales") <= 300)

          val highSalesCount = highSalesDF.count()
          val lowSalesCount = lowSalesDF.count()

          val totalCount = validDF.count()
          val highPercent = highSalesCount.toDouble / totalCount * 100
          val lowPercent = lowSalesCount.toDouble / totalCount * 100

          println(s"高销量商家（>1500单）数量: ${highSalesCount} (占比 ${highPercent.formatted("%.1f")}%)")
          println(s"低销量商家（≤300单）数量: ${lowSalesCount} (占比 ${lowPercent.formatted("%.1f")}%)")

              // 高销量商家的品类分布
              println("\n高销量商家品类分布（Top5）:")
              highSalesDF.groupBy("category_clean")
                .agg(count("shop_name").alias("shop_count"))
                .orderBy(desc("shop_count"))
                .limit(5)
                .show()

              // 高销量商家的距离特征
              val highSalesAvgDistance = highSalesDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
              val lowSalesAvgDistance = lowSalesDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]

              println(s"高销量商家平均距离: ${f"$highSalesAvgDistance%.0f"}米")
              println(s"低销量商家平均距离: ${f"$lowSalesAvgDistance%.0f"}米")

              // 高销量商家的评分特征
              val highSalesAvgRating = highSalesDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
              val lowSalesAvgRating = lowSalesDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]

              println(s"高销量商家平均评分: ${f"$highSalesAvgRating%.2f"}")
              println(s"低销量商家平均评分: ${f"$lowSalesAvgRating%.2f"}")

              // ========== 分析7：销量金字塔 ==========
              println("\n========== 销量金字塔 ==========")

              val salesPyramid = validDF
                .withColumn("sales_tier",
                  when(col("monthly_sales") <= 100, "基础层（≤100单）")
                    .when(col("monthly_sales") <= 500, "成长层（100-500单）")
                    .when(col("monthly_sales") <= 1500, "骨干层（500-1500单）")
                    .when(col("monthly_sales") <= 3000, "精英层（1500-3000单）")
                    .otherwise("顶流层（>3000单）")
                )
                .groupBy("sales_tier")
                .agg(
                  count("shop_name").alias("shop_count"),
                  avg("rating").alias("avg_rating"),
                  avg("distance").alias("avg_distance")
                )
                .orderBy(
                  when(col("sales_tier") === "基础层（≤100单）", 1)
                    .when(col("sales_tier") === "成长层（100-500单）", 2)
                    .when(col("sales_tier") === "骨干层（500-1500单）", 3)
                    .when(col("sales_tier") === "精英层（1500-3000单）", 4)
                    .otherwise(5)
                )

              println("销量金字塔:")
              salesPyramid.show()

    } finally {
      spark.stop()
    }
  }
}