package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object MinPriceDistributionAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MinPriceDistributionAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（起送价>=0）
      val validDF = cleanedDF.filter(col("min_price") >= 0)
      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：起送价统计摘要 ==========
      println("\n========== 起送价统计摘要 ==========")

      val priceStats = validDF.select(
        avg("min_price").alias("avg_price"),
        stddev("min_price").alias("stddev_price"),
        min("min_price").alias("min_price"),
        max("min_price").alias("max_price"),
        expr("percentile_approx(min_price, 0.25)").alias("p25"),
        expr("percentile_approx(min_price, 0.50)").alias("median_price"),
        expr("percentile_approx(min_price, 0.75)").alias("p75"),
        expr("percentile_approx(min_price, 0.90)").alias("p90")
      ).collect()(0)

      val avgPrice = priceStats.getDouble(0)
      val stddevPrice = priceStats.getDouble(1)
      val minPrice = priceStats.getDouble(2)
      val maxPrice = priceStats.getDouble(3)
      val p25 = priceStats.getDouble(4)
      val medianPrice = priceStats.getDouble(5)
      val p75 = priceStats.getDouble(6)
      val p90 = priceStats.getDouble(7)

      println(s"店铺总数: ${validDF.count()}")
      println(s"平均起送价: ${f"$avgPrice%.1f"}元")
      println(s"起送价标准差: ${f"$stddevPrice%.1f"}元")
      println(s"最低起送价: ${f"$minPrice%.1f"}元")
      println(s"最高起送价: ${f"$maxPrice%.1f"}元")
      println(s"25%分位数: ${f"$p25%.1f"}元")
      println(s"中位数起送价: ${f"$medianPrice%.1f"}元")
      println(s"75%分位数: ${f"$p75%.1f"}元")
      println(s"90%分位数: ${f"$p90%.1f"}元")

      // 免起送商家统计
      val freeMinPriceCount = validDF.filter(col("min_price") === 0).count()
      val freeMinPriceRatio = freeMinPriceCount.toDouble / validDF.count() * 100
      println(s"\n免起送商家数量: ${freeMinPriceCount} (占比 ${f"$freeMinPriceRatio%.1f"}%)")

        // 写入统计摘要表
        val summaryData = Seq(
          ("店铺总数", validDF.count().toDouble),
          ("平均起送价", avgPrice),
          ("起送价标准差", stddevPrice),
          ("最低起送价", minPrice),
          ("最高起送价", maxPrice),
          ("25%分位数", p25),
          ("中位数起送价", medianPrice),
          ("75%分位数", p75),
          ("90%分位数", p90),
          ("免起送商家占比", freeMinPriceRatio)
        ).toDF("metric_name", "metric_value")

        summaryData.write
          .mode(SaveMode.Overwrite)
          .jdbc(MySQLUtils.getUrl(), "min_price_summary", MySQLUtils.getProperties())

        println(" 起送价统计摘要已写入 min_price_summary 表")

        // ========== 分析2：起送价分布（直方图数据） ==========
        println("\n========== 起送价分布统计 ==========")

        // 定义起送价区间
        val dfWithBucket = validDF
          .withColumn("price_bucket",
            when(col("min_price") === 0, "0元（免起送）")
              .when(col("min_price") <= 10, "1-10元")
              .when(col("min_price") <= 15, "10-15元")
              .when(col("min_price") <= 20, "15-20元")
              .when(col("min_price") <= 25, "20-25元")
              .when(col("min_price") <= 30, "25-30元")
              .when(col("min_price") <= 40, "30-40元")
              .otherwise("40元以上")
          )

        // 分组统计
        val distribution = dfWithBucket.groupBy("price_bucket")
          .agg(
            count("shop_name").alias("shop_count"),
            min("min_price").alias("min_price"),
            max("min_price").alias("max_price")
          )
          .withColumn("percentage",
            round(col("shop_count") / sum("shop_count").over() * 100, 2))
          .withColumn("cumulative_percentage",
            round(sum("percentage").over(Window.orderBy("price_bucket")), 2))
          .orderBy(
            when(col("price_bucket") === "0元（免起送）", 1)
              .when(col("price_bucket") === "1-10元", 2)
              .when(col("price_bucket") === "10-15元", 3)
              .when(col("price_bucket") === "15-20元", 4)
              .when(col("price_bucket") === "20-25元", 5)
              .when(col("price_bucket") === "25-30元", 6)
              .when(col("price_bucket") === "30-40元", 7)
              .otherwise(8)
          )

        println("起送价分布结果:")
        distribution.show()

        // 写入起送价分布表
        distribution.write
          .mode(SaveMode.Overwrite)
          .jdbc(MySQLUtils.getUrl(), "min_price_distribution", MySQLUtils.getProperties())

        println(" 起送价分布已写入 min_price_distribution 表")

        // ========== 分析3：各品类的起送价分布 ==========
        println("\n========== 各品类起送价分布 ==========")

        val categoryMinPriceDist = validDF
          .withColumn("price_level",
            when(col("min_price") === 0, "免起送")
              .when(col("min_price") <= 15, "低起送(≤15元)")
              .when(col("min_price") <= 25, "中起送(15-25元)")
              .otherwise("高起送(>25元)")
          )
          .groupBy("category_clean", "price_level")
          .agg(count("shop_name").alias("shop_count"))
          .withColumnRenamed("category_clean", "category")
          .orderBy("category", "price_level")

        println("各品类起送价级别分布:")
        categoryMinPriceDist.show(50)

        // ========== 分析4：不同距离区间的起送价分布 ==========
        println("\n========== 不同距离区间的起送价分布 ==========")

        val distanceMinPriceDist = validDF
          .withColumn("distance_group",
            when(col("distance") <= 1000, "近距(≤1km)")
              .when(col("distance") <= 2000, "中距(1-2km)")
              .otherwise("远距(>2km)")
          )
          .withColumn("price_level",
            when(col("min_price") === 0, "免起送")
              .when(col("min_price") <= 15, "低起送")
              .otherwise("高起送")
          )
          .groupBy("distance_group", "price_level")
          .agg(count("shop_name").alias("shop_count"))
          .orderBy("distance_group", "price_level")

        println("不同距离区间的起送价分布:")
        distanceMinPriceDist.show()

        // ========== 分析5：起送价与销量的关系 ==========
        println("\n========== 起送价与销量分析 ==========")

        val minPriceSalesAnalysis = dfWithBucket
          .groupBy("price_bucket")
          .agg(
            count("shop_name").alias("shop_count"),
            sum("monthly_sales").alias("total_sales"),
            avg("monthly_sales").alias("avg_sales"),
            avg("rating").alias("avg_rating")
          )
          .orderBy("price_bucket")

        println("各起送价区间的销量与评分:")
        minPriceSalesAnalysis.show()

        // ========== 分析6：不同品类的平均起送价排名 ==========
        println("\n========== 各品类平均起送价排名 ==========")

        val categoryAvgMinPrice = validDF
          .groupBy("category_clean")
          .agg(
            avg("min_price").alias("avg_min_price"),
            count("shop_name").alias("shop_count"),
            stddev("min_price").alias("price_stddev")
          )
          .withColumnRenamed("category_clean", "category")
          .orderBy(desc("avg_min_price"))

        println("各品类平均起送价（从高到低）:")
        categoryAvgMinPrice.show(20)

        // ========== 分析7：起送价与人均消费的关系 ==========
        println("\n========== 起送价与人均消费关系 ==========")

        val priceCorrelation = validDF.stat.corr("min_price", "avg_price")
        println(s"起送价与人均消费的相关系数: ${f"$priceCorrelation%.4f"}")

        if (priceCorrelation > 0.5) {
          println("结论：起送价与人均消费高度正相关 ")
        } else if (priceCorrelation > 0.3) {
          println("结论：起送价与人均消费有较强正相关")
        } else if (priceCorrelation > 0) {
          println("结论：起送价与人均消费有弱正相关")
        } else {
          println("结论：起送价与人均消费关系不明显")
        }

        // ========== 分析8：免起送商家特征 ==========
        println("\n========== 免起送商家特征 ==========")

        val freeMinPriceDF = validDF.filter(col("min_price") === 0)
        val paidMinPriceDF = validDF.filter(col("min_price") > 0)

        val freeAvgSales = freeMinPriceDF.select(avg("monthly_sales")).collect()(0)(0).asInstanceOf[Double]
        val paidAvgSales = paidMinPriceDF.select(avg("monthly_sales")).collect()(0)(0).asInstanceOf[Double]
        val freeAvgRating = freeMinPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
        val paidAvgRating = paidMinPriceDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
        val freeAvgPrice = freeMinPriceDF.select(avg("avg_price")).collect()(0)(0).asInstanceOf[Double]
        val paidAvgPrice = paidMinPriceDF.select(avg("avg_price")).collect()(0)(0).asInstanceOf[Double]

        println(s"免起送商家数量: ${freeMinPriceDF.count()}")
        println(s"免起送商家平均月售: ${f"$freeAvgSales%.0f"}单")
        println(s"免起送商家平均评分: ${f"$freeAvgRating%.2f"}分")
        println(s"免起送商家平均人均: ${f"$freeAvgPrice%.1f"}元")
        println(s"\n有起送价商家数量: ${paidMinPriceDF.count()}")
        println(s"有起送价商家平均月售: ${f"$paidAvgSales%.0f"}单")
        println(s"有起送价商家平均评分: ${f"$paidAvgRating%.2f"}分")
        println(s"有起送价商家平均人均: ${f"$paidAvgPrice%.1f"}元")

        // ========== 分析9：起送价金字塔 ==========
        println("\n========== 起送价金字塔 ==========")

        val minPricePyramid = validDF
          .withColumn("price_tier",
            when(col("min_price") === 0, "免起送层")
              .when(col("min_price") <= 15, "低起送层(≤15元)")
              .when(col("min_price") <= 25, "中起送层(15-25元)")
              .otherwise("高起送层(>25元)")
          )
          .groupBy("price_tier")
          .agg(
            count("shop_name").alias("shop_count"),
            avg("monthly_sales").alias("avg_sales"),
            avg("rating").alias("avg_rating")
          )
          .orderBy(
            when(col("price_tier") === "免起送层", 1)
              .when(col("price_tier") === "低起送层(≤15元)", 2)
              .when(col("price_tier") === "中起送层(15-25元)", 3)
              .otherwise(4)
          )

        println("起送价金字塔:")
        minPricePyramid.show()

        // ========== 分析10：起送价分布箱线图数据（按品类） ==========
        println("\n========== 品类起送价箱线图数据 ==========")

        val minPriceBoxplot = validDF
          .groupBy("category_clean")
          .agg(
            count("min_price").alias("shop_count"),
            expr("percentile_approx(min_price, 0.00)").alias("min_price"),
            expr("percentile_approx(min_price, 0.25)").alias("q1_price"),
            expr("percentile_approx(min_price, 0.50)").alias("median_price"),
            expr("percentile_approx(min_price, 0.75)").alias("q3_price"),
            expr("percentile_approx(min_price, 1.00)").alias("max_price"),
            avg("min_price").alias("mean_price")
          )
          .filter(col("shop_count") >= 10)
          .withColumnRenamed("category_clean", "category")
          .orderBy(desc("median_price"))

        println("各品类起送价箱线图数据:")
        minPriceBoxplot.show(20)

     } finally {
        spark.stop()
     }
  }
}