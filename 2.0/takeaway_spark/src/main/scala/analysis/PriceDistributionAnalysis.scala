package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.MySQLUtils

object PriceDistributionAnalysis {

  /**
   * 供RunAllAnalysis调用的分析方法
   */
  def analyze(spark: SparkSession, cleanedDF: DataFrame): Unit = {
    import spark.implicits._
    println("开始执行人均消费分布分析...")

    // 过滤有效数据（人均>0）
    val validDF = cleanedDF.filter(col("avg_price") > 0)
    println(s"有效数据量: ${validDF.count()}")

    // ========== 分析1：人均消费统计摘要 ==========
    println("\n========== 人均消费统计摘要 ==========")

    val priceStats = validDF.select(
      avg("avg_price").alias("avg_price"),
      stddev("avg_price").alias("stddev_price"),
      min("avg_price").alias("min_price"),
      max("avg_price").alias("max_price"),
      expr("percentile_approx(avg_price, 0.25)").alias("p25"),
      expr("percentile_approx(avg_price, 0.50)").alias("median_price"),
      expr("percentile_approx(avg_price, 0.75)").alias("p75"),
      expr("percentile_approx(avg_price, 0.90)").alias("p90")
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
    println(s"平均人均消费: ${f"$avgPrice%.1f"}元")
    println(s"人均消费标准差: ${f"$stddevPrice%.1f"}元")
    println(s"最低人均消费: ${f"$minPrice%.1f"}元")
    println(s"最高人均消费: ${f"$maxPrice%.1f"}元")
    println(s"25%分位数: ${f"$p25%.1f"}元")
    println(s"中位数人均: ${f"$medianPrice%.1f"}元")
    println(s"75%分位数: ${f"$p75%.1f"}元")
    println(s"90%分位数: ${f"$p90%.1f"}元")

    // 写入统计摘要表
    val summaryData = Seq(
      ("店铺总数", validDF.count().toDouble),
      ("平均人均消费", avgPrice),
      ("人均消费标准差", stddevPrice),
      ("最低人均消费", minPrice),
      ("最高人均消费", maxPrice),
      ("25%分位数", p25),
      ("中位数人均", medianPrice),
      ("75%分位数", p75),
      ("90%分位数", p90)
    ).toDF("metric_name", "metric_value")

    summaryData.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "price_summary", MySQLUtils.getProperties())

    println("人均消费统计摘要已写入 price_summary 表")

    // ========== 分析2：人均消费分布（直方图数据） ==========
    println("\n========== 人均消费分布统计 ==========")

    val dfWithBucket = validDF
      .withColumn("price_bucket",
        when(col("avg_price") <= 10, "≤10元")
          .when(col("avg_price") <= 15, "10-15元")
          .when(col("avg_price") <= 20, "15-20元")
          .when(col("avg_price") <= 25, "20-25元")
          .when(col("avg_price") <= 30, "25-30元")
          .when(col("avg_price") <= 40, "30-40元")
          .when(col("avg_price") <= 50, "40-50元")
          .otherwise("50元以上")
      )

    val distribution = dfWithBucket.groupBy("price_bucket")
      .agg(
        count("shop_name").alias("shop_count"),
        min("avg_price").alias("min_price"),
        max("avg_price").alias("max_price")
      )
      .withColumn("percentage",
        round(col("shop_count") / sum("shop_count").over() * 100, 2))
      .withColumn("cumulative_percentage",
        round(sum("percentage").over(Window.orderBy("price_bucket")), 2))
      .orderBy(
        when(col("price_bucket") === "≤10元", 1)
          .when(col("price_bucket") === "10-15元", 2)
          .when(col("price_bucket") === "15-20元", 3)
          .when(col("price_bucket") === "20-25元", 4)
          .when(col("price_bucket") === "25-30元", 5)
          .when(col("price_bucket") === "30-40元", 6)
          .when(col("price_bucket") === "40-50元", 7)
          .otherwise(8)
      )

    println("人均消费分布结果:")
    distribution.show()

    distribution.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "price_distribution", MySQLUtils.getProperties())

    println("人均消费分布已写入 price_distribution 表")

    // ========== 分析3：各品类的人均消费分布 ==========
    println("\n========== 各品类人均消费分布 ==========")

    val categoryPriceDist = validDF
      .withColumn("price_level",
        when(col("avg_price") <= 15, "低价位(≤15元)")
          .when(col("avg_price") <= 25, "中价位(15-25元)")
          .when(col("avg_price") <= 40, "中高价位(25-40元)")
          .otherwise("高价位(>40元)")
      )
      .groupBy("category_clean", "price_level")
      .agg(count("shop_name").alias("shop_count"))
      .withColumnRenamed("category_clean", "category")
      .orderBy("category", "price_level")

    println("各品类价格级别分布:")
    categoryPriceDist.show(50)

    // ========== 分析4：不同距离区间的价格分布 ==========
    println("\n========== 不同距离区间的价格分布 ==========")

    val distancePriceDist = validDF
      .withColumn("distance_group",
        when(col("distance") <= 1000, "近距(≤1km)")
          .when(col("distance") <= 2000, "中距(1-2km)")
          .otherwise("远距(>2km)")
      )
      .withColumn("price_level",
        when(col("avg_price") <= 15, "低价位")
          .when(col("avg_price") <= 25, "中价位")
          .otherwise("高价位")
      )
      .groupBy("distance_group", "price_level")
      .agg(count("shop_name").alias("shop_count"))
      .orderBy("distance_group", "price_level")

    println("不同距离区间的价格分布:")
    distancePriceDist.show()

    // ========== 分析5：价格区间销量分析 ==========
    println("\n========== 价格区间销量分析 ==========")

    val priceSalesAnalysis = dfWithBucket
      .groupBy("price_bucket")
      .agg(
        count("shop_name").alias("shop_count"),
        sum("monthly_sales").alias("total_sales"),
        avg("monthly_sales").alias("avg_sales"),
        avg("rating").alias("avg_rating")
      )
      .orderBy("price_bucket")

    println("各价格区间的销量与评分:")
    priceSalesAnalysis.show()

    // ========== 分析6：各品类平均价格排名 ==========
    println("\n========== 各品类平均价格排名 ==========")

    val categoryAvgPrice = validDF
      .groupBy("category_clean")
      .agg(
        avg("avg_price").alias("avg_price"),
        count("shop_name").alias("shop_count"),
        stddev("avg_price").alias("price_stddev")
      )
      .withColumnRenamed("category_clean", "category")
      .orderBy(desc("avg_price"))

    println("各品类平均价格（从高到低）:")
    categoryAvgPrice.show(20)

    // ========== 分析7：价格与销量的相关性 ==========
    println("\n========== 价格与销量相关性 ==========")

    val priceSalesCorr = validDF.stat.corr("avg_price", "monthly_sales")
    println(s"人均消费与月售的相关系数: ${f"$priceSalesCorr%.4f"}")

    if (priceSalesCorr > 0.2) {
      println("结论：价格越高的商家，销量也越高 ")
    } else if (priceSalesCorr > 0) {
      println("结论：价格与销量有弱正相关")
    } else if (priceSalesCorr > -0.2) {
      println("结论：价格与销量关系不明显")
    } else {
      println("结论：价格越高的商家，销量反而越低 ")
    }

    // ========== 分析8：价格区间高评分占比 ==========
    println("\n========== 价格区间高评分占比 ==========")

    val priceHighRating = dfWithBucket
      .withColumn("is_high_rating", when(col("rating") >= 4.5, 1).otherwise(0))
      .groupBy("price_bucket")
      .agg(
        count("shop_name").alias("shop_count"),
        sum("is_high_rating").alias("high_rating_count")
      )
      .withColumn("high_rating_ratio",
        round(col("high_rating_count") / col("shop_count") * 100, 2))
      .orderBy("price_bucket")

    println("各价格区间的高评分占比:")
    priceHighRating.show()

    // ========== 分析9：价格金字塔 ==========
    println("\n========== 价格金字塔 ==========")

    val pricePyramid = validDF
      .withColumn("price_tier",
        when(col("avg_price") <= 10, "亲民层(≤10元)")
          .when(col("avg_price") <= 15, "实惠层(10-15元)")
          .when(col("avg_price") <= 20, "普通层(15-20元)")
          .when(col("avg_price") <= 30, "中高端层(20-30元)")
          .otherwise("高端层(>30元)")
      )
      .groupBy("price_tier")
      .agg(
        count("shop_name").alias("shop_count"),
        avg("monthly_sales").alias("avg_sales"),
        avg("rating").alias("avg_rating")
      )
      .orderBy(
        when(col("price_tier") === "亲民层(≤10元)", 1)
          .when(col("price_tier") === "实惠层(10-15元)", 2)
          .when(col("price_tier") === "普通层(15-20元)", 3)
          .when(col("price_tier") === "中高端层(20-30元)", 4)
          .otherwise(5)
      )

    println("价格金字塔:")
    pricePyramid.show()

    // ========== 分析10：品类价格箱线图数据 ==========
    println("\n========== 品类价格箱线图数据 ==========")

    val priceBoxplot = validDF
      .groupBy("category_clean")
      .agg(
        count("avg_price").alias("shop_count"),
        expr("percentile_approx(avg_price, 0.00)").alias("min_price"),
        expr("percentile_approx(avg_price, 0.25)").alias("q1_price"),
        expr("percentile_approx(avg_price, 0.50)").alias("median_price"),
        expr("percentile_approx(avg_price, 0.75)").alias("q3_price"),
        expr("percentile_approx(avg_price, 1.00)").alias("max_price"),
        avg("avg_price").alias("mean_price")
      )
      .filter(col("shop_count") >= 10)
      .withColumnRenamed("category_clean", "category")
      .orderBy(desc("median_price"))

    println("各品类价格箱线图数据:")
    priceBoxplot.show(20)

    println("人均消费分布分析完成")
  }

  /**
   * 独立运行时的main方法
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PriceDistributionAnalysis")
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