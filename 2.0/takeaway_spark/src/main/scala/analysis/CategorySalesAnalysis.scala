package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.MySQLUtils

object CategorySalesAnalysis {

  /**
   * 供RunAllAnalysis调用的分析方法
   */
  def analyze(spark: SparkSession, cleanedDF: DataFrame): Unit = {
    println("开始执行品类销量分析...")

    // 过滤有效数据（月售>0）
    val validDF = cleanedDF.filter(col("monthly_sales") > 0)
    println(s"有效数据量: ${validDF.count()}")

    // ========== 分析1：品类月售统计（用于对比） ==========
    println("\n========== 品类月售统计 ==========")

    val categorySalesStats = validDF
      .groupBy("category_clean")
      .agg(
        count("shop_name").alias("shop_count"),
        sum("monthly_sales").alias("total_sales"),
        avg("monthly_sales").alias("avg_sales"),
        expr("percentile_approx(monthly_sales, 0.50)").alias("median_sales"),
        min("monthly_sales").alias("min_sales"),
        max("monthly_sales").alias("max_sales"),
        sum(when(col("monthly_sales") > 1000, 1).otherwise(0)).alias("high_sales_count")
      )
      .withColumn("high_sales_ratio",
        round(col("high_sales_count") / col("shop_count") * 100, 2))
      .select(
        col("category_clean").alias("category"),
        col("shop_count"),
        col("total_sales"),
        round(col("avg_sales"), 0).alias("avg_sales"),
        col("median_sales"),
        col("min_sales"),
        col("max_sales"),
        col("high_sales_ratio")
      )
      .orderBy(desc("avg_sales"))

    println("品类月售统计（按平均月售排序）:")
    categorySalesStats.show(20)

    // 写入品类月售统计表
    categorySalesStats.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "category_sales_stats", MySQLUtils.getProperties())

    println("品类月售统计已写入 category_sales_stats 表")

    // ========== 分析2：品类月售分布（分组柱状图） ==========
    println("\n========== 品类月售分布 ==========")

    // 定义月售区间
    val dfWithBucket = validDF
      .withColumn("sales_bucket",
        when(col("monthly_sales") <= 300, "低销量(≤300单)")
          .when(col("monthly_sales") <= 800, "中低销量(300-800单)")
          .when(col("monthly_sales") <= 1500, "中高销量(800-1500单)")
          .otherwise("高销量(>1500单)")
      )

    // 按品类和月售区间分组统计
    val categorySalesDist = dfWithBucket
      .groupBy("category_clean", "sales_bucket")
      .agg(count("shop_name").alias("shop_count"))
      .withColumnRenamed("category_clean", "category")
      .orderBy("category", "sales_bucket")

    // 计算每个品类内的占比
    val categoryTotal = categorySalesDist
      .groupBy("category")
      .agg(sum("shop_count").alias("total"))

    val distributionWithPercent = categorySalesDist
      .join(categoryTotal, "category")
      .withColumn("percentage", round(col("shop_count") / col("total") * 100, 2))
      .withColumn("bucket_order",
        when(col("sales_bucket") === "低销量(≤300单)", 1)
          .when(col("sales_bucket") === "中低销量(300-800单)", 2)
          .when(col("sales_bucket") === "中高销量(800-1500单)", 3)
          .otherwise(4)
      )
      .select("category", "sales_bucket", "shop_count", "percentage")
      .orderBy("category", "bucket_order")
      .drop("bucket_order")

    println("各品类月售分布:")
    distributionWithPercent.show(50)

    // 写入品类月售分布表
    distributionWithPercent.write
      .mode(SaveMode.Overwrite)
      .jdbc(MySQLUtils.getUrl(), "category_sales_distribution", MySQLUtils.getProperties())

    println("品类月售分布已写入 category_sales_distribution 表")

    // ========== 分析3：品类月售排名（Top品类） ==========
    println("\n========== 销量最高品类（总月售排名） ==========")
    categorySalesStats
      .select("category", "total_sales", "shop_count", "avg_sales")
      .orderBy(desc("total_sales"))
      .show(10)

    println("\n========== 平均销量最高品类 ==========")
    categorySalesStats
      .select("category", "avg_sales", "shop_count", "high_sales_ratio")
      .orderBy(desc("avg_sales"))
      .show(10)

    // ========== 分析4：不同品类的销量集中度 ==========
    println("\n========== 不同品类的销量集中度 ==========")

    // 计算每个品类头部商家（Top 20%）的销量占比
    val categoryConcentration = validDF
      .groupBy("category_clean")
      .agg(
        count("shop_name").alias("total_count"),
        sum("monthly_sales").alias("total_sales")
      )
      .withColumnRenamed("category_clean", "category")

    // 计算每个品类Top 20%商家的销量
    val top20PercentSales = validDF
      .withColumn("rank", row_number().over(Window.partitionBy("category_clean").orderBy(desc("monthly_sales"))))
      .withColumn("total_count", count("shop_name").over(Window.partitionBy("category_clean")))
      .filter(col("rank") <= ceil(col("total_count") * 0.2))
      .groupBy("category_clean")
      .agg(sum("monthly_sales").alias("top20_sales"))
      .withColumnRenamed("category_clean", "category")

    val concentrationResult = categoryConcentration
      .join(top20PercentSales, "category")
      .withColumn("concentration_ratio",
        round(col("top20_sales") / col("total_sales") * 100, 2))
      .select("category", "total_count", "total_sales", "top20_sales", "concentration_ratio")
      .orderBy(desc("concentration_ratio"))

    println("各品类头部商家（Top20%）销量占比:")
    concentrationResult.show(20)

    // ========== 分析5：高销量品类特征 ==========
    println("\n========== 高销量品类特征 ==========")

    val highSalesCategory = categorySalesStats
      .filter(col("avg_sales") > 1000)
      .orderBy(desc("avg_sales"))

    println("高销量品类（平均月售>1000单）:")
    highSalesCategory.show()

    // ========== 分析6：品类销量与店铺数量的关系 ==========
    println("\n========== 品类销量与店铺数量的关系 ==========")

    val correlation = categorySalesStats.stat.corr("shop_count", "avg_sales")
    println(s"品类店铺数量与平均月售的相关系数: ${f"$correlation%.4f"}")

    if (correlation > 0.3) {
      println("结论：店铺数量多的品类，平均月售也较高 ")
    } else if (correlation > 0) {
      println("结论：品类店铺数量与平均月售有弱正相关")
    } else {
      println("结论：品类店铺数量与平均月售关系不明显")
    }

    // ========== 分析7：品类销量分布热力图数据 ==========
    println("\n========== 品类销量分布热力图数据 ==========")

    // 构建品类×销量区间的矩阵数据
    val heatmapData = dfWithBucket
      .groupBy("category_clean", "sales_bucket")
      .agg(count("shop_name").alias("shop_count"))
      .withColumnRenamed("category_clean", "category")
      .orderBy("category", "sales_bucket")

    println("热力图数据（品类×销量区间）:")
    heatmapData.show(50)

    // ========== 分析8：各品类的销量箱线图数据 ==========
    println("\n========== 品类销量箱线图数据 ==========")

    val salesBoxplot = validDF
      .groupBy("category_clean")
      .agg(
        count("monthly_sales").alias("shop_count"),
        expr("percentile_approx(monthly_sales, 0.00)").alias("min_sales"),
        expr("percentile_approx(monthly_sales, 0.25)").alias("q1_sales"),
        expr("percentile_approx(monthly_sales, 0.50)").alias("median_sales"),
        expr("percentile_approx(monthly_sales, 0.75)").alias("q3_sales"),
        expr("percentile_approx(monthly_sales, 1.00)").alias("max_sales"),
        avg("monthly_sales").alias("mean_sales")
      )
      .filter(col("shop_count") >= 10)
      .withColumnRenamed("category_clean", "category")
      .orderBy(desc("median_sales"))

    println("各品类月售箱线图数据:")
    salesBoxplot.show(20)

    println("品类销量分析完成")
  }

  /**
   * 独立运行时的main方法
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CategorySalesAnalysis")
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