package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SalesDeliveryFeeAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SalesDeliveryFeeAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（月售>0，配送费>=0）
      val validDF = cleanedDF
        .filter(col("monthly_sales") > 0)
        .filter(col("delivery_fee") >= 0)

      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：月售与配送费的整体相关性 ==========
      println("\n========== 月售与配送费相关性分析 ==========")

      // 计算相关系数
      val correlation = validDF.stat.corr("monthly_sales", "delivery_fee")
      println(s"月售与配送费的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.2) {
        println("  负相关：配送费越低，月售越高 ")
      } else if (correlation > 0.2) {
        println("  正相关：配送费越高，月售越高 ")
      } else if (correlation < 0) {
        println("  弱负相关：配送费与月售有一定负向关系")
      } else {
        println("  弱相关：配送费与月售关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = validDF
        .select(
          col("monthly_sales"),
          col("delivery_fee"),
          col("shop_name"),
          col("category_clean").alias("category"),
          col("rating")
        )
        .orderBy(col("delivery_fee"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "sales_delivery_fee_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 sales_delivery_fee_scatter 表")

      // ========== 分析3：配送费分组月售统计 ==========
      println("\n========== 配送费分组月售统计 ==========")

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
            .when(col("delivery_fee") <= 1.0, "0-1元")
            .when(col("delivery_fee") <= 2.0, "1-2元")
            .when(col("delivery_fee") <= 3.0, "2-3元")
            .when(col("delivery_fee") <= 5.0, "3-5元")
            .otherwise("5元以上")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("fee_group")
        .agg(
          count("shop_name").alias("shop_count"),
          min("delivery_fee").alias("min_fee"),
          max("delivery_fee").alias("max_fee"),
          sum("monthly_sales").alias("total_sales"),
          avg("monthly_sales").alias("avg_sales"),
          expr("percentile_approx(monthly_sales, 0.50)").alias("median_sales")
        )
        .select(
          col("fee_group"),
          col("min_fee"),
          col("max_fee"),
          col("shop_count"),
          round(col("avg_sales"), 0).alias("avg_sales"),
          col("median_sales"),
          col("total_sales")
        )
        .orderBy(
          when(col("fee_group") === "免配送费", 1)
            .when(col("fee_group") === "0-1元", 2)
            .when(col("fee_group") === "1-2元", 3)
            .when(col("fee_group") === "2-3元", 4)
            .when(col("fee_group") === "3-5元", 5)
            .otherwise(6)
        )

      println("\n配送费分组月售统计:")
      groupStats.show()

      // 写入分组统计表
      groupStats.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "delivery_fee_sales_group", MySQLUtils.getProperties())

      println(" 配送费分组月售统计已写入 delivery_fee_sales_group 表")

      // ========== 分析4：免配送费 vs 收费配送对比 ==========
      println("\n========== 免配送费 vs 收费配送对比 ==========")

      val freeDeliveryDF = validDF.filter(col("delivery_fee") === 0)
      val paidDeliveryDF = validDF.filter(col("delivery_fee") > 0)

      val freeAvgSales = freeDeliveryDF.select(avg("monthly_sales")).collect()(0).getDouble(0)
      val paidAvgSales = paidDeliveryDF.select(avg("monthly_sales")).collect()(0).getDouble(0)
      val freeMedianSales = freeDeliveryDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      val paidMedianSales = paidDeliveryDF
        .selectExpr("cast(percentile_approx(monthly_sales, 0.5) as double)")
        .collect()(0)
        .getDouble(0)

      println(s"免配送费商家数量: ${freeDeliveryDF.count()}")
      println(s"免配送费商家平均月售: ${f"$freeAvgSales%.0f"}单")
      println(s"免配送费商家月售中位数: ${f"$freeMedianSales%.0f"}单")
      println(s"\n收费配送商家数量: ${paidDeliveryDF.count()}")
      println(s"收费配送商家平均月售: ${f"$paidAvgSales%.0f"}单")
      println(s"收费配送商家月售中位数: ${f"$paidMedianSales%.0f"}单")

      val diff = freeAvgSales - paidAvgSales

      if (diff > 200) {
        println(f"\n结论：免配送费商家的月售显著高于收费配送商家  ($diff%.0f单)")
        } else if (diff > 0) {
          println(f"\n结论：免配送费商家的月售略高于收费配送商家 ($diff%.0f单)")
        } else {
          println(s"\n结论：免配送费与收费配送商家月售差异不大")
      }

      // ========== 分析5：高销量商家的配送费特征 ==========
      println("\n========== 高销量商家的配送费特征 ==========")

      val highSalesDF = validDF.filter(col("monthly_sales") > 1500)
      val lowSalesDF = validDF.filter(col("monthly_sales") <= 300)

      val highSalesAvgFee = highSalesDF.select(avg("delivery_fee")).collect()(0)(0).asInstanceOf[Double]
      val lowSalesAvgFee = lowSalesDF.select(avg("delivery_fee")).collect()(0)(0).asInstanceOf[Double]
      val highSalesFreeRatio = highSalesDF.filter(col("delivery_fee") === 0).count().toDouble / highSalesDF.count() * 100
      val lowSalesFreeRatio = lowSalesDF.filter(col("delivery_fee") === 0).count().toDouble / lowSalesDF.count() * 100

      println(s"高销量商家（>1500单）平均配送费: ${f"$highSalesAvgFee%.2f"}元")
      println(s"高销量商家（>1500单）免配送费占比: ${f"$highSalesFreeRatio%.1f"}%")
        println(s"\n低销量商家（≤300单）平均配送费: ${f"$lowSalesAvgFee%.2f"}元")
        println(s"低销量商家（≤300单）免配送费占比: ${f"$lowSalesFreeRatio%.1f"}%")

          // ========== 分析6：按品类细分 ==========
          println("\n========== 不同品类的配送费-月售关系 ==========")

          val categoryFeeSales = validDF
            .groupBy("category_clean")
            .agg(
              avg("delivery_fee").alias("avg_delivery_fee"),
              avg("monthly_sales").alias("avg_sales"),
              count("shop_name").alias("shop_count"),
              corr("delivery_fee", "monthly_sales").alias("correlation")
            )
            .filter(col("shop_count") >= 10)
            .orderBy("correlation")

          println("各品类的配送费-月售相关系数:")
          categoryFeeSales.show(20)

          // 找出配送费影响最明显的品类
          println("\n配送费对月售影响最明显的品类（负相关最强）:")
          categoryFeeSales
            .filter(col("correlation") < -0.2)
            .select("category_clean", "avg_delivery_fee", "avg_sales", "correlation")
            .show(10)

          // ========== 分析7：配送费与月售的趋势线数据 ==========
          println("\n========== 配送费-月售趋势 ==========")

          // 按配送费区间计算平均月售
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
              avg("monthly_sales").alias("avg_sales"),
              count("shop_name").alias("shop_count")
            )
            .orderBy("fee_interval")

          println("配送费-月售趋势:")
          trendData.show()

          // ========== 分析8：不同评分区间的配送费敏感度 ==========
          println("\n========== 不同评分区间的配送费敏感度 ==========")

          val ratingFeeSensitivity = validDF
            .withColumn("rating_group",
              when(col("rating") < 4.0, "低评分(<4.0)")
                .when(col("rating") < 4.5, "中评分(4.0-4.5)")
                .otherwise("高评分(≥4.5)")
            )
            .groupBy("rating_group")
            .agg(
              corr("delivery_fee", "monthly_sales").alias("correlation"),
              avg("delivery_fee").alias("avg_fee"),
              avg("monthly_sales").alias("avg_sales")
            )

          println("不同评分区间的配送费敏感度:")
          ratingFeeSensitivity.show()

    } finally {
      spark.stop()
    }
  }
}
