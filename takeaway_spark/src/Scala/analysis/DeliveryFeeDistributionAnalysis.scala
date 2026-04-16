package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DeliveryFeeDistributionAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeliveryFeeDistributionAnalysis")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 过滤有效数据（配送费>=0）
      val validDF = cleanedDF.filter(col("delivery_fee") >= 0)
      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：配送费统计摘要 ==========
      println("\n========== 配送费统计摘要 ==========")

      val feeStats = validDF.select(
        avg("delivery_fee").alias("avg_fee"),
        stddev("delivery_fee").alias("stddev_fee"),
        min("delivery_fee").alias("min_fee"),
        max("delivery_fee").alias("max_fee"),
        expr("percentile_approx(delivery_fee, 0.25)").alias("p25"),
        expr("percentile_approx(delivery_fee, 0.50)").alias("median_fee"),
        expr("percentile_approx(delivery_fee, 0.75)").alias("p75"),
        expr("percentile_approx(delivery_fee, 0.90)").alias("p90")
      ).collect()(0)

      val avgFee = feeStats.getDouble(0)
      val stddevFee = feeStats.getDouble(1)
      val minFee = feeStats.getDouble(2)
      val maxFee = feeStats.getDouble(3)
      val p25 = feeStats.getDouble(4)
      val medianFee = feeStats.getDouble(5)
      val p75 = feeStats.getDouble(6)
      val p90 = feeStats.getDouble(7)

      println(s"店铺总数: ${validDF.count()}")
      println(s"平均配送费: ${f"$avgFee%.2f"}元")
      println(s"配送费标准差: ${f"$stddevFee%.2f"}元")
      println(s"最低配送费: ${f"$minFee%.2f"}元")
      println(s"最高配送费: ${f"$maxFee%.2f"}元")
      println(s"25%分位数: ${f"$p25%.2f"}元")
      println(s"中位数配送费: ${f"$medianFee%.2f"}元")
      println(s"75%分位数: ${f"$p75%.2f"}元")
      println(s"90%分位数: ${f"$p90%.2f"}元")

      // 免配送费商家统计
      val freeDeliveryCount = validDF.filter(col("delivery_fee") === 0).count()
      val freeDeliveryRatio = freeDeliveryCount.toDouble / validDF.count() * 100
      println(s"\n免配送费商家数量: ${freeDeliveryCount} (占比 ${f"$freeDeliveryRatio%.1f"}%)")

        // 写入统计摘要表
        val summaryData = Seq(
          ("店铺总数", validDF.count().toDouble),
          ("平均配送费", avgFee),
          ("配送费标准差", stddevFee),
          ("最低配送费", minFee),
          ("最高配送费", maxFee),
          ("25%分位数", p25),
          ("中位数配送费", medianFee),
          ("75%分位数", p75),
          ("90%分位数", p90),
          ("免配送费商家占比", freeDeliveryRatio)
        ).toDF("metric_name", "metric_value")

        summaryData.write
          .mode(SaveMode.Overwrite)
          .jdbc(MySQLUtils.getUrl(), "delivery_fee_summary", MySQLUtils.getProperties())

        println(" 配送费统计摘要已写入 delivery_fee_summary 表")

        // ========== 分析2：配送费分布（直方图数据） ==========
        println("\n========== 配送费分布统计 ==========")

        // 定义配送费区间
        val dfWithBucket = validDF
          .withColumn("fee_bucket",
            when(col("delivery_fee") === 0, "0元（免配送费）")
              .when(col("delivery_fee") <= 0.5, "0.5元以下")
              .when(col("delivery_fee") <= 1.0, "0.5-1元")
              .when(col("delivery_fee") <= 1.5, "1-1.5元")
              .when(col("delivery_fee") <= 2.0, "1.5-2元")
              .when(col("delivery_fee") <= 2.5, "2-2.5元")
              .when(col("delivery_fee") <= 3.0, "2.5-3元")
              .when(col("delivery_fee") <= 4.0, "3-4元")
              .when(col("delivery_fee") <= 5.0, "4-5元")
              .otherwise("5元以上")
          )

        // 分组统计
        val distribution = dfWithBucket.groupBy("fee_bucket")
          .agg(
            count("shop_name").alias("shop_count"),
            min("delivery_fee").alias("min_fee"),
            max("delivery_fee").alias("max_fee")
          )
          .withColumn("percentage",
            round(col("shop_count") / sum("shop_count").over() * 100, 2))
          .withColumn("cumulative_percentage",
            round(sum("percentage").over(Window.orderBy("fee_bucket")), 2))
          .orderBy(
            when(col("fee_bucket") === "0元（免配送费）", 1)
              .when(col("fee_bucket") === "0.5元以下", 2)
              .when(col("fee_bucket") === "0.5-1元", 3)
              .when(col("fee_bucket") === "1-1.5元", 4)
              .when(col("fee_bucket") === "1.5-2元", 5)
              .when(col("fee_bucket") === "2-2.5元", 6)
              .when(col("fee_bucket") === "2.5-3元", 7)
              .when(col("fee_bucket") === "3-4元", 8)
              .when(col("fee_bucket") === "4-5元", 9)
              .otherwise(10)
          )

        println("配送费分布结果:")
        distribution.show()

        // 写入配送费分布表
        distribution.write
          .mode(SaveMode.Overwrite)
          .jdbc(MySQLUtils.getUrl(), "delivery_fee_distribution", MySQLUtils.getProperties())

        println(" 配送费分布已写入 delivery_fee_distribution 表")

        // ========== 分析3：各品类的配送费分布 ==========
        println("\n========== 各品类配送费分布 ==========")

        val categoryFeeDist = validDF
          .withColumn("fee_level",
            when(col("delivery_fee") === 0, "免配送费")
              .when(col("delivery_fee") <= 1.0, "低配送费(≤1元)")
              .when(col("delivery_fee") <= 2.0, "中配送费(1-2元)")
              .otherwise("高配送费(>2元)")
          )
          .groupBy("category_clean", "fee_level")
          .agg(count("shop_name").alias("shop_count"))
          .withColumnRenamed("category_clean", "category")
          .orderBy("category", "fee_level")

        println("各品类配送费级别分布:")
        categoryFeeDist.show(50)

        // ========== 分析4：不同距离区间的配送费分布 ==========
        println("\n========== 不同距离区间的配送费分布 ==========")

        val distanceFeeDist = validDF
          .withColumn("distance_group",
            when(col("distance") <= 1000, "近距(≤1km)")
              .when(col("distance") <= 2000, "中距(1-2km)")
              .otherwise("远距(>2km)")
          )
          .withColumn("fee_level",
            when(col("delivery_fee") === 0, "免配送费")
              .when(col("delivery_fee") <= 1.0, "低配送费")
              .otherwise("高配送费")
          )
          .groupBy("distance_group", "fee_level")
          .agg(count("shop_name").alias("shop_count"))
          .orderBy("distance_group", "fee_level")

        println("不同距离区间的配送费分布:")
        distanceFeeDist.show()

        // ========== 分析5：配送费与销量的关系 ==========
        println("\n========== 配送费与销量分析 ==========")

        val feeSalesAnalysis = dfWithBucket
          .groupBy("fee_bucket")
          .agg(
            count("shop_name").alias("shop_count"),
            sum("monthly_sales").alias("total_sales"),
            avg("monthly_sales").alias("avg_sales"),
            avg("rating").alias("avg_rating")
          )
          .orderBy("fee_bucket")

        println("各配送费区间的销量与评分:")
        feeSalesAnalysis.show()

        // ========== 分析6：不同品类的平均配送费排名 ==========
        println("\n========== 各品类平均配送费排名 ==========")

        val categoryAvgFee = validDF
          .groupBy("category_clean")
          .agg(
            avg("delivery_fee").alias("avg_fee"),
            count("shop_name").alias("shop_count"),
            stddev("delivery_fee").alias("fee_stddev")
          )
          .withColumnRenamed("category_clean", "category")
          .orderBy(desc("avg_fee"))

        println("各品类平均配送费（从高到低）:")
        categoryAvgFee.show(20)

        // ========== 分析7：配送费与距离的关系 ==========
        println("\n========== 配送费与距离关系 ==========")

        val distanceCorrelation = validDF.stat.corr("distance", "delivery_fee")
        println(s"距离与配送费的相关系数: ${f"$distanceCorrelation%.4f"}")

        if (distanceCorrelation > 0.4) {
          println("结论：距离越远，配送费越高 ")
        } else if (distanceCorrelation > 0.2) {
          println("结论：距离与配送费有较强正相关")
        } else if (distanceCorrelation > 0) {
          println("结论：距离与配送费有弱正相关")
        } else {
          println("结论：距离与配送费关系不明显")
        }

        // ========== 分析8：免配送费商家特征 ==========
        println("\n========== 免配送费商家特征 ==========")

        val freeDeliveryDF = validDF.filter(col("delivery_fee") === 0)
        val paidDeliveryDF = validDF.filter(col("delivery_fee") > 0)

        val freeAvgSales = freeDeliveryDF.select(avg("monthly_sales")).collect()(0)(0).asInstanceOf[Double]
        val paidAvgSales = paidDeliveryDF.select(avg("monthly_sales")).collect()(0)(0).asInstanceOf[Double]
        val freeAvgRating = freeDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
        val paidAvgRating = paidDeliveryDF.select(avg("rating")).collect()(0)(0).asInstanceOf[Double]
        val freeAvgDistance = freeDeliveryDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]
        val paidAvgDistance = paidDeliveryDF.select(avg("distance")).collect()(0)(0).asInstanceOf[Double]

        println(s"免配送费商家数量: ${freeDeliveryDF.count()}")
        println(s"免配送费商家平均月售: ${f"$freeAvgSales%.0f"}单")
        println(s"免配送费商家平均评分: ${f"$freeAvgRating%.2f"}分")
        println(s"免配送费商家平均距离: ${f"$freeAvgDistance%.0f"}米")
        println(s"\n收费配送商家数量: ${paidDeliveryDF.count()}")
        println(s"收费配送商家平均月售: ${f"$paidAvgSales%.0f"}单")
        println(s"收费配送商家平均评分: ${f"$paidAvgRating%.2f"}分")
        println(s"收费配送商家平均距离: ${f"$paidAvgDistance%.0f"}米")

        // ========== 分析9：配送费金字塔 ==========
        println("\n========== 配送费金字塔 ==========")

        val feePyramid = validDF
          .withColumn("fee_tier",
            when(col("delivery_fee") === 0, "免配送费层")
              .when(col("delivery_fee") <= 1.0, "低配送费层(≤1元)")
              .when(col("delivery_fee") <= 2.0, "中配送费层(1-2元)")
              .otherwise("高配送费层(>2元)")
          )
          .groupBy("fee_tier")
          .agg(
            count("shop_name").alias("shop_count"),
            avg("monthly_sales").alias("avg_sales"),
            avg("rating").alias("avg_rating")
          )
          .orderBy(
            when(col("fee_tier") === "免配送费层", 1)
              .when(col("fee_tier") === "低配送费层(≤1元)", 2)
              .when(col("fee_tier") === "中配送费层(1-2元)", 3)
              .otherwise(4)
          )

        println("配送费金字塔:")
        feePyramid.show()

        // ========== 分析10：配送费分布箱线图数据（按品类） ==========
        println("\n========== 品类配送费箱线图数据 ==========")

        val feeBoxplot = validDF
          .groupBy("category_clean")
          .agg(
            count("delivery_fee").alias("shop_count"),
            expr("percentile_approx(delivery_fee, 0.00)").alias("min_fee"),
            expr("percentile_approx(delivery_fee, 0.25)").alias("q1_fee"),
            expr("percentile_approx(delivery_fee, 0.50)").alias("median_fee"),
            expr("percentile_approx(delivery_fee, 0.75)").alias("q3_fee"),
            expr("percentile_approx(delivery_fee, 1.00)").alias("max_fee"),
            avg("delivery_fee").alias("mean_fee")
          )
          .filter(col("shop_count") >= 10)
          .withColumnRenamed("category_clean", "category")
          .orderBy(desc("median_fee"))

        println("各品类配送费箱线图数据:")
        feeBoxplot.show(20)

    } finally {
      spark.stop()
    }
  }
}
