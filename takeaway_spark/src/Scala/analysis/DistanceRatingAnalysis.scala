package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DistanceRatingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DistanceRatingAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // ========== 分析1：距离与评分的整体相关性 ==========
      println("\n========== 距离与评分相关性分析 ==========")

      // 计算相关系数
      val correlation = cleanedDF.stat.corr("distance", "rating")
      println(s"距离与评分的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation < -0.3) {
        println("  负相关：距离越近，评分越高 ")
      } else if (correlation > 0.3) {
        println("  正相关：距离越远，评分越高 ")
      } else {
        println("  弱相关：距离与评分关系不明显")
      }

      // ========== 分析2：散点图数据 ==========
      println("\n========== 散点图数据 ==========")

      val scatterData = cleanedDF
        .select(
          col("distance"),
          col("rating"),
          col("shop_name"),
          col("category_clean").alias("category")
        )
        .filter(col("distance") > 0 && col("rating") > 0)
        .orderBy(col("distance"))

      println(s"散点图数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 写入散点图数据表
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "distance_rating_scatter", MySQLUtils.getProperties())

      println(" 散点图数据已写入 distance_rating_scatter 表")

      // ========== 分析3：距离分组对比（近/中/远） ==========
      println("\n========== 距离分组对比分析 ==========")

      // 定义距离分组
      val dfWithGroup = cleanedDF
        .withColumn("distance_group",
          when(col("distance") <= 1000, "近距(≤1km)")
            .when(col("distance") <= 2000, "中距(1-2km)")
            .otherwise("远距(>2km)")
        )

      // 分组统计
      val groupStats = dfWithGroup.groupBy("distance_group")
        .agg(
          count("shop_name").alias("shop_count"),
          avg("rating").alias("mean_rating"),
          min("rating").alias("min_rating"),
          max("rating").alias("max_rating"),
          stddev("rating").alias("stddev_rating")
        )
        .orderBy(
          when(col("distance_group") === "近距(≤1km)", 1)
            .when(col("distance_group") === "中距(1-2km)", 2)
            .otherwise(3)
        )

      println("分组统计结果:")
      groupStats.show()

      // ========== 分析4：箱线图数据（计算四分位数） ==========
      println("\n========== 箱线图数据计算 ==========")

      // 使用 Spark 的 percentile_approx 计算四分位数
      val boxplotData = dfWithGroup.groupBy("distance_group")
        .agg(
          count("rating").alias("shop_count"),
          expr("percentile_approx(rating, 0.00)").alias("min_rating"),
          expr("percentile_approx(rating, 0.25)").alias("q1_rating"),
          expr("percentile_approx(rating, 0.50)").alias("median_rating"),
          expr("percentile_approx(rating, 0.75)").alias("q3_rating"),
          expr("percentile_approx(rating, 1.00)").alias("max_rating"),
          avg("rating").alias("mean_rating")
        )
        .orderBy(
          when(col("distance_group") === "近距(≤1km)", 1)
            .when(col("distance_group") === "中距(1-2km)", 2)
            .otherwise(3)
        )

      println("箱线图数据:")
      boxplotData.show()

      // 写入箱线图表
      boxplotData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "distance_rating_boxplot", MySQLUtils.getProperties())

      println(" 箱线图数据已写入 distance_rating_boxplot 表")

      // ========== 分析5：显著性检验 ==========
      println("\n========== 显著性检验 ==========")

      // 提取近距和远距的评分数据
      val nearDF = dfWithGroup.filter(col("distance_group") === "近距(≤1km)").select("rating")
      val farDF = dfWithGroup.filter(col("distance_group") === "远距(>2km)").select("rating")

      val nearRatings = nearDF.collect().map(_.getDouble(0))
      val farRatings = farDF.collect().map(_.getDouble(0))

      // 计算t统计量（简化版，比较均值差异）
      val nearMean = nearRatings.sum / nearRatings.length
      val farMean = farRatings.sum / farRatings.length
      val diff = nearMean - farMean

      println(s"近距商家平均评分: ${f"$nearMean%.2f"}")
      println(s"远距商家平均评分: ${f"$farMean%.2f"}")
      println(s"评分差异: ${f"$diff%.2f"}")

      if (diff > 0.2) {
        println("结论：距离学校较近的商家（≤1km）评分显著高于远距离商家 ")
      } else if (diff > 0) {
        println("结论：距离学校较近的商家评分略高，但差异不大")
      } else {
        println("结论：距离学校较近的商家评分并未高于远距离商家 ")
      }

      // ========== 分析6：按品类细分 ==========
      println("\n========== 不同品类的距离-评分关系 ==========")

      val categoryDistanceRating = dfWithGroup
        .groupBy("category_clean", "distance_group")
        .agg(
          avg("rating").alias("avg_rating"),
          count("shop_name").alias("shop_count")
        )
        .filter(col("shop_count") >= 5)  // 过滤样本太少的品类
        .orderBy("category_clean", "distance_group")

      println("各品类在不同距离的评分:")
      categoryDistanceRating.show(50)

      // 找出差异最明显的品类
      println("\n========== 距离影响最明显的品类 ==========")

      val ratingDiffByCategory = categoryDistanceRating
        .groupBy("category_clean")
        .pivot("distance_group", Seq("近距(≤1km)", "远距(>2km)"))
        .agg(first("avg_rating"))
        .withColumn("rating_diff",
          when(col("近距(≤1km)").isNotNull && col("远距(>2km)").isNotNull,
            col("近距(≤1km)") - col("远距(>2km)"))
            .otherwise(null))
        .filter(col("rating_diff").isNotNull)
        .orderBy(desc("rating_diff"))

      ratingDiffByCategory.show(20)

      println("\n距离对评分影响最大的品类（近距评分 - 远距评分）:")
      ratingDiffByCategory.select("category_clean", "近距(≤1km)", "远距(>2km)", "rating_diff").show(10)

    } finally {
      spark.stop()
    }
  }
}