package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.feature.VectorAssembler

object SalesRatingCorrelation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SalesRatingCorrelationAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 2. 散点图数据（保留原始数据供前端绘图）
      val scatterData = cleanedDF
        .select(
          col("monthly_sales"),
          col("rating"),
          col("shop_name"),
          col("category_clean").alias("category")
        )
        .filter(col("monthly_sales") > 0 && col("rating") > 0)
        .orderBy(col("monthly_sales").desc)

      println("========== 月售-评分散点图数据 ==========")
      println(s"数据量: ${scatterData.count()}")
      scatterData.show(20)

      // 3. 写入MySQL（散点图数据）
      scatterData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "sales_rating_correlation", MySQLUtils.getProperties())

      println(" 散点图数据已写入 sales_rating_correlation 表")

      // 4. 相关性分析（使用Spark MLlib）
      println("\n========== 相关性分析 ==========")

      // 准备数值列
      val numericDF = cleanedDF
        .select(
          col("monthly_sales").cast("double"),
          col("rating").cast("double")
        )
        .na.drop()

      // 计算相关系数（Pearson）
      val correlation = numericDF.stat.corr("monthly_sales", "rating")
      println(s"月售与评分的皮尔逊相关系数: ${f"$correlation%.4f"}")

      // 解释相关性
      println("\n相关性解读:")
      if (correlation > 0.7) {
        println("  强正相关：高评分商家通常月售更高")
      } else if (correlation > 0.3) {
        println("  中等正相关：评分对月售有一定正向影响")
      } else if (correlation > 0) {
        println("  弱正相关：评分与月售关系不明显")
      } else if (correlation > -0.3) {
        println("  弱负相关：高评分商家月售反而偏低")
      } else {
        println("  强负相关：评分与月售呈反向关系")
      }

      // 5. 分组分析：不同评分区间的平均月售
      println("\n========== 不同评分区间的平均月售 ==========")
      val ratingGroupAnalysis = cleanedDF
        .withColumn("rating_group",
          when(col("rating") < 3.0, "3.0分以下")
            .when(col("rating") < 3.5, "3.0-3.5分")
            .when(col("rating") < 4.0, "3.5-4.0分")
            .when(col("rating") < 4.5, "4.0-4.5分")
            .otherwise("4.5分以上")
        )
        .groupBy("rating_group")
        .agg(
          avg("monthly_sales").alias("平均月售"),
          count("shop_name").alias("店铺数量"),
          avg("rating").alias("平均评分")
        )
        .orderBy("rating_group")

      ratingGroupAnalysis.show()

      // 6. 高评分商家 vs 普通商家对比
      println("\n========== 高评分商家 vs 普通商家 ==========")
      val highRatingDF = cleanedDF.filter(col("rating") >= 4.5)
      val normalRatingDF = cleanedDF.filter(col("rating") < 4.5)

      val highRatingAvgSales = highRatingDF.select(avg("monthly_sales")).collect()(0)(0)
      val normalRatingAvgSales = normalRatingDF.select(avg("monthly_sales")).collect()(0)(0)

      println(s"高评分商家（≥4.5分）平均月售: ${highRatingAvgSales}")
      println(s"普通商家（<4.5分）平均月售: ${normalRatingAvgSales}")

      if (highRatingAvgSales.asInstanceOf[Double] > normalRatingAvgSales.asInstanceOf[Double]) {
        println("结论：高评分商家的月售普遍高于普通商家 ")
      } else {
        println("结论：高评分商家的月售并未显著高于普通商家 ")
      }

      // 7. 高月售商家 vs 普通商家评分对比
      println("\n========== 高月售商家 vs 普通商家 ==========")

      // 计算月售的中位数作为分界线
      val medianSales = cleanedDF
        .selectExpr("percentile_approx(monthly_sales, 0.5)")
        .collect()(0)(0)

      println(s"月售中位数: ${medianSales}")
      val medianValue = medianSales.toString.toDouble

      val highSalesDF = cleanedDF.filter(col("monthly_sales") > medianValue)
      val lowSalesDF = cleanedDF.filter(col("monthly_sales") <= medianValue)

      val highSalesAvgRating = highSalesDF.select(avg("rating")).collect()(0)(0)
      val lowSalesAvgRating = lowSalesDF.select(avg("rating")).collect()(0)(0)

      println(s"高月售商家（>中位数）平均评分: ${highSalesAvgRating}")
      println(s"低月售商家（≤中位数）平均评分: ${lowSalesAvgRating}")

      if (highSalesAvgRating.asInstanceOf[Double] > lowSalesAvgRating.asInstanceOf[Double]) {
        println("结论：高月售商家的评分普遍高于低月售商家 ")
      } else {
        println("结论：高月售商家的评分并未显著高于低月售商家 ")
      }

    } finally {
      spark.stop()
    }
  }
}
