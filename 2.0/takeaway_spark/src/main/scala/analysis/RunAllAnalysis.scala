package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.SparkSession

object RunAllAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AllAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      println("========== 开始执行所有分析 ==========")

      // 只执行一次数据清洗，获取清洗后的DataFrame
      // 第一次运行会清洗所有food_data/*.csv文件并保存到food_data_cleanned
      // 后续运行直接读取food_data_cleanned目录下的Parquet文件
      val cleanedDF = DataCleaner.getCleanedData(spark)
      val dataCount = cleanedDF.count()
      println(s"数据加载完成，共 $dataCount 条记录")

      // 缓存DataFrame到内存，加速后续计算
      cleanedDF.cache()

      // 依次执行所有分析（每个分析类需要改造接收DataFrame参数）
      println("\n========== 1/25: 品类距离分析 ==========")
      category_distance.analyze(spark, cleanedDF)

      println("\n========== 2/25: 品类统计 ==========")
      CategoryStats.analyze(spark, cleanedDF)

      println("\n========== 3/25: 品类评分分析 ==========")
      CategoryRatingAnalysis.analyze(spark, cleanedDF)

      println("\n========== 4/25: 品类销量分析 ==========")
      CategorySalesAnalysis.analyze(spark, cleanedDF)

      println("\n========== 5/25: 相关性分析 ==========")
      CorrelationAnalysis.analyze(spark, cleanedDF)

      println("\n========== 6/25: 配送费距离分析 ==========")
      DeliveryFeeDistanceAnalysis.analyze(spark, cleanedDF)

      println("\n========== 7/25: 配送费分布分析 ==========")
      DeliveryFeeDistributionAnalysis.analyze(spark, cleanedDF)

      println("\n========== 8/25: 配送费评分分析 ==========")
      DeliveryFeeRatingAnalysis.analyze(spark, cleanedDF)

      println("\n========== 9/25: 配送费时间分析 ==========")
      DeliveryFeeTimeAnalysis.analyze(spark, cleanedDF)

      println("\n========== 10/25: 配送时间分布 ==========")
      DeliveryTimeDistributionAnalysis.analyze(spark, cleanedDF)

      println("\n========== 11/25: 配送时间评分分析 ==========")
      DeliveryTimeRatingAnalysis.analyze(spark, cleanedDF)

      println("\n========== 12/25: 距离统计 ==========")
      DistanceStats.analyze(spark, cleanedDF)

      println("\n========== 13/25: 距离评分分析 ==========")
      DistanceRatingAnalysis.analyze(spark, cleanedDF)

      println("\n========== 14/25: 起送价分布 ==========")
      MinPriceDistributionAnalysis.analyze(spark, cleanedDF)

      println("\n========== 15/25: 起送价评分分析 ==========")
      MinPriceRatingAnalysis.analyze(spark, cleanedDF)

      println("\n========== 16/25: 价格分布 ==========")
      PriceDistributionAnalysis.analyze(spark, cleanedDF)

      println("\n========== 17/25: 价格评分分析 ==========")
      PriceRatingAnalysis.analyze(spark, cleanedDF)

      println("\n========== 18/25: 评分分布 ==========")
      RatingDistribution.analyze(spark, cleanedDF)

      println("\n========== 19/25: 销量配送费分析 ==========")
      SalesDeliveryFeeAnalysis.analyze(spark, cleanedDF)

      println("\n========== 20/25: 销量配送时间分析 ==========")
      SalesDeliveryTimeAnalysis.analyze(spark, cleanedDF)

      println("\n========== 21/25: 销量距离分析 ==========")
      SalesDistanceAnalysis.analyze(spark, cleanedDF)

      println("\n========== 22/25: 销量分布 ==========")
      SalesDistributionAnalysis.analyze(spark, cleanedDF)

      println("\n========== 23/25: 销量评分相关性 ==========")
      SalesRatingCorrelation.analyze(spark, cleanedDF)

      println("\n========== 24/25: 散点图数据 ==========")
      ScatterData.analyze(spark, cleanedDF)

      println("\n========== 25/25: 热门商家 ==========")
      TopShops.analyze(spark, cleanedDF)

      // 释放缓存
      cleanedDF.unpersist()

      println("\n========== 所有分析执行完成 ==========")

    } finally {
      spark.stop()
    }
  }
}