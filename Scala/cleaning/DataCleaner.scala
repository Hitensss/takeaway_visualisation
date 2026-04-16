package cleaning

import model.CleanedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocessing.DataPreprocessor

object DataCleaner {

  /**
   * 获取清洗后的DataFrame
   * 流程：预处理 -> 解析 -> 品类归类
   *
   * @param spark SparkSession
   * @param filePath HDFS上的CSV文件路径
   * @return 清洗后的DataFrame
   */
  def getCleanedData(spark: SparkSession,
                     filePath: String = "hdfs://192.168.2.128:9000/外卖商家数据集.csv"): DataFrame = {

    import spark.implicits._

    // ========== 1. 数据预处理（去重、缺失值填充、异常值过滤） ==========
    println("========== 开始数据预处理 ==========")
    val preprocessedDF = DataPreprocessor.preprocess(spark, filePath)

    // 打印数据质量报告（可选）
    DataPreprocessor.printQualityReport(preprocessedDF)

    // ========== 2. 字段解析和品类归类 ==========
    println("========== 开始字段解析和品类归类 ==========")

    val cleanedDF = preprocessedDF
      .select(
        col("商家名称").alias("shop_name"),
        col("品类").alias("category_raw"),
        col("月售"),
        col("评分"),
        col("距离"),
        col("人均"),
        col("送达时间"),
        col("配送费"),
        col("起送价")
      )
      .map { row =>
        // 安全获取各字段值
        val monthlySalesStr = Option(row.getAs[String]("月售")).getOrElse("月售0")
        val ratingStr = Option(row.getAs[String]("评分")).getOrElse("0")
        val distanceStr = Option(row.getAs[String]("距离")).getOrElse("0m")
        val avgPriceStr = Option(row.getAs[String]("人均")).getOrElse("0")
        val deliveryTimeStr = Option(row.getAs[String]("送达时间")).getOrElse("0分钟")
        val deliveryFeeStr = Option(row.getAs[String]("配送费")).getOrElse("免配送费")
        val minPriceStr = Option(row.getAs[String]("起送价")).getOrElse("0")

        CleanedData(
          shop_name = row.getAs[String]("shop_name"),
          category_raw = row.getAs[String]("category_raw"),
          category_clean = CategoryMapper.mapCategory(row.getAs[String]("category_raw")),
          monthly_sales = FieldParser.parseMonthlySales(monthlySalesStr),
          rating = FieldParser.parseRating(ratingStr),
          distance = FieldParser.parseDistance(distanceStr),
          delivery_time = FieldParser.parseDeliveryTime(deliveryTimeStr),
          delivery_fee = FieldParser.parseDeliveryFee(deliveryFeeStr),
          min_price = FieldParser.parseMinPrice(minPriceStr),
          avg_price = FieldParser.parseAvgPrice(avgPriceStr)
        )
      }

    println(s"清洗完成，最终数据量: ${cleanedDF.count()}")
    println("========== 数据清洗完成 ==========\n")

    cleanedDF.toDF()
  }
}