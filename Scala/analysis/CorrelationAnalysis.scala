package analysis

import cleaning.DataCleaner
import utils.MySQLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.feature.VectorAssembler

object CorrelationAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CorrelationAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)
      println(s"清洗后数据量: ${cleanedDF.count()}")

      // 2. 选择数值型字段
      val numericColumns = Seq("rating", "monthly_sales", "avg_price",
        "delivery_fee", "distance", "min_price", "delivery_time")

      val columnNames = Seq("评分", "月售", "人均", "配送费", "距离", "起送价", "送达时间")

      // 3. 过滤有效数据（所有字段都>0）
      val validDF = cleanedDF
        .filter(col("rating") > 0)
        .filter(col("monthly_sales") > 0)
        .filter(col("avg_price") > 0)
        .filter(col("delivery_fee") >= 0)
        .filter(col("distance") > 0)
        .filter(col("min_price") >= 0)
        .filter(col("delivery_time") > 0)

      println(s"有效数据量: ${validDF.count()}")

      // 4. 选择数值列并转换为Double类型
      val numericDF = validDF.select(
        col("rating").cast("double"),
        col("monthly_sales").cast("double"),
        col("avg_price").cast("double"),
        col("delivery_fee").cast("double"),
        col("distance").cast("double"),
        col("min_price").cast("double"),
        col("delivery_time").cast("double")
      ).na.drop()

      println("数据预览:")
      numericDF.show(5)

      // 5. 计算相关系数矩阵
      val assembler = new VectorAssembler()
        .setInputCols(numericColumns.toArray)
        .setOutputCol("features")

      val vectorDF = assembler.transform(numericDF).select("features")
      val correlationMatrix = Correlation.corr(vectorDF, "features").head()
      val correlation = correlationMatrix.getAs[org.apache.spark.ml.linalg.Matrix](0)

      // 6. 构建相关系数表（用于热力图）
      println("\n========== 相关系数矩阵 ==========")
      println("变量列表: " + columnNames.mkString(", "))
      println()

      // 打印矩阵
      print("          ")
      columnNames.foreach(name => print(f"$name%10s"))
      println()

      for (i <- 0 until columnNames.length) {
        print(f"${columnNames(i)}%10s")
        for (j <- 0 until columnNames.length) {
          val corr = correlation(i, j)
          val formatted = if (math.abs(corr) < 0.001) "  0.000  " else f"$corr%8.3f"
          print(formatted)
        }
        println()
      }

      // 7. 构建DataFrame格式的相关系数表
      import spark.implicits._

      val correlationRows = scala.collection.mutable.ListBuffer[(String, String, Double, Double, String)]()

      for (i <- 0 until columnNames.length) {
        for (j <- 0 until columnNames.length) {
          if (i <= j) { // 只存储上三角矩阵，避免重复
            val corr = correlation(i, j)
            val absCorr = math.abs(corr)
            val level = absCorr match {
              case c if c >= 0.8 => "极强相关"
              case c if c >= 0.6 => "强相关"
              case c if c >= 0.4 => "中等相关"
              case c if c >= 0.2 => "弱相关"
              case _ => "极弱相关或无相关"
            }
            correlationRows += ((columnNames(i), columnNames(j), corr, absCorr, level))
          }
        }
      }

      val correlationDF = correlationRows.toDF("variable1", "variable2", "correlation", "correlation_abs", "correlation_level")

      // 显示结果
      println("\n========== 相关系数表 ==========")
      correlationDF.filter(col("variable1") =!= col("variable2"))
        .orderBy(desc("correlation_abs"))
        .show(30)

      // 写入MySQL
      correlationDF.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "correlation_matrix", MySQLUtils.getProperties())

      println(" 相关系数矩阵已写入 correlation_matrix 表")

      // 8. 构建相关性统计摘要（最有价值的发现）
      println("\n========== 相关性统计摘要 ==========")

      val summaryRows = scala.collection.mutable.ListBuffer[(String, Double, String, String)]()

      // 找出正相关最强的几对
      val positiveCorrs = correlationRows.filter { case (v1, v2, corr, _, _) =>
        v1 != v2 && corr > 0.3
      }.sortBy(-_._3)

      // 找出负相关最强的几对
      val negativeCorrs = correlationRows.filter { case (v1, v2, corr, _, _) =>
        v1 != v2 && corr < -0.3
      }.sortBy(_._3)

      // 正相关洞察
      positiveCorrs.take(5).foreach { case (v1, v2, corr, _, _) =>
        val insight = if (corr > 0.5) {
          s"${v1}与${v2}呈现强正相关，说明两者同步增长"
        } else if (corr > 0.3) {
          s"${v1}与${v2}呈现中等正相关，有一定正向关联"
        } else {
          s"${v1}与${v2}呈现弱正相关"
        }
        summaryRows += ((s"${v1} ↔ ${v2}", corr, "正相关", insight))
      }

      // 负相关洞察
      negativeCorrs.take(5).foreach { case (v1, v2, corr, _, _) =>
        val insight = if (corr < -0.5) {
          s"${v1}与${v2}呈现强负相关，说明两者反向变动"
        } else if (corr < -0.3) {
          s"${v1}与${v2}呈现中等负相关，有一定反向关联"
        } else {
          s"${v1}与${v2}呈现弱负相关"
        }
        summaryRows += ((s"${v1} ↔ ${v2}", corr, "负相关", insight))
      }

      val summaryDF = summaryRows.toDF("variable_pair", "correlation", "relationship_type", "insight")

      println("最显著的正相关关系:")
      positiveCorrs.take(5).foreach { case (v1, v2, corr, _, _) =>
        println(f"  $v1 ↔ $v2 : ${f"$corr%.3f"}")
      }

      println("\n最显著的负相关关系:")
      negativeCorrs.take(5).foreach { case (v1, v2, corr, _, _) =>
        println(f"  $v1 ↔ $v2 : ${f"$corr%.3f"}")
      }

      // 写入摘要表
      summaryDF.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "correlation_summary", MySQLUtils.getProperties())

      println(" 相关性统计摘要已写入 correlation_summary 表")

      // 9. 输出业务洞察
      println("\n========== 业务洞察 ==========")

      // 检查评分与月售的关系
      val ratingSalesCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "评分" && v2 == "月售") || (v1 == "月售" && v2 == "评分")
      }.head._3

      if (ratingSalesCorr > 0.3) {
        println(" 评分与月售呈正相关：高评分商家通常有更高的销量")
      } else if (ratingSalesCorr > 0) {
        println(" 评分与月售有微弱正相关：评分对销量有一定影响，但非决定性因素")
      } else {
        println(" 评分与月售关系不明显：高评分不一定带来高销量")
      }

      // 检查距离与月售的关系
      val distanceSalesCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "距离" && v2 == "月售") || (v1 == "月售" && v2 == "距离")
      }.head._3

      if (distanceSalesCorr < -0.3) {
        println(" 距离与月售呈负相关：距离越近的商家销量越高")
      } else if (distanceSalesCorr < 0) {
        println(" 距离与月售有微弱负相关：距离对销量有一定影响")
      } else {
        println(" 距离与月售关系不明显：地理位置不是销量的决定因素")
      }

      // 检查配送费与月售的关系
      val feeSalesCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "配送费" && v2 == "月售") || (v1 == "月售" && v2 == "配送费")
      }.head._3

      if (feeSalesCorr < -0.2) {
        println(" 配送费与月售呈负相关：免配送费或低配送费商家销量更高")
      } else if (feeSalesCorr < 0) {
        println(" 配送费与月售有微弱负相关：配送费对销量有一定影响")
      } else {
        println(" 配送费与月售关系不明显：用户对配送费不敏感")
      }

      // 检查送达时间与月售的关系
      val timeSalesCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "送达时间" && v2 == "月售") || (v1 == "月售" && v2 == "送达时间")
      }.head._3

      if (timeSalesCorr < -0.3) {
        println(" 送达时间与月售呈负相关：送得越快，销量越高")
      } else if (timeSalesCorr < 0) {
        println(" 送达时间与月售有微弱负相关：配送效率对销量有一定影响")
      } else {
        println(" 送达时间与月售关系不明显：用户对等待时间不敏感")
      }

      // 检查人均与起送价的关系
      val priceMinPriceCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "人均" && v2 == "起送价") || (v1 == "起送价" && v2 == "人均")
      }.head._3

      if (priceMinPriceCorr > 0.5) {
        println(" 人均与起送价呈强正相关：高人均店铺通常起送价也高")
      } else if (priceMinPriceCorr > 0) {
        println(" 人均与起送价呈正相关：两者有一定关联")
      }

      // 检查距离与配送费的关系
      val distanceFeeCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "距离" && v2 == "配送费") || (v1 == "配送费" && v2 == "距离")
      }.head._3

      if (distanceFeeCorr > 0.4) {
        println(" 距离与配送费呈正相关：距离越远，配送费越高")
      } else if (distanceFeeCorr > 0) {
        println(" 距离与配送费有微弱正相关")
      }

      // 检查送达时间与距离的关系
      val timeDistanceCorr = correlationRows.filter { case (v1, v2, _, _, _) =>
        (v1 == "送达时间" && v2 == "距离") || (v1 == "距离" && v2 == "送达时间")
      }.head._3

      if (timeDistanceCorr > 0.5) {
        println(" 送达时间与距离呈强正相关：距离越远，送达越慢")
      } else if (timeDistanceCorr > 0) {
        println(" 送达时间与距离呈正相关")
      }

    } finally {
      spark.stop()
    }
  }
}
