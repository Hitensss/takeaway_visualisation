package preprocessing

import cleaning.FieldParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataPreprocessor {

  /**
   * 数据预处理主方法
   */
  def preprocess(spark: SparkSession, filePath: String): DataFrame = {

    // 读取原始数据
    val df = spark.read
      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv(filePath)

    println(s"原始数据量: ${df.count()}")

    // ========== 1. 去重 ==========
    val beforeDedup = df.count()
    val df1 = df.dropDuplicates(Seq("商家名称"))
    val afterDedup = df1.count()
    println(s"去重: $beforeDedup -> $afterDedup (删除 ${beforeDedup - afterDedup} 条重复)")

    // ========== 2. 缺失值处理 ==========

    // 2.1 状态缺失：默认为1，不影响
    var df2 = df1.withColumn("状态",
      when(col("状态").isNull || col("状态") === "", "1")
        .otherwise(col("状态")))

    // 2.2 评分缺失：用同品类均值填充
    // 先计算每个品类的平均评分
    val categoryAvgRating = df2
      .filter(col("评分").isNotNull && col("评分") =!= "" && col("评分") =!= "0")
      .withColumn("评分_num", col("评分").cast("double"))
      .groupBy("品类")
      .agg(avg("评分_num").alias("avg_rating"))

    // 用同品类均值填充缺失值
    df2 = df2.join(categoryAvgRating, Seq("品类"), "left")
      .withColumn("评分",
        when(col("评分").isNull || col("评分") === "",
          round(col("avg_rating"), 1).cast("string"))
          .otherwise(col("评分")))
      .drop("avg_rating")

    // 2.3 人均缺失：用同品类均值填充
    // 先计算每个品类的平均人均
    val categoryAvgPrice = df2
      .filter(col("人均").isNotNull && col("人均") =!= "" && col("人均") =!= "0")
      .withColumn("人均_num", regexp_replace(col("人均"), "[^0-9.]", "").cast("double"))
      .groupBy("品类")
      .agg(avg("人均_num").alias("avg_price"))

    // 用同品类均值填充缺失值
    df2 = df2.join(categoryAvgPrice, Seq("品类"), "left")
      .withColumn("人均",
        when(col("人均").isNull || col("人均") === "",
          round(col("avg_price"), 0).cast("string"))
          .otherwise(col("人均")))
      .drop("avg_price")

    // 2.4 月售缺失：用0填充
    df2 = df2.withColumn("月售",
      when(col("月售").isNull || col("月售") === "", "月售0")
        .otherwise(col("月售")))

    // 2.5 距离缺失：用平均值填充
    // 先计算有效的平均距离（从数据中计算）
    val avgDistance = computeAverageDistanceFromStrings(df2)

    // 如果平均距离大于0，使用计算出的值，否则使用默认值1000
    val avgDistanceValue = if (avgDistance > 0) avgDistance else 1000.0
    val avgDistanceInt = avgDistanceValue.toInt

    df2 = df2.withColumn("距离",
      when(col("距离").isNull || col("距离") === "",
        concat(lit(avgDistanceInt), lit("m")))
        .otherwise(col("距离")))

    // 2.6 品类缺失：用"其他"填充
    df2 = df2.withColumn("品类",
      when(col("品类").isNull || col("品类") === "", "其他")
        .otherwise(col("品类")))

    // 2.7 送达时间缺失：用全局中位数填充
    // 计算全局中位数
    val medianDeliveryTime = df2
      .filter(
        col("送达时间").isNotNull &&
          col("送达时间") =!= "" &&
          col("送达时间") =!= "0分钟"
      )
      .withColumn("送达时间_num",
        regexp_replace(col("送达时间"), "分钟", "").cast("int")
      )
      .filter(col("送达时间_num").isNotNull)
      .selectExpr("percentile_approx(`送达时间_num`, 0.5)")  // 关键：加反引号
      .collect()(0)(0)

    val medianTime = medianDeliveryTime match {
      case d: Double => d.toInt
      case l: Long => l.toInt
      case i: Int => i
      case _ => 30 // 默认值
    }

    df2 = df2.withColumn("送达时间",
      when(col("送达时间").isNull || col("送达时间") === "",
        concat(lit(medianTime), lit("分钟")))
        .otherwise(col("送达时间")))

    // 2.8 配送费缺失：用0填充
    df2 = df2.withColumn("配送费",
      when(col("配送费").isNull || col("配送费") === "", "免配送费")
        .otherwise(col("配送费")))


    // ========== 3. 异常值过滤 ==========

    /*没必要了，删除
    // 3.1 只保留营业中的店铺
    val beforeFilter1 = df2.count()
    var df3 = df2.filter(col("状态") === "1")
    val afterFilter1 = df3.count()
    println(s"过滤停业店铺: $beforeFilter1 -> $afterFilter1 (删除 ${beforeFilter1 - afterFilter1} 条)")
    */

    // 3.2 过滤距离为0或空值
    val beforeFilter2 = df2.count()
    df2 = df2.filter(col("距离") =!= "0m" && col("距离") =!= "0" && col("距离") =!= "" && col("距离").isNotNull)
    val afterFilter2 = df2.count()
    println(s"过滤无效距离: $beforeFilter2 -> $afterFilter2 (删除 ${beforeFilter2 - afterFilter2} 条)")

    // 3.3 过滤评分为0
    val beforeFilter3 = df2.count()
    df2 = df2.filter(col("评分") =!= "0" && col("评分") =!= "0.0")
    val afterFilter3 = df2.count()
    println(s"过滤0分店铺: $beforeFilter3 -> $afterFilter3 (删除 ${beforeFilter3 - afterFilter3} 条)")

    // 3.4 过滤月售为0
    val beforeFilter4 = df2.count()
    df2 = df2.filter(col("月售") =!= "月售0" && col("月售")=!="0")
    val afterFilter4 = df2.count()
    println(s"过滤月售0店铺: $beforeFilter4 -> $afterFilter4 (删除 ${beforeFilter4 - afterFilter4} 条)")

    // 3.4 过滤人均为0
    val beforeFilter5 = df2.count()
    df2 = df2.filter(col("人均") =!= "人均0")
    val afterFilter5 = df2.count()
    println(s"过滤月售0店铺: $beforeFilter5 -> $afterFilter5 (删除 ${beforeFilter5 - afterFilter5} 条)")


    println(s"最终数据量: ${df2.count()}")

    df2
  }

  /**
   * 计算平均距离（从有效数据中计算）
   * @return 计算出的平均距离，如果没有有效数据则返回0
   */
  private def computeAverageDistanceFromStrings(df: DataFrame): Double = {
    import df.sparkSession.implicits._

    val distances = df
      .filter(col("距离").isNotNull && col("距离") =!= "" && col("距离") =!= "0m")
      .map(row => FieldParser.parseDistance(row.getAs[String]("距离")))
      .filter(_ > 0)
      .collect()

    if (distances.length > 0) {
      distances.sum.toDouble / distances.length
    } else {
      0.0  // 返回0，让调用方决定默认值
    }
  }

  /**
   * 打印数据质量报告
   */
  def printQualityReport(df: DataFrame): Unit = {
    println("\n========== 数据质量报告 ==========")
    println(s"总记录数: ${df.count()}")

    val columns = Seq("商家名称", "品类", "评分", "人均", "月售", "距离")
    columns.foreach { colName =>
      val nullCount = df.filter(col(colName).isNull || col(colName) === "").count()
      println(s"  $colName 缺失: $nullCount 条")
    }

    println("\n异常值统计:")
    println(s"  评分为0的店铺: ${df.filter(col("评分") === "0").count()} 条")
    println(s"  距离为0的店铺: ${df.filter(col("距离") === "0m").count()} 条")
    println(s"  月售为0的店铺: ${df.filter(col("月售") === "月售0").count()} 条")
    println("==================================\n")
  }
}