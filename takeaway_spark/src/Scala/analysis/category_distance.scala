package analysis

import cleaning.DataCleaner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.MySQLUtils

//品类-距离分布
object category_distance {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CategoryDistanceAnalysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // 读取清洗后的数据
      val cleanedDF = DataCleaner.getCleanedData(spark)

      // 添加距离分组
      val dfWithGroup = cleanedDF.withColumn("distance_group",
        when(col("distance") < 300, "0-300m")
          .when(col("distance") < 800, "300-800m")
          .when(col("distance") < 1500, "800-1500m")
          .otherwise("1500m+")
      )

      // 按品类和距离分组统计
      val result = dfWithGroup.groupBy("category_clean", "distance_group")
        .agg(count("shop_name").alias("shop_count"))
        .withColumnRenamed("category_clean", "category")
        .orderBy("category", "distance_group")

      println("========== 品类-距离分布结果 ==========")
      result.show(50)

      // 写入MySQL
      result.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_distance", MySQLUtils.getProperties())

      println(" 品类-距离分布已写入 category_distance 表")

      // 过滤有效数据（距离>0）
      val validDF = cleanedDF.filter(col("distance") > 0)
      println(s"有效数据量: ${validDF.count()}")

      // ========== 分析1：品类距离箱线图数据 ==========
      println("\n========== 品类距离箱线图统计 ==========")

      // 按品类计算距离的统计指标（箱线图所需数据）
      val boxplotData = validDF
        .groupBy("category_clean")
        .agg(
          count("distance").alias("shop_count"),
          expr("percentile_approx(distance, 0.00)").alias("min_distance"),
          expr("percentile_approx(distance, 0.25)").alias("q1_distance"),
          expr("percentile_approx(distance, 0.50)").alias("median_distance"),
          expr("percentile_approx(distance, 0.75)").alias("q3_distance"),
          expr("percentile_approx(distance, 1.00)").alias("max_distance"),
          avg("distance").alias("avg_distance"),
          stddev("distance").alias("stddev_distance")
        )
        .filter(col("shop_count") >= 5)  // 过滤样本太少的品类
        .withColumnRenamed("category_clean", "category")
        .orderBy(desc("median_distance"))  // 按中位数距离降序排列

      println("品类距离箱线图数据（按中位数距离排序）:")
      boxplotData.show(50)

      // 写入MySQL（替换原来的表结构）
      boxplotData.write
        .mode(SaveMode.Overwrite)
        .jdbc(MySQLUtils.getUrl(), "category_distance_boxplot", MySQLUtils.getProperties())

      println(" 品类距离箱线图数据已写入 category_distance_boxplot 表")

      // ========== 分析2：统计摘要 ==========
      println("\n========== 距离统计摘要 ==========")

      // 整体距离统计
      val overallStats = validDF.select(
        avg("distance").alias("avg_distance"),
        expr("percentile_approx(distance, 0.50)").alias("median_distance"),
        min("distance").alias("min_distance"),
        max("distance").alias("max_distance")
      ).collect()(0)

      println(s"整体平均距离: ${overallStats.getAs[Double](0)}米")
      println(s"整体中位数距离: ${overallStats.getAs[Double](1)}米")
      println(s"整体最小距离: ${overallStats.getInt(2)}米")
      println(s"整体最大距离: ${overallStats.getInt(3)}米")

      // ========== 分析3：品类距离排名 ==========
      println("\n========== 品类平均距离排名 ==========")

      val categoryRank = validDF
        .groupBy("category_clean")
        .agg(
          avg("distance").alias("avg_distance"),
          count("shop_name").alias("shop_count")
        )
        .withColumnRenamed("category_clean", "category")
        .orderBy(desc("avg_distance"))

      println("平均距离最远的品类（Top5）:")
      categoryRank.filter(col("shop_count") >= 5).show(5)

      println("\n平均距离最近的品类（Bottom5）:")
      categoryRank.filter(col("shop_count") >= 5).orderBy("avg_distance").show(5)

      // ========== 分析4：距离离散程度分析 ==========
      println("\n========== 距离离散程度分析 ==========")

      val dispersionData = boxplotData
        .withColumn("iqr", col("q3_distance") - col("q1_distance"))
        .withColumn("cv", round(col("stddev_distance") / col("avg_distance") * 100, 2))
        .select("category", "shop_count", "median_distance", "iqr", "cv", "min_distance", "max_distance")
        .orderBy(desc("iqr"))

      println("距离离散程度最大的品类（IQR越大，距离分布越分散）:")
      dispersionData.show(10)

      println("\n距离离散程度最小的品类（IQR越小，距离越集中）:")
      dispersionData.orderBy("iqr").show(10)

      // ========== 分析5：异常值检测 ==========
      println("\n========== 异常值检测（距离过远/过近的品类） ==========")


      // 计算整体距离的统计量
      val globalMedian = validDF
        .selectExpr("cast(percentile_approx(distance, 0.50) as double)")
        .collect()(0)
        .getDouble(0)

      val globalQ1 = validDF
        .selectExpr("cast(percentile_approx(distance, 0.25) as double)")
        .collect()(0)
        .getDouble(0)

      val globalQ3 = validDF
        .selectExpr("cast(percentile_approx(distance, 0.75) as double)")
        .collect()(0)
        .getDouble(0)

      val globalIQR = globalQ3 - globalQ1
      val upperBound = globalQ3 + 1.5 * globalIQR

      println(s"整体距离中位数: ${globalMedian}米")
      println(s"整体上界（异常值阈值）: ${upperBound}米")

      val farCategories = boxplotData
        .filter(col("median_distance") > upperBound)
        .select("category", "median_distance", "shop_count")
        .orderBy(desc("median_distance"))

      println("\n距离明显偏远的品类（中位数 > 整体上界）:")
      farCategories.show()

      // ========== 分析6：按距离分组统计（保留原有分析，作为补充） ==========
      println("\n========== 距离分组统计（补充分析） ==========")

      val distanceGroupStats = validDF
        .withColumn("distance_group",
          when(col("distance") < 500, "极近(≤500m)")
            .when(col("distance") < 1000, "近距(500-1000m)")
            .when(col("distance") < 1500, "中距(1000-1500m)")
            .when(col("distance") < 2000, "中远距(1500-2000m)")
            .otherwise("远距(>2000m)")
        )
        .groupBy("distance_group")
        .agg(
          count("shop_name").alias("shop_count"),
          avg("distance").alias("avg_distance")
        )
        .orderBy("distance_group")

      println("距离分组统计:")
      distanceGroupStats.show()

      // ========== 分析7：各品类距离分布对比（可选：写入详细数据供前端箱线图使用） ==========
      println("\n========== 品类距离详细数据（供前端箱线图） ==========")

      // 为了前端箱线图，还需要每个品类的所有距离值吗？
      // 由于箱线图只需要统计值，上面的 boxplotData 已经足够
      // 但为了更详细的分析，可以输出每个品类的距离百分位数

      val percentileData = validDF
        .groupBy("category_clean")
        .agg(
          expr("percentile_approx(distance, array(0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95))").alias("percentiles")
        )
        .withColumnRenamed("category_clean", "category")
        .limit(20)

      println("部分品类距离百分位数（5%, 10%, 25%, 50%, 75%, 90%, 95%）:")
      percentileData.show(20, false)

      // ========== 分析8：结论输出 ==========
      println("\n========== 分析结论 ==========")

      // 找出距离中位数最大和最小的品类
      val farthestCategory = boxplotData.orderBy(desc("median_distance")).select("category", "median_distance").first()
      val nearestCategory = boxplotData.orderBy("median_distance").select("category", "median_distance").first()

      println(s"距离最远的品类: ${farthestCategory.getString(0)} (中位数: ${farthestCategory.getAs[Double](1)}米)")
      println(s"距离最近的品类: ${nearestCategory.getString(0)} (中位数: ${nearestCategory.getAs[Double](1)}米)")

      // 找出离散程度最大和最小的品类
      val mostDispersed = dispersionData.orderBy(desc("iqr")).select("category", "iqr").first()
      val leastDispersed = dispersionData.orderBy("iqr").select("category", "iqr").first()

      println(s"距离分布最分散的品类: ${mostDispersed.getString(0)} (IQR: ${mostDispersed.getAs[Double](1)}米)")
      println(s"距离分布最集中的品类: ${leastDispersed.getString(0)} (IQR: ${leastDispersed.getAs[Double](1)}米)")

    } finally {
      spark.stop()
    }
  }
}