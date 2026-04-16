package utils

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

object MySQLUtils {

  // MySQL连接配置（修改为你的虚拟机IP）
  private val host = "192.168.2.128"
  private val port = "3306"
  private val database = "food_analysis"
  private val user = "root"
  private val password = "root"

  // 不带数据库的URL（用于创建数据库）
  private val baseUrl = s"jdbc:mysql://$host:$port/?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  // 带数据库的URL（用于正常连接）
  private val fullUrl = s"jdbc:mysql://$host:$port/$database?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  /**
   * 确保数据库存在，如果不存在则创建
   */
  private def ensureDatabaseExists(): Unit = {
    var connection: Connection = null
    var statement: Statement = null

    try {
      // 先连接到MySQL服务器（不带数据库）
      val props = new Properties()
      props.setProperty("user", user)
      props.setProperty("password", password)
      props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

      connection = DriverManager.getConnection(baseUrl, props)
      statement = connection.createStatement()

      // 创建数据库（如果不存在）
      val createDbSQL = s"CREATE DATABASE IF NOT EXISTS `$database` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
      statement.executeUpdate(createDbSQL)
      println(s"数据库 '$database' 已存在或创建成功")

    } catch {
      case e: Exception =>
        println(s"创建数据库失败: ${e.getMessage}")
        throw new RuntimeException(s"无法创建数据库 $database", e)
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  /**
   * 获取MySQL连接Properties
   * 会自动检查并创建数据库
   */
  def getProperties(): Properties = {
    // 确保数据库存在
    ensureDatabaseExists()

    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    props
  }

  /**
   * 获取JDBC URL（包含数据库名）
   */
  def getUrl(): String = {
    // 确保数据库存在
    ensureDatabaseExists()
    fullUrl
  }

  /**
   * 获取JDBC URL（指定表名）
   */
  def getTableUrl(tableName: String): String = {
    ensureDatabaseExists()
    s"$fullUrl&table=$tableName"
  }

  /**
   * 检查表是否存在
   */
  def tableExists(tableName: String): Boolean = {
    var connection: Connection = null
    var statement: Statement = null
    var resultSet: java.sql.ResultSet = null

    try {
      connection = DriverManager.getConnection(fullUrl, getProperties())
      statement = connection.createStatement()
      resultSet = statement.executeQuery(
        s"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$database' AND table_name = '$tableName'"
      )
      if (resultSet.next()) {
        resultSet.getInt(1) > 0
      } else {
        false
      }
    } catch {
      case _: Exception => false
    } finally {
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  /**
   * 创建表（如果不存在）
   * @param tableName 表名
   * @param schemaSQL 创建表的SQL语句
   */
  def createTableIfNotExists(tableName: String, schemaSQL: String): Unit = {
    if (!tableExists(tableName)) {
      var connection: Connection = null
      var statement: Statement = null

      try {
        connection = DriverManager.getConnection(fullUrl, getProperties())
        statement = connection.createStatement()
        statement.executeUpdate(schemaSQL)
        println(s"表 '$tableName' 创建成功")
      } catch {
        case e: Exception =>
          println(s"创建表失败: ${e.getMessage}")
          throw e
      } finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    } else {
      println(s"表 '$tableName' 已存在")
    }
  }
}