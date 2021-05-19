package screen_analyze.util
import java.util.Properties
import javax.sql.DataSource
import java.sql.Connection
import com.alibaba.druid.pool.DruidDataSourceFactory
object DruidUtil extends Serializable{
   val dataSource: Option[DataSource] = {
    try {
        val druidProps = new Properties()
        // 获取Druid连接池的配置文件
        val druidConfig = getClass.getResourceAsStream("/druid.properties")
        // 倒入配置文件
        druidProps.load(druidConfig)
        Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case error: Exception =>
        None
    }
  }
 
  // 连接方式
  def getConnection: Option[Connection] = {
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None
    }
  }
}