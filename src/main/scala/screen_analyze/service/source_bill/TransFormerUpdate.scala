package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class TransFormerUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //    log.info("********TransFormerUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    val mapCount = scala.collection.mutable.Map[String, Long]()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      old = oldArr.getJSONObject(m)
      //新数据
      val b_delete_flag = data.getString("b_delete_flag")
      val t_power_rate = data.getLongValue("t_power_rate")
      val station_id = data.getLongValue("station_id")
      val l_seller_id = data.getLongValue("l_seller_id")
      //旧数据
      val old_b_delete_flag = old.getString("b_delete_flag")
      val old_t_power_rate = old.getLongValue("t_power_rate")
      val old_t_power = old.getString("t_power_rate")
      var power_rate = t_power_rate
      if (old_t_power != null) { power_rate = old_t_power_rate }
      //station_show_total_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          val total_table_sql = new StringBuffer("insert into station_show_total_table(l_station_id,l_seller_id,")
            .append(" total_station_change_power_count)")
            .append(" values(?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_station_change_power_count=total_station_change_power_count+?")
          prepareStatement = connect.prepareStatement(total_table_sql.toString())
          prepareStatement.setLong(1, station_id)
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setLong(3, t_power_rate)
          prepareStatement.setLong(4, t_power_rate)
          prepareStatement.execute()
          prepareStatement.close()
        } //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          val total_table_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_change_power_count=total_station_change_power_count-?")
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(total_table_sql.toString())
          prepareStatement.setLong(2, station_id)
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(1, power_rate)
          prepareStatement.execute()
          prepareStatement.close()
        } //修改t_power_rate
        else if (old_t_power != null && "0".equals(b_delete_flag)) {
          val diff_power = t_power_rate - old_t_power_rate
          val total_table_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_change_power_count=total_station_change_power_count+?")
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(total_table_sql.toString())
          prepareStatement.setLong(2, station_id)
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(1, diff_power)
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
      } catch {
        case ex: Exception => log.info("*****station_show_total_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_total_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_total_table**关闭prepareStatement失败****")
        }
      }
    }
  }
}