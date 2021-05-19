package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.StringUtil
import screen_analyze.util.TimeConUtil
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class OperationPileBillBandDelete {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
    //    log.info("********OperationPileBillBandDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val l_pile_bill_id = data.getLongValue("l_pile_bill_id")
      val b_delete_flag = data.getString("b_delete_flag")
      val m_power = data.getLongValue("m_power")
      val d_add_time = data.getString("d_add_time")
      val l_station_id = data.getIntValue("l_station_id")
      val i_band_type = data.getString("i_band_type")
      var l_seller_id = 0L
      var dayd = ""
      if ("0".equals(b_delete_flag)) {
        if (!StringUtil.isBlank(d_add_time)) { dayd = TimeConUtil.timeConversion(d_add_time, "%Y-%m-%d") }
        //查询数据
        try {
          connect = DruidUtil.getConnection.get
          val query_l_seller_id = "select l_seller_id from t_origin_history_operation_bill where id=?"
          prepareStatement = connect.prepareStatement(query_l_seller_id)
          prepareStatement.setLong(1, l_pile_bill_id)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            l_seller_id = resultSet.getLong("l_seller_id")
          }
          prepareStatement.close()
          resultSet.close()
          connect.close()
        } catch {
          case ex: Exception => log.info("*****查询数据**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***查询数据**关闭connect失败****")
          }
          try {
            if (prepareStatement != null && !prepareStatement.isClosed()) {
              prepareStatement.close()
            }
          } catch {
            case ex: Exception => log.info("***查询数据**关闭prepareStatement失败****")
          }
          try {
            if (resultSet != null && !resultSet.isClosed()) {
              resultSet.close()
            }
          } catch {
            case ex: Exception => log.info("***查询数据**关闭resultSet失败****")
          }
        }

        //station_show_day_table
        try {
          connect = DruidUtil.getConnection.get
          val show_day_table_sql = new StringBuffer("update station_show_day_table set")
            .append(" day_station_feng_power_count=day_station_feng_power_count-?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count-?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count-?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setInt(5, l_seller_id.toInt)
          prepareStatement.setString(6, dayd)
          for (i <- 1 to 3) {
            prepareStatement.setLong(i, 0L)
          }
          if ("2".equals(i_band_type)) {
            prepareStatement.setLong(1, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(2, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(3, m_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()
        } catch {
          case ex: Exception => log.info("*****station_show_day_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭connect失败****")
          }
          try {
            if (prepareStatement != null && !prepareStatement.isClosed()) {
              prepareStatement.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭prepareStatement失败****")
          }
        }

        //station_show_total_table
        try {
          connect = DruidUtil.getConnection.get
          val show_day_table_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_jian_power_count=total_station_jian_power_count-?,")
            .append(" total_station_feng_power_count=total_station_feng_power_count-?,")
            .append(" total_station_ping_power_count=total_station_ping_power_count-?,")
            .append(" total_station_gu_power_count=total_station_gu_power_count-?")
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(5, l_station_id)
          prepareStatement.setInt(6, l_seller_id.toInt)
          for (i <- 1 to 4) {
            prepareStatement.setLong(i, 0L)
          }
          if ("1".equals(i_band_type)) {
            prepareStatement.setLong(1, m_power)
          } else if ("2".equals(i_band_type)) {
            prepareStatement.setLong(2, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(3, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(4, m_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
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
}