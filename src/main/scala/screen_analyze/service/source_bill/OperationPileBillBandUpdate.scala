package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{BillDruidUtil, ConfigDruidUtil, DruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
//noinspection ScalaUnreachableCode
class OperationPileBillBandUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var connect1: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //   log.info("********OperationPileAlarmHisUpdate处理json:" + jsonSql.toJSONString())
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
      val l_pile_bill_id = data.getLongValue("bill_id")
      val b_delete_flag = data.getString("deleted")
      val m_power = data.getLongValue("power")
      val d_add_time = data.getString("create_time")
      var l_station_id = 0L
      val i_band_type = data.getString("band_type")
      var l_seller_id = 0L
      var dayd = ""
      if (!StringUtil.isBlank(d_add_time)) { dayd = TimeConUtil.timeConversion(d_add_time, "%Y-%m-%d") }
      //旧数据
      val old_b_delete_flag = old.getString("deleted")
      val old_m_power = old.getLongValue("power")
      val old_power = old.getString("power")
      val old_d_add_time = old.getString("create_time")
      val old_i_band_type = old.getString("band_type")
      var pile_id = 0L
      var band_type = i_band_type
      var power = m_power
      var isday = dayd
      if (old_d_add_time != null) { isday = TimeConUtil.timeConversion(old_d_add_time, "%Y-%m-%d") }
      if (old_power != null) { power = old_m_power }
      if (old_i_band_type != null) { band_type = old_i_band_type }
      //查询数据
      try {
        connect = BillDruidUtil.getConnection.get
        connect1 = ConfigDruidUtil.getConnection.get
        val query_l_seller_id = "select seller_id,pile_id from t_bill where id=?"
        prepareStatement = connect.prepareStatement(query_l_seller_id)
        prepareStatement.setLong(1, l_pile_bill_id)
        resultSet = prepareStatement.executeQuery()
        if (resultSet.next()) {
          l_seller_id = resultSet.getLong("seller_id")
          pile_id = resultSet.getLong("pile_id")
        }
        prepareStatement.close()
        resultSet.close()

        val query_station_id = "select station_id from t_pile where id = ? "
        prepareStatement = connect1.prepareStatement(query_station_id)
        prepareStatement.setLong(1,pile_id)
        resultSet = prepareStatement.executeQuery()
        if(resultSet.next()){
          l_station_id = resultSet.getLong("station_id")
        }
        prepareStatement.close()
        resultSet.close()


        connect.close()
        connect1.close()
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
          if (connect1 != null && !connect1.isClosed()) {
            connect1.close()
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
        //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag) && l_seller_id != 0) {
          val show_day_table_sql = new StringBuffer("insert into station_show_day_table(l_station_id,l_seller_id,d_day_time,")
            .append(" day_station_feng_power_count,day_station_ping_power_count,day_station_gu_power_count)")
            .append(" values(?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" day_station_feng_power_count=day_station_feng_power_count+?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count+?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count+?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(1, l_station_id)
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setString(3, dayd)
          for (i <- 4 to 9) {
            prepareStatement.setLong(i, 0L)
          }
          if ("2".equals(i_band_type)) {
            prepareStatement.setLong(4, m_power)
            prepareStatement.setLong(7, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(5, m_power)
            prepareStatement.setLong(8, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(6, m_power)
            prepareStatement.setLong(9, m_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          val show_day_table_sql = new StringBuffer("update station_show_day_table set")
            .append(" day_station_feng_power_count=day_station_feng_power_count-?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count-?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count-?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setInt(5, l_seller_id.toInt)
          prepareStatement.setString(6, isday)
          for (i <- 1 to 3) {
            prepareStatement.setLong(i, 0L)
          }
          if ("2".equals(band_type)) {
            prepareStatement.setLong(1, power)
          } else if ("3".equals(band_type)) {
            prepareStatement.setLong(2, power)
          } else if ("4".equals(band_type)) {
            prepareStatement.setLong(3, power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改d_add_time
        else if (old_d_add_time != null && !isday.equals(dayd) && "0".equals(b_delete_flag)) {
          //减少旧日期数据
          val show_day_table_sql = new StringBuffer("update station_show_day_table set")
            .append(" day_station_feng_power_count=day_station_feng_power_count-?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count-?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count-?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setInt(5, l_seller_id.toInt)
          prepareStatement.setString(6, isday)
          for (i <- 1 to 3) {
            prepareStatement.setLong(i, 0L)
          }
          if ("2".equals(band_type)) {
            prepareStatement.setLong(1, power)
          } else if ("3".equals(band_type)) {
            prepareStatement.setLong(2, power)
          } else if ("4".equals(band_type)) {
            prepareStatement.setLong(3, power)
          }
          prepareStatement.execute()
          prepareStatement.close()
          //增加新日期数据
          val show_day_table_sql1 = new StringBuffer("insert into station_show_day_table(l_station_id,l_seller_id,d_day_time,")
            .append(" day_station_feng_power_count,day_station_ping_power_count,day_station_gu_power_count)")
            .append(" values(?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" day_station_feng_power_count=day_station_feng_power_count+?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count+?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count+?")
          prepareStatement = connect.prepareStatement(show_day_table_sql1.toString())
          prepareStatement.setLong(1, l_station_id)
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setString(3, dayd)
          for (i <- 4 to 9) {
            prepareStatement.setLong(i, 0L)
          }
          if ("2".equals(i_band_type)) {
            prepareStatement.setLong(4, m_power)
            prepareStatement.setLong(7, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(5, m_power)
            prepareStatement.setLong(8, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(6, m_power)
            prepareStatement.setLong(9, m_power)
          }
          prepareStatement.execute()
          prepareStatement.close()

        } //修改i_band_type
        else if (old_i_band_type != null && "0".equals(b_delete_flag)) {
          val show_day_table_sql = new StringBuffer("update station_show_day_table set")
            .append(" day_station_feng_power_count=day_station_feng_power_count+?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count+?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count+?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setInt(5, l_seller_id.toInt)
          prepareStatement.setString(6, dayd)
          for (i <- 1 to 3) {
            prepareStatement.setLong(i, 0L)
          }
          //增加新数据
          if ("2".equals(i_band_type)) {
            prepareStatement.setLong(1, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(2, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(3, m_power)
          }
          //减少旧数据
          if ("2".equals(old_i_band_type)) {
            prepareStatement.setLong(1, -power)
          } else if ("3".equals(old_i_band_type)) {
            prepareStatement.setLong(2, -power)
          } else if ("4".equals(old_i_band_type)) {
            prepareStatement.setLong(3, -power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改m_power
        else if (old_power != null && "0".equals(b_delete_flag)) {
          val diff_power = m_power - old_m_power
          val show_day_table_sql = new StringBuffer("update station_show_day_table set")
            .append(" day_station_feng_power_count=day_station_feng_power_count+?,")
            .append(" day_station_ping_power_count=day_station_ping_power_count+?,")
            .append(" day_station_gu_power_count=day_station_gu_power_count+?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setInt(5, l_seller_id.toInt)
          prepareStatement.setString(6, dayd)
          for (i <- 1 to 3) {
            prepareStatement.setLong(i, 0L)
          }
          if ("2".equals(i_band_type)) {
            prepareStatement.setLong(1, diff_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(2, diff_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(3, diff_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
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
        //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag) && l_seller_id != 0) {
          val show_day_table_sql = new StringBuffer("insert into station_show_total_table(l_station_id,l_seller_id,")
            .append(" total_station_jian_power_count,total_station_feng_power_count,total_station_ping_power_count,total_station_gu_power_count)")
            .append(" values(?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_station_jian_power_count=total_station_jian_power_count+?,")
            .append(" total_station_feng_power_count=total_station_feng_power_count+?,")
            .append(" total_station_ping_power_count=total_station_ping_power_count+?,")
            .append(" total_station_gu_power_count=total_station_gu_power_count+?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(1, l_station_id)
          prepareStatement.setInt(2, l_seller_id.toInt)
          for (i <- 3 to 10) {
            prepareStatement.setLong(i, 0L)
          }
          if ("1".equals(i_band_type)) {
            prepareStatement.setLong(3, m_power)
            prepareStatement.setLong(7, m_power)
          } else if ("2".equals(i_band_type)) {
            prepareStatement.setLong(4, m_power)
            prepareStatement.setLong(8, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(5, m_power)
            prepareStatement.setLong(9, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(6, m_power)
            prepareStatement.setLong(10, m_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
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
          if ("1".equals(band_type)) {
            prepareStatement.setLong(1, power)
          } else if ("2".equals(band_type)) {
            prepareStatement.setLong(2, power)
          } else if ("3".equals(band_type)) {
            prepareStatement.setLong(3, power)
          } else if ("4".equals(band_type)) {
            prepareStatement.setLong(4, power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改i_band_type
        else if (old_i_band_type != null && "0".equals(b_delete_flag)) {
          val show_day_table_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_jian_power_count=total_station_jian_power_count+?,")
            .append(" total_station_feng_power_count=total_station_feng_power_count+?,")
            .append(" total_station_ping_power_count=total_station_ping_power_count+?,")
            .append(" total_station_gu_power_count=total_station_gu_power_count+?")
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(5, l_station_id)
          prepareStatement.setInt(6, l_seller_id.toInt)
          for (i <- 1 to 4) {
            prepareStatement.setLong(i, 0L)
          }
          //增加新数据
          if ("1".equals(i_band_type)) {
            prepareStatement.setLong(1, m_power)
          } else if ("2".equals(i_band_type)) {
            prepareStatement.setLong(2, m_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(3, m_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(4, m_power)
          }
          //减少旧数据
          if ("1".equals(old_i_band_type)) {
            prepareStatement.setLong(1, -power)
          } else if ("2".equals(old_i_band_type)) {
            prepareStatement.setLong(2, -power)
          } else if ("3".equals(old_i_band_type)) {
            prepareStatement.setLong(3, -power)
          } else if ("4".equals(old_i_band_type)) {
            prepareStatement.setLong(4, -power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改m_power
        else if (old_power != null && "0".equals(b_delete_flag)) {
          val diff_power = m_power - old_m_power
          val show_day_table_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_jian_power_count=total_station_jian_power_count+?,")
            .append(" total_station_feng_power_count=total_station_feng_power_count+?,")
            .append(" total_station_ping_power_count=total_station_ping_power_count+?,")
            .append(" total_station_gu_power_count=total_station_gu_power_count+?")
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(show_day_table_sql.toString())
          prepareStatement.setLong(5, l_station_id)
          prepareStatement.setInt(6, l_seller_id.toInt)
          for (i <- 1 to 4) {
            prepareStatement.setLong(i, 0L)
          }
          //增加新数据
          if ("1".equals(i_band_type)) {
            prepareStatement.setLong(1, diff_power)
          } else if ("2".equals(i_band_type)) {
            prepareStatement.setLong(2, diff_power)
          } else if ("3".equals(i_band_type)) {
            prepareStatement.setLong(3, diff_power)
          } else if ("4".equals(i_band_type)) {
            prepareStatement.setLong(4, diff_power)
          }
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