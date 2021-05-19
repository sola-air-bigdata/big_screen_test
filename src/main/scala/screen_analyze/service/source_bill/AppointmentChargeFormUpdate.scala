package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{ConfigDruidUtil, DruidUtil, MemberDruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
//noinspection ScalaUnreachableCode
class AppointmentChargeFormUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
//    log.info("********AppointmentChargeFormUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      log.info(data)
      old = oldArr.getJSONObject(m)
      log.info(old)
      //新数据
      val v_member_no = data.getString("member_id")
      val stop_appointment_reson = data.getString("appoint_status")
      var l_station_id:Long = 0l
      val l_seller_id = data.getLongValue("seller_id")
      val d_add_time = data.getString("d_add_time")
      var s_member_type = ""
      val pile_id = data.getInteger("pile_id")
      //旧数据
      val old_stop_appointment_reson = old.getString("appoint_status")
      val old_d_add_time = old.getString("create_time")

      //查询数据
      try {
        connect = MemberDruidUtil.getConnection.get
        //查询会员类型
        val query_member_type = "select member_type from t_member where id=? and deleted=0"
        prepareStatement = connect.prepareStatement(query_member_type)
        prepareStatement.setString(1, v_member_no)
        resultSet = prepareStatement.executeQuery()
        if (resultSet.next()) {
          s_member_type = resultSet.getString("member_type")
        }
        resultSet.close()
        prepareStatement.close()
        connect.close()
      } catch {
        case ex: Exception => log.info("*****station_show_month_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement失败****")
        }
        try {
          if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭resultSet失败****")
        }
      }


      //查询数据
      try {
        connect = ConfigDruidUtil.getConnection.get
        //查询站点ID
        val query_member_type = "select station_id from t_pile where id = ? and deleted = 0 "
        prepareStatement = connect.prepareStatement(query_member_type)
        prepareStatement.setInt(1,pile_id)
        resultSet = prepareStatement.executeQuery()
        if (resultSet.next()) {
          l_station_id = resultSet.getLong("station_id")
        }
        resultSet.close()
        prepareStatement.close()
        connect.close()
      } catch {
        case ex: Exception => log.info("*****station_show_month_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement失败****")
        }
        try {
          if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭resultSet失败****")
        }
      }

      //更新station_show_month_total表
      if ("1".equals(s_member_type) && !StringUtil.isBlank(d_add_time)) {
        try {
          val dayd_month = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
          connect = DruidUtil.getConnection.get
          var appointment_reson = stop_appointment_reson
          if (old_stop_appointment_reson != null) { appointment_reson = old_stop_appointment_reson }
          //时间修改
          if (old_d_add_time != null) {
            val old_dayd_month = TimeConUtil.timeConversion(old_d_add_time, "%Y-%m")
            if (!dayd_month.equals(old_dayd_month)) {
              //减少旧日期数据
              if ("YiWanCheng".equals(appointment_reson)) {
                val month_total_sql = new StringBuffer("update station_show_month_table set")
                  .append(" month_personal_booked_num = month_personal_booked_num-?")
                  .append(" where l_station_id=? and l_seller_id=? and d_month_time=? ")
                prepareStatement = connect.prepareStatement(month_total_sql.toString())
                prepareStatement.setLong(2, l_station_id)
                prepareStatement.setInt(3, l_seller_id.toInt)
                prepareStatement.setString(4, old_dayd_month)
                prepareStatement.setLong(1, 1)
                prepareStatement.execute()
                prepareStatement.close()
              }
              //增加新日期数据
              if ("YiWanCheng".equals(stop_appointment_reson)) {
                val month_total_sql = new StringBuffer("insert into station_show_month_table(l_station_id,l_seller_id")
                  .append(",d_month_time,month_personal_booked_num) values(?,?,?,?)")
                  .append(" ON DUPLICATE KEY UPDATE")
                  .append(" month_personal_booked_num = month_personal_booked_num+?")
                prepareStatement = connect.prepareStatement(month_total_sql.toString())
                prepareStatement.setLong(1, l_station_id)
                prepareStatement.setInt(2, l_seller_id.toInt)
                prepareStatement.setString(3, dayd_month)
                prepareStatement.setLong(4, 1)
                prepareStatement.setLong(5, 1)
                prepareStatement.execute()
                prepareStatement.close()
              }
            }
          } //停止原因修改
          else if (old_stop_appointment_reson != null) {
            //stop_appointment_reson由YiWanCheng修改为其他
            if ("QiDong".equals(old_stop_appointment_reson)) {
              val month_total_sql = new StringBuffer("update station_show_month_table set")
                .append(" month_personal_booked_num = month_personal_booked_num-?")
                .append(" where l_station_id=? and l_seller_id=? and d_month_time=? ")
              prepareStatement = connect.prepareStatement(month_total_sql.toString())
              prepareStatement.setLong(2, l_station_id)
              prepareStatement.setInt(3, l_seller_id.toInt)
              prepareStatement.setString(4, dayd_month)
              prepareStatement.setLong(1, 1)
              prepareStatement.execute()
              prepareStatement.close()
            } //stop_appointment_reson由其他修改为QiDong
            else if ("QiDong".equals(stop_appointment_reson)) {
              val month_total_sql = new StringBuffer("update station_show_month_table set")
                .append(" month_personal_booked_num = month_personal_booked_num+?")
                .append(" where l_station_id=? and l_seller_id=? and d_month_time=? ")
              prepareStatement = connect.prepareStatement(month_total_sql.toString())
              prepareStatement.setLong(2, l_station_id)
              prepareStatement.setInt(3, l_seller_id.toInt)
              prepareStatement.setString(4, dayd_month)
              prepareStatement.setLong(1, 1)
              prepareStatement.execute()
              prepareStatement.close()
            }
          }
          connect.close()
        } catch {
          case ex: Exception => log.info("*****station_show_month_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭connect失败****")
          }
          try {
            if (prepareStatement != null && !prepareStatement.isClosed()) {
              prepareStatement.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement失败****")
          }
        }
      }
    }
  }
}