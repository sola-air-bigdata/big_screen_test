package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
import screen_analyze.util.{DruidUtil, ConfigDruidUtil}
class OperationPileDelete {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
    //   log.info("********OperationPileDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)

      val b_delete_flag = data.getString("deleted")

      val l_type_id = data.getLongValue("type_id")
      var l_station_id = data.getLongValue("station_id")
      var i_power = 0L
      val l_seller_id = data.getLongValue("seller_id")
      //查询数据
      try {
        connect = ConfigDruidUtil.getConnection.get
        if ("0".equals(b_delete_flag)) {
          //查询桩的站点
          val query_station = "select id from t_station where id=? and deleted=0"
          prepareStatement = connect.prepareStatement(query_station)
          prepareStatement.setLong(1, l_station_id)
          resultSet = prepareStatement.executeQuery()
          if (!resultSet.next()) {
            l_station_id = 0
          }
          prepareStatement.close()
          resultSet.close()
          //查询桩功率
          if (l_station_id != 0) {
            val query_i_power = "select power from t_pile_type where id=?"
            prepareStatement = connect.prepareStatement(query_i_power)
            prepareStatement.setLong(1, l_type_id)
            resultSet = prepareStatement.executeQuery()
            if (resultSet.next()) {
              i_power = (resultSet.getDouble("power") * 100).toLong
            }
            prepareStatement.close()
            resultSet.close()
          }
        }
        connect.close()



        connect = DruidUtil.getConnection.get
        if ("0".equals(b_delete_flag)) {
          val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_equipment_power=total_equipment_power-?,")
            .append(" total_charging_pile_count=total_charging_pile_count-?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(1, i_power)
          if (l_station_id != 0) {
            prepareStatement.setLong(2, 1)
          } else {
            prepareStatement.setLong(2, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }


        connect = DruidUtil.getConnection.get
        if ("0".equals(b_delete_flag) && l_station_id != 0) {
          val all_total_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_i_power_count=total_station_i_power_count-?,")
            .append(" total_station_pile_num=total_station_pile_num-?")
            .append(" where l_seller_id=? and l_station_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setLong(1, i_power)
          prepareStatement.setLong(2, 1)
          prepareStatement.execute()
          prepareStatement.close()
        }
        log.info("station_show_total_table更新完成")
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
      //t_all_total更新
      try {
        connect = DruidUtil.getConnection.get
        if ("0".equals(b_delete_flag)) {
          val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_equipment_power=total_equipment_power-?,")
            .append(" total_charging_pile_count=total_charging_pile_count-?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(1, i_power)
          if (l_station_id != 0) {
            prepareStatement.setLong(2, 1)
          } else {
            prepareStatement.setLong(2, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        log.info("t_all_total更新完成")
        connect.close()
      } catch {
        case ex: Exception => log.info("*****t_all_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_table**关闭prepareStatement失败****")
        }
      }
      //station_show_total_table更新
      try {
        connect = DruidUtil.getConnection.get
        if ("0".equals(b_delete_flag) && l_station_id != 0) {
          val all_total_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_i_power_count=total_station_i_power_count-?,")
            .append(" total_station_pile_num=total_station_pile_num-?")
            .append(" where l_seller_id=? and l_station_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setLong(1, i_power)
          prepareStatement.setLong(2, 1)
          prepareStatement.execute()
          prepareStatement.close()
        }
        log.info("station_show_total_table更新完成")
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