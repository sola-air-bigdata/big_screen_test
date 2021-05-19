package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.StringUtil
import screen_analyze.util.TimeConUtil
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class OperationPileAlarmHisDelete {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
    //  log.info("********OperationPileAlarmHisDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val b_delete_flag = data.getString("b_delete_flag")
      val l_alarm_define_id = data.getString("l_alarm_define_id")
      val d_add_time = data.getString("d_add_time")
      val l_seller_id = data.getLongValue("l_seller_id")
      val l_pile_id = data.getLongValue("l_pile_id")
      var is_new_pile = 0
      if (!"51".equals(l_alarm_define_id)&& !StringUtil.isBlank(d_add_time)&&"0".equals(b_delete_flag)) {
        val current_time = TimeConUtil.getMonth(d_add_time, "1")
        val next_time = TimeConUtil.getMonth(d_add_time, "0")
        val day_month = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
        //查询数据
        try {
          connect = DruidUtil.getConnection.get
          val query_pile = new StringBuffer("select tah.l_pile_id from t_origin_history_operation_pile_alarm_his tah LEFT JOIN t_business_base_operation_pile tp ON tah.l_pile_id = tp.id" +
            " where tah.b_delete_flag = 0 AND tah.l_alarm_define_id <> 51 AND tp.b_delete_flag = 0")
            .append(" and timediff(tah.d_add_time,?)>0 and timediff(tah.d_add_time,?)<0 and tah.l_seller_id=? and tah.l_pile_id=?")
          prepareStatement = connect.prepareStatement(query_pile.toString())
          prepareStatement.setString(1, current_time)
          prepareStatement.setString(2, next_time)
          prepareStatement.setLong(3, l_seller_id)
          prepareStatement.setLong(4, l_pile_id)
          resultSet = prepareStatement.executeQuery()
          if (!resultSet.next()) {
            is_new_pile = 1
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
        //更新all_month_total
        try {
          connect = DruidUtil.getConnection.get
          val month_total_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_fault_pile=month_fault_pile-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setString(3, day_month)
          prepareStatement.setLong(1, is_new_pile)
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()
        } catch {
          case ex: Exception => log.info("*****t_all_month_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***t_all_month_table**关闭connect失败****")
          }
          try {
            if (prepareStatement != null && !prepareStatement.isClosed()) {
              prepareStatement.close()
            }
          } catch {
            case ex: Exception => log.info("***t_all_month_table**关闭prepareStatement失败****")
          }
        }
        
        
      }
    }
  }
}