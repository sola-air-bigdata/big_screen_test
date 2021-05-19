package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.StringUtil
import screen_analyze.util.TimeConUtil
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
//noinspection ScalaUnreachableCode
class OperationPileAlarmHisInsert {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
    //   log.info("********OperationPileAlarmHisInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
//      var pile_exist = -1;
      val b_delete_flag = data.getString("deleted")
      val l_alarm_define_id = data.getString("alarm_define_id")
      val d_add_time = data.getString("create_time")
      val l_seller_id = data.getLongValue("seller_id")
      val l_pile_id = data.getLongValue("pile_id")
      var is_new_pile = 0
      if (!"51".equals(l_alarm_define_id) && !StringUtil.isBlank(d_add_time)&&"0".equals(b_delete_flag)) {
        val current_time = TimeConUtil.getMonth(d_add_time, "1")
        val next_time = TimeConUtil.getMonth(d_add_time, "0")
        val day_month = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
        try {
          connect = DruidUtil.getConnection.get
          //累加桩警告数
          val addAlarmSql = new StringBuffer("insert into t_all_month_alarm_table(l_seller_id,l_pile_id,alarm_date,alarm_count)")
            .append("values(?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" alarm_count=alarm_count+?")
          prepareStatement = connect.prepareStatement(addAlarmSql.toString())
          prepareStatement.setLong(1,l_seller_id)
          prepareStatement.setLong(2,l_pile_id)
          prepareStatement.setString(3,day_month)
          prepareStatement.setLong(4,1)
          prepareStatement.setLong(5,1)
          prepareStatement.execute()
          //查询历史警告数数据
          val query_pilealarm_count = "select alarm_count from t_all_month_alarm_table where l_seller_id=? and l_pile_id=? and alarm_date=?"
          prepareStatement = connect.prepareStatement(query_pilealarm_count.toString())
          prepareStatement.setLong(1, l_seller_id)
          prepareStatement.setLong(2, l_pile_id)
          prepareStatement.setString(3, day_month)
          resultSet = prepareStatement.executeQuery()
          var alarmCount = 0L;
          if(resultSet.next()){
            alarmCount = resultSet.getLong("alarm_count")
            if (alarmCount==1){
              is_new_pile=1;
            }
          }

//          if(pile_exist==1) {
//            val query_pile = new StringBuffer("SELECT count( * ) idNum FROM t_origin_history_operation_pile_alarm_his tah " +
//              " WHERE tah.b_delete_flag = 0  AND tah.l_alarm_define_id <> 51  AND " +
//              "  timediff( tah.d_add_time, ? ) > 0  AND timediff( tah.d_add_time, ? ) < 0  AND tah.l_seller_id = ? AND " +
//              " tah.l_pile_id = ?")
//            prepareStatement = connect.prepareStatement(query_pile.toString())
//            prepareStatement.setString(1, current_time)
//            prepareStatement.setString(2, next_time)
//            prepareStatement.setLong(3, l_seller_id)
//            prepareStatement.setLong(4, l_pile_id)
//            resultSet = prepareStatement.executeQuery()
//            resultSet.last()
//           val count = resultSet.getInt("idNum");
//            if (count == 1) {
//              is_new_pile = 1
//            }
//          }
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
            val month_total_sql = new StringBuffer("insert into t_all_month_table(l_seller_id,d_month_time,month_fault_pile)")
              .append(" values(?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_fault_pile=month_fault_pile+?")
            prepareStatement = connect.prepareStatement(month_total_sql.toString())
            prepareStatement.setInt(1, l_seller_id.toInt)
            prepareStatement.setString(2, day_month)
            prepareStatement.setLong(3, is_new_pile)
            prepareStatement.setLong(4, is_new_pile)
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