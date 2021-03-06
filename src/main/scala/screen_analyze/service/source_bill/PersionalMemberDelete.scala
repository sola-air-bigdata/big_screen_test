package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.StringUtil
import screen_analyze.util.TimeConUtil
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class PersionalMemberDelete {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
    //    log.info("********PersionalMemberDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val s_member_type = data.getString("s_member_type")
      val d_add_time = data.getString("d_add_time")
      val b_delete_flag = data.getString("b_delete_flag")
      val l_seller_id = data.getLongValue("l_seller_id")
      val v_telephone = data.getString("v_telephone")
      
      //更新t_all_day_table
      try {
        connect = DruidUtil.getConnection.get
        if (!StringUtil.isBlank(d_add_time) && "0".equals(b_delete_flag)) {
          val dayd = TimeConUtil.timeConversion(d_add_time, "%Y-%m-%d")
          val day_total_sql = new StringBuffer("update t_all_day_table set")
            .append(" day_growth_team_user=day_growth_team_user-?,")
            .append(" day_growth_personal_user=day_growth_personal_user-?")
            .append(" where l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(day_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setString(4, dayd)
          if ("1".equals(s_member_type)) {
            prepareStatement.setLong(1, 0)
            prepareStatement.setLong(2, 1)
          } else {
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(2, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
      } catch {
        case ex: Exception => log.info("*****t_all_day_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭prepareStatement失败****")
        }
      }
      //更新t_all_month_total
      try {
        connect = DruidUtil.getConnection.get
        if (!StringUtil.isBlank(d_add_time) && "0".equals(b_delete_flag)) {
          val dayd = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
          val day_total_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_growth_team_user=month_growth_team_user-?,")
            .append(" month_growth_personal_user=month_growth_personal_user-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(day_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setString(4, dayd)
          if ("1".equals(s_member_type)) {
            prepareStatement.setLong(2, 1)
            prepareStatement.setLong(1, 0)
          } else {
            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(1, 1)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
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
      //t_all_total更新
      try{
        connect = DruidUtil.getConnection.get
        if("0".equals(b_delete_flag)&& !v_telephone.startsWith("90000") &&"1".equals(s_member_type)){
          val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" personal_service_user_num=personal_service_user_num-?")
            .append(" where l_seller_id=?")
            prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(2, l_seller_id.toInt)
             prepareStatement.setLong(1, 1)
             prepareStatement.execute()
             prepareStatement.close()
             
        }
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
      
      
    }
   }
}