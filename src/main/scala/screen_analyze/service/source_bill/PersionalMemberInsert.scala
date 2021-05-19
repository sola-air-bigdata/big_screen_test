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
class PersionalMemberInsert {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
    //   log.info("********PersionalMemberInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val s_member_type = data.getString("member_type")
      val d_add_time = data.getString("create_time")
      val b_delete_flag = data.getString("deleted")
      val l_seller_id = data.getLongValue("seller_id")
      val v_telephone = data.getString("telephone")
      //更新t_all_day_table
      try {
        connect = DruidUtil.getConnection.get
        if (!StringUtil.isBlank(d_add_time) && "0".equals(b_delete_flag)) {
          val dayd = TimeConUtil.timeConversion(d_add_time, "%Y-%m-%d")
          val day_total_sql = new StringBuffer("insert into t_all_day_table(l_seller_id,d_day_time,day_growth_team_user,day_growth_personal_user)")
            .append(" values(?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" day_growth_team_user=day_growth_team_user+?,")
            .append(" day_growth_personal_user=day_growth_personal_user+?")
          prepareStatement = connect.prepareStatement(day_total_sql.toString())
          prepareStatement.setInt(1, l_seller_id.toInt)
          prepareStatement.setString(2, dayd)
          if ("1".equals(s_member_type)) {
            prepareStatement.setLong(3, 0)
            prepareStatement.setLong(4, 1)
            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(6, 1)
          } else {
            prepareStatement.setLong(3, 1)
            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(6, 0)
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
          val day_total_sql = new StringBuffer("insert into t_all_month_table(l_seller_id,d_month_time,month_growth_team_user,month_growth_personal_user)")
            .append(" values(?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_growth_team_user=month_growth_team_user+?,")
            .append(" month_growth_personal_user=month_growth_personal_user+?")
          prepareStatement = connect.prepareStatement(day_total_sql.toString())
          prepareStatement.setInt(1, l_seller_id.toInt)
          prepareStatement.setString(2, dayd)
          if ("1".equals(s_member_type)) {
            prepareStatement.setLong(3, 0)
            prepareStatement.setLong(4, 1)
            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(6, 1)
          } else {
            prepareStatement.setLong(3, 1)
            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(6, 0)
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
        if("0".equals(b_delete_flag)){
          var isNotNinePersonal = 0
          var isPersonal =0
          var isTeamMem = 0;
          if( !v_telephone.startsWith("90000") &&"1".equals(s_member_type)){
            isNotNinePersonal = 1;
          }
          if("1".equals(s_member_type)){
            isPersonal=1
          }else{
            isTeamMem=1;
          }

          val all_total_sql = new StringBuffer("insert into t_all_table(l_seller_id,personal_service_user_num,total_service_user_count,total_service_team_user_count,total_service_personal_user_count)")
            .append(" values(?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" personal_service_user_num=personal_service_user_num+?")
            .append(" total_service_user_count=total_service_user_count+?")
            .append(" total_service_team_user_count=total_service_team_user_count+?")
            .append(" total_service_personal_user_count=total_service_personal_user_count+?")
             prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(1, l_seller_id.toInt)
             prepareStatement.setLong(2, isNotNinePersonal)
             prepareStatement.setLong(3, 1)
             prepareStatement.setLong(4, isTeamMem)
             prepareStatement.setLong(5, isPersonal)
             prepareStatement.setLong(6, isNotNinePersonal)
             prepareStatement.setLong(7, 1)
             prepareStatement.setLong(8, isTeamMem)
             prepareStatement.setLong(9, isPersonal)
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