package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{BillDruidUtil, DruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
//noinspection ScalaUnreachableCode
class PersionalMemberUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //    log.info("********PersionalMemberUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      old = oldArr.getJSONObject(m)
      //新数据
      val s_member_type = data.getString("member_type")
      val d_add_time = data.getString("create_time")
      val b_delete_flag = data.getString("deleted")
      val l_seller_id = data.getLongValue("seller_id")
      var v_telephone = data.getString("telephone")
      if (StringUtil.isBlank(v_telephone)) { v_telephone = "" }
      val i_level = data.getString("level")
      val id = data.getLongValue("id")
      var isnewC = 0
      //旧数据
      var old_v_telephone = old.getString("telephone")
      val old_i_level = old.getString("level")
      val old_b_delete_flag = old.getString("deleted")
      var telephone = v_telephone
      if (old_v_telephone != null) { telephone = old_v_telephone }

      //查询数据
      try {
        connect = BillDruidUtil.getConnection.get
        val query_isnewC = "select id from t_bill where member_id=? and finish=1 and deleted=0"
        prepareStatement = connect.prepareStatement(query_isnewC)
        prepareStatement.setLong(1, id)
        resultSet = prepareStatement.executeQuery()
        resultSet.last()
        if (resultSet.getRow > 0) {
          isnewC = 1
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

      //更新t_all_day_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag) && !StringUtil.isBlank(d_add_time)) {
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
        } //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag) && !StringUtil.isBlank(d_add_time)) {
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
        //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag) && !StringUtil.isBlank(d_add_time)) {
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
        } //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag) && !StringUtil.isBlank(d_add_time)) {
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
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag) ) {
          var isNotNinePersonal =0
          var isPersonal = 0
          var isTeamMem = 0
          if(!v_telephone.startsWith("90000") && "1".equals(s_member_type)){
            isNotNinePersonal =1
          }
          if("1".equals(s_member_type)){
            isPersonal = 1
          }else{
            isTeamMem = 1
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
        } //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          var isNotNinePersonal =0
          var isPersonal = 0
          var isTeamMem = 0
          if(!v_telephone.startsWith("90000") && "1".equals(s_member_type)){
            isNotNinePersonal =1
          }
          if("1".equals(s_member_type)){
            isPersonal = 1
          }else{
            isTeamMem = 1
          }


          val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" personal_service_user_num=personal_service_user_num-?")
            .append(" total_service_user_count=total_service_user_count-?")
            .append(" total_service_team_user_count=total_service_team_user_count-?")
            .append(" total_service_personal_user_count=total_service_personal_user_count-?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setLong(1, isNotNinePersonal)
          prepareStatement.setLong(2, 1)
          prepareStatement.setLong(3, isTeamMem)
          prepareStatement.setLong(4, isPersonal)
          prepareStatement.setInt(5, l_seller_id.toInt)
          prepareStatement.execute()
          prepareStatement.close()
        } //修改level或v_telephone
        else if ((old_i_level != null || old_v_telephone != null) && "0".equals(b_delete_flag)) {
          val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" personal_service_user_num=personal_service_user_num+?,")
            .append(" total_service_silver_user_count=total_service_silver_user_count+?,")
            .append(" total_service_gold_user_count=total_service_gold_user_count+?,")
            .append(" total_service_platinum_user_count=total_service_platinum_user_count+?,")
            .append(" total_service_diamond_user_count=total_service_diamond_user_count+?,")
            .append(" total_service_blackgold_user_count=total_service_blackgold_user_count+?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(7, l_seller_id.toInt)
          for (j <- 1 to 6) {
            prepareStatement.setLong(j, 0)
          }
          if (old_v_telephone != null) {
            if (StringUtil.isBlank(old_v_telephone)) { old_v_telephone = "" }
            if (old_v_telephone.startsWith("90000") && !v_telephone.startsWith("90000")) {
              prepareStatement.setLong(1, 1)
            } else if (!old_v_telephone.startsWith("90000") && v_telephone.startsWith("90000")) {
              prepareStatement.setLong(1, -1)
            }
          }
          if (old_i_level != null) {
            //减少旧会员等级数据
            if ("1".equals(old_i_level)) {
              prepareStatement.setLong(2, -isnewC)
            } else if ("2".equals(old_i_level)) {
              prepareStatement.setLong(3, -isnewC)
            } else if ("3".equals(old_i_level)) {
              prepareStatement.setLong(4, -isnewC)
            } else if ("4".equals(old_i_level)) {
              prepareStatement.setLong(5, -isnewC)
            } else if ("5".equals(old_i_level)) {
              prepareStatement.setLong(6, -isnewC)
            }
            //增加新会员等级数据
            log.info("i_level:"+i_level+"isnewC:"+isnewC)
            if ("1".equals(i_level)) {
              prepareStatement.setLong(2, isnewC)
            } else if ("2".equals(i_level)) {
              prepareStatement.setLong(3, isnewC)
            } else if ("3".equals(i_level)) {
              prepareStatement.setLong(4, isnewC)
            } else if ("4".equals(i_level)) {
              prepareStatement.setLong(5, isnewC)
            } else if ("5".equals(i_level)) {
              prepareStatement.setLong(6, isnewC)
            }
          }
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