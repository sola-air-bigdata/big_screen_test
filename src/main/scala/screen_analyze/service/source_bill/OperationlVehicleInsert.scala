package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
import screen_analyze.util.{DruidUtil, MemberDruidUtil}
//noinspection ScalaUnreachableCode
class OperationlVehicleInsert {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
    //   log.info("********OperationlVehicleInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val b_delete_flag = data.getString("deleted")
      val l_seller_id = data.getLongValue("seller_id")
      val l_member = data.getLongValue("member_id")
      var s_member_type = ""
       //查询数据
      try {
        connect = MemberDruidUtil.getConnection.get
        if ("0".equals(b_delete_flag)) {
          //查询会员类型
          val query_member_type = "select member_type from t_member where id =? and deleted=0"
          prepareStatement = connect.prepareStatement(query_member_type)
          prepareStatement.setLong(1, l_member)
          resultSet = prepareStatement.executeQuery()
          if(resultSet.next()){
            s_member_type = resultSet.getString("member_type")
          }
          resultSet.close()
          prepareStatement.close()
        }
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
      try{
        connect = DruidUtil.getConnection.get
        if("0".equals(b_delete_flag)&&"2".equals(s_member_type)){
          val all_total_sql = new StringBuffer("insert into t_all_table(l_seller_id,total_service_team_car_num)")
            .append(" values(?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_service_team_car_num=total_service_team_car_num+?") 
            prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(1, l_seller_id.toInt)
             prepareStatement.setLong(2, 1)
             prepareStatement.setLong(3, 1)
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
      
      //t_all_member_table更新
      try{
        connect = DruidUtil.getConnection.get
        if("0".equals(b_delete_flag)&&"2".equals(s_member_type)){
           val all_total_sql = new StringBuffer("insert into t_all_member_table(l_member_id,team_car_num)")
            .append(" values(?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" team_car_num=team_car_num+?") 
            prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setLong(1, l_member)
             prepareStatement.setLong(2, 1)
             prepareStatement.setLong(3, 1)
             prepareStatement.execute()
             prepareStatement.close()
        }
        connect.close()
      } catch {
        case ex: Exception => log.info("*****t_all_member_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_member_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_member_table**关闭prepareStatement失败****")
        }
      }
      
      
    }
   }
}