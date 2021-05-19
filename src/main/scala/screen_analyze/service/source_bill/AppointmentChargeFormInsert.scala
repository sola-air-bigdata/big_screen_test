package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{ConfigDruidUtil, DruidUtil, MemberDruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
class AppointmentChargeFormInsert {
  @transient private lazy val log = Logger.getLogger(this.getClass)
   var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var connect1: Connection = _
  var connect2: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
//    log.info("********AppointmentChargeFormInsert处理json:"+jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data= new JSONObject
    var arrSize = dataArr.size()
   for( m <- 0 to arrSize-1){
     data = dataArr.getJSONObject(m)
     //更新station_show_month_total表
     try{
       connect = DruidUtil.getConnection.get
       connect1 = MemberDruidUtil.getConnection.get
       connect2 = ConfigDruidUtil.getConnection.get
       val v_member_no = data.getString("member_id")
       val stop_appointment_reson = data.getString("appoint_status")
       val pile_id = data.getInteger("pile_id")
       var l_station_id:Long = 0l
       val l_seller_id = data.getLongValue("seller_id")
       val d_add_time = data.getString("create_time")
       var s_member_type:String = null
       if("YiWanCheng".equals(stop_appointment_reson)){
       //查询会员类型
       val query_member_type = "select member_type from t_member where id=? and deleted=0"
      prepareStatement = connect1.prepareStatement(query_member_type)
      prepareStatement.setString(1, v_member_no)
      resultSet = prepareStatement.executeQuery()
      if(resultSet.next()){
        s_member_type = resultSet.getString("member_type")
      }
       log.info("会员类型*****************"+s_member_type)
       resultSet.close()
       prepareStatement.close()

         val query_station_id = "select station_id from t_pile where id = ? and deleted = 0 "
         prepareStatement = connect2.prepareStatement(query_station_id)
         prepareStatement.setInt(1,pile_id)
         resultSet = prepareStatement.executeQuery()
         if(resultSet.next()){
           l_station_id = resultSet.getLong("station_id")
         }
         resultSet.close()
         prepareStatement.close()

       if("1".equals(s_member_type)&& !StringUtil.isBlank(d_add_time)){
         val d_month_time = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
         log.info("执行更新操作************************** l_station_id："+l_station_id+" l_seller_id:"+l_seller_id+" d_month_time:"+d_month_time)
       val month_total_sql = new StringBuffer("insert into station_show_month_table(l_station_id,l_seller_id")
                             .append(",d_month_time,month_personal_booked_num) values(?,?,?,?)")
                             .append(" ON DUPLICATE KEY UPDATE")
                             .append(" month_personal_booked_num = month_personal_booked_num+?")
                prepareStatement = connect.prepareStatement(month_total_sql.toString())
                prepareStatement.setLong(1, l_station_id)
                prepareStatement.setInt(2, l_seller_id.toInt)
                prepareStatement.setString(3, d_month_time)
                prepareStatement.setLong(4, 1)
                prepareStatement.setLong(5, 1)
                prepareStatement.execute()
                prepareStatement.close()
       }
     }
       connect.close()
       connect1.close()
       connect2.close()
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
        if (connect1 != null && !connect1.isClosed()) {
          connect1.close()
        }
      } catch {
        case ex: Exception => log.info("***station_show_month_table**关闭connect失败****")
      }
       try {
        if (connect2 != null && !connect2.isClosed()) {
          connect2.close()
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
     
   }
  }
}