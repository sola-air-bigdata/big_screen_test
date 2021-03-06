package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.StringUtil
import screen_analyze.util.TimeConUtil
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil

class AppointmentChargeFormDelete {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
//    log.info("********AppointmentChargeFormDelete处理json:"+jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data= new JSONObject
    var arrSize = dataArr.size()
   for( m <- 0 to arrSize-1){
     data = dataArr.getJSONObject(m)
     //更新station_show_month_total表
     try{
       connect = DruidUtil.getConnection.get
       val v_member_no = data.getString("v_member_no")
       val stop_appointment_reson = data.getString("stop_appointment_reson")
       val l_station_id = data.getLongValue("l_station_id")
       val l_seller_id = data.getLongValue("l_seller_id")
       val d_add_time = data.getString("d_add_time")
       var s_member_type:String = null
       if("QiDong".equals(stop_appointment_reson)){
       //查询会员类型
       val query_member_type = "select s_member_type from t_business_base_operation_persional_member where v_telephone=? and b_delete_flag=0"
      prepareStatement = connect.prepareStatement(query_member_type)
      prepareStatement.setString(1, v_member_no)
      resultSet = prepareStatement.executeQuery()
      if(resultSet.next()){
        s_member_type = resultSet.getString("s_member_type")
      }
       resultSet.close()
       prepareStatement.close()
       if("1".equals(s_member_type)&& !StringUtil.isBlank(d_add_time)){
       val d_month_time = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
       val month_total_sql = new StringBuffer("update station_show_month_table set")
                             .append(" month_personal_booked_num = month_personal_booked_num-?")
                             .append(" where l_station_id=? and l_seller_id=? and d_month_time=? ")
                prepareStatement = connect.prepareStatement(month_total_sql.toString())
                prepareStatement.setLong(2, l_station_id)
                prepareStatement.setInt(3, l_seller_id.toInt)
                prepareStatement.setString(4, d_month_time)
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