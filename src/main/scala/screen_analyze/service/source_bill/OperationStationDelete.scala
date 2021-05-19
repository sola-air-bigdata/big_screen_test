package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class OperationStationDelete {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
    //    log.info("********OperationStationDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
       val b_delete_flag = data.getString("b_delete_flag")
      val l_seller_id = data.getLongValue("l_seller_id")
      val v_status = data.getString("v_status")
       //t_all_total更新
      try {
        connect = DruidUtil.getConnection.get
         if ("0".equals(b_delete_flag)) {
          val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_foreign_station_count=total_foreign_station_count-?,")
            .append(" total_internal_station_count=total_internal_station_count-?,")
            .append(" total_construction_station_count=total_construction_station_count-?,")
            .append(" total_bebuilt_station_count=total_bebuilt_station_count-?,")
            .append(" total_station_count=total_station_count-?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(6, l_seller_id.toInt)
          for(i <- 1 to 5){
            prepareStatement.setLong(i, 0L)
          }
          prepareStatement.setLong(5, 1)
          if("关闭".equals(v_status)){
            prepareStatement.setLong(2, 1)
          }else if("开启".equals(v_status)){
           prepareStatement.setLong(1, 1)
          }else if("未知".equals(v_status)){
           prepareStatement.setLong(4, 1)
          }else if("在建".equals(v_status)){
           prepareStatement.setLong(3, 1)
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