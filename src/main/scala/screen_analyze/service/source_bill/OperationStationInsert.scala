package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class OperationStationInsert {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
    //    log.info("********OperationStationInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val b_delete_flag = data.getString("deleted")
      val l_seller_id = data.getLongValue("seller_id")
      val v_status = data.getString("status")
       //t_all_total更新
      try {
        connect = DruidUtil.getConnection.get
         if ("0".equals(b_delete_flag)) {
          val all_total_sql = new StringBuffer("insert into t_all_table(l_seller_id,")
            .append("total_foreign_station_count,total_internal_station_count,total_construction_station_count,total_bebuilt_station_count,total_station_count)")
            .append(" values(?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_foreign_station_count=total_foreign_station_count+?,")
            .append(" total_internal_station_count=total_internal_station_count+?,")
            .append(" total_construction_station_count=total_construction_station_count+?,")
            .append(" total_bebuilt_station_count=total_bebuilt_station_count+?,")
            .append(" total_station_count=total_station_count+?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(1, l_seller_id.toInt)
          for(i <- 2 to 11){
            prepareStatement.setLong(i, 0L)
          }
          log.info("v_status:"+v_status+"l_seller_id:"+l_seller_id+"*************")
          prepareStatement.setLong(6, 1)
          prepareStatement.setLong(11, 1)
          if("2".equals(v_status)){
            prepareStatement.setLong(3, 1)
           prepareStatement.setLong(8, 1)
          }else if("1".equals(v_status)){
           prepareStatement.setLong(2, 1)
           prepareStatement.setLong(7, 1)
          }else if("3".equals(v_status)){
           prepareStatement.setLong(5, 1)
           prepareStatement.setLong(10, 1)
          }else if("4".equals(v_status)){
           prepareStatement.setLong(4, 1)
           prepareStatement.setLong(9, 1)
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