package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class OperationStationUpdate {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //    log.info("********OperationStationUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      old = oldArr.getJSONObject(m)
      //新数据
      val b_delete_flag = data.getString("deleted")
      val l_seller_id = data.getLongValue("seller_id")
      val v_status = data.getString("status")
      //旧数据
      val old_b_delete_flag = old.getString("deleted")
      val old_v_status = old.getString("status")
      
      var status = v_status
      if(old_v_status!=null){status = old_v_status}
       //t_all_total更新
      try {
        connect = DruidUtil.getConnection.get
         //修改b_delete_flag从1到0
        if(old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)){
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
         //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)){
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
          if("2".equals(status)){
            prepareStatement.setLong(2, 1)
          }else if("1".equals(status)){
           prepareStatement.setLong(1, 1)
          }else if("3".equals(status)){
           prepareStatement.setLong(4, 1)
          }else if("4".equals(status)){
           prepareStatement.setLong(3, 1)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        //修改v_status
        else if (old_v_status!=null&& "0".equals(b_delete_flag)){
           val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_foreign_station_count=total_foreign_station_count+?,")
            .append(" total_internal_station_count=total_internal_station_count+?,")
            .append(" total_construction_station_count=total_construction_station_count+?,")
            .append(" total_bebuilt_station_count=total_bebuilt_station_count+?")
            .append(" where l_seller_id=?")
            prepareStatement = connect.prepareStatement(all_total_sql.toString())
            prepareStatement.setInt(5, l_seller_id.toInt)
            for(i <- 1 to 4){
            prepareStatement.setLong(i, 0L)
          }
           //减少旧数据
           if("2".equals(old_v_status)){
            prepareStatement.setLong(2, -1)
          }else if("1".equals(old_v_status)){
           prepareStatement.setLong(1, -1)
          }else if("3".equals(old_v_status)){
           prepareStatement.setLong(4, -1)
          }else if("4".equals(old_v_status)){
           prepareStatement.setLong(3, -1)
          }
           //增加新数据
           if("2".equals(v_status)){
            prepareStatement.setLong(2, 1)
          }else if("1".equals(v_status)){
           prepareStatement.setLong(1, 1)
          }else if("3".equals(v_status)){
           prepareStatement.setLong(4, 1)
          }else if("4".equals(v_status)){
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