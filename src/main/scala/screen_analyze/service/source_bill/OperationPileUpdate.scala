package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
import screen_analyze.util.{DruidUtil, ConfigDruidUtil}
class OperationPileUpdate {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //    log.info("********OperationPileUpdate处理json:" + jsonSql.toJSONString())
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
      val l_type_id = data.getString("type_id")
      var l_station_id = data.getLongValue("station_id")
      var i_power = 0L
      val l_seller_id = data.getLongValue("seller_id")
      //旧数据
      var old_i_power = 0L
      val old_b_delete_flag = old.getString("deleted")
      val old_l_type_id = old.getString("type_id")
      
      //查询数据
      try {
        connect = ConfigDruidUtil.getConnection.get
          //查询桩的站点
          val query_station = "select id from t_station where id=? and deleted=0"
          prepareStatement = connect.prepareStatement(query_station)
          prepareStatement.setLong(1, l_station_id)
          resultSet = prepareStatement.executeQuery()
          if (!resultSet.next()) {
            l_station_id = 0
          }
          prepareStatement.close()
          resultSet.close()
          //新桩类型查询桩功率
          if (l_station_id != 0) {
            val query_i_power = "select power from t_pile_type where id=?"
            prepareStatement = connect.prepareStatement(query_i_power)
            prepareStatement.setLong(1, l_type_id.toLong)
            resultSet = prepareStatement.executeQuery()
            if (resultSet.next()) {
              i_power = (resultSet.getDouble("power") * 100).toLong
            }
            prepareStatement.close()
            resultSet.close()
          }
           //旧桩类型查询桩功率
          if (l_station_id != 0&&old_l_type_id!=null) {
            val query_i_power = "select power from t_pile_type where id=?"
            prepareStatement = connect.prepareStatement(query_i_power)
            prepareStatement.setLong(1, old_l_type_id.toLong)
            resultSet = prepareStatement.executeQuery()
            if (resultSet.next()) {
              old_i_power = (resultSet.getDouble("power") * 100).toLong
            }
            prepareStatement.close()
            resultSet.close()
          }
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
      try {
        connect = DruidUtil.getConnection.get
        log.info("========================================t_all_total更新")
        //修改b_delete_flag从1到0
        if(old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)&&l_station_id!=0){
           val all_total_sql = new StringBuffer("insert into t_all_table(l_seller_id,total_equipment_power,total_charging_pile_count)")
            .append(" values(?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_equipment_power=total_equipment_power+?,")
            .append(" total_charging_pile_count=total_charging_pile_count+?")
             prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(1, l_seller_id.toInt)
             prepareStatement.setLong(2, i_power)
             prepareStatement.setLong(4, i_power)
             prepareStatement.setLong(3, 1)
             prepareStatement.setLong(5, 1)
           prepareStatement.execute()
           prepareStatement.close()
        }
        //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)&&l_station_id!=0){
          log.info("执行修改b_delete_flag从0到1  i_power:"+i_power+"old_l_type_id:"+old_l_type_id)
          var power = i_power
          if(old_l_type_id!=null){power = old_i_power}
           val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_equipment_power=total_equipment_power+?,")
            .append(" total_charging_pile_count=total_charging_pile_count+?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
            prepareStatement.setLong(1, -power)
          prepareStatement.setLong(2, -1)
          prepareStatement.execute()
          prepareStatement.close()
        }
        //修改l_type_id
        else if(old_l_type_id!=null &&"0".equals(b_delete_flag)){
           var power = i_power
          if(old_l_type_id!=null){power = old_i_power}
           val diff_power = i_power - power
           log.info("diff_power:"+diff_power)
           val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_equipment_power=total_equipment_power+?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setLong(1, diff_power)
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
      
       //station_show_total_table更新
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag从1到0
        if(old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)&&l_station_id != 0){
           val all_total_sql = new StringBuffer("insert into station_show_total_table(l_seller_id,l_station_id,total_station_i_power_count,total_station_pile_num)")
            .append(" values(?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_station_i_power_count=total_station_i_power_count+?,")
            .append(" total_station_pile_num=total_station_pile_num+?")
             prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(1, l_seller_id.toInt)
             prepareStatement.setLong(2, l_station_id)
             prepareStatement.setLong(3, i_power)
             prepareStatement.setLong(5, i_power)
             prepareStatement.setLong(4, 1)
             prepareStatement.setLong(6, 1)
           prepareStatement.execute()
           prepareStatement.close()
        }
         //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)&&l_station_id != 0){
          var power = i_power
          if(old_l_type_id!=null){power = old_i_power}
           val all_total_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_i_power_count=total_station_i_power_count+?,")
            .append(" total_station_pile_num=total_station_pile_num+?")
            .append(" where l_seller_id=? and l_station_id=?")
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setLong(2, -1)
          prepareStatement.setLong(1, -power)
          prepareStatement.execute()
          prepareStatement.close()
        }
        //修改l_type_id
        else if(old_l_type_id!=null &&"0".equals(b_delete_flag)&&l_station_id != 0){
           var power = i_power
          if(old_l_type_id!=null){power = old_i_power}
           val diff_power = i_power - power
           val all_total_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_i_power_count=total_station_i_power_count+?")
            .append(" where l_seller_id=? and l_station_id=?" )
          prepareStatement = connect.prepareStatement(all_total_sql.toString())
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setLong(3, l_station_id)
          prepareStatement.setLong(1, diff_power)
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
       } catch {
        case ex: Exception => log.info("*****station_show_total_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_total_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_total_table**关闭prepareStatement失败****")
        }
      }
      
    }
   }
}