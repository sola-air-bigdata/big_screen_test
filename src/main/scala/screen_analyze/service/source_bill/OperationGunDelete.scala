package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class OperationGunDelete {
    @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
//    log.info("********OperationGunDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val l_pile_id = data.getLongValue("l_pile_id")
      var l_station_id = 0L
      var l_seller_id = 0L
      //查询数据
      try {
        connect = DruidUtil.getConnection.get
        //根据桩号查询桩信息
        val query_pile_sql = "select l_station_id from t_business_base_operation_pile where id=? and b_delete_flag=0"
        prepareStatement = connect.prepareStatement(query_pile_sql)
        prepareStatement.setLong(1, l_pile_id)
        resultSet = prepareStatement.executeQuery()
        if(resultSet.next()){
          l_station_id = resultSet.getLong("l_station_id")
        }
        prepareStatement.close()
        resultSet.close()
        //根据站点号查询站点信息
        if(l_station_id!=0){
        val query_station_sql = "select id,l_seller_id from t_business_base_operation_station where id=? and b_delete_flag=0"
        prepareStatement = connect.prepareStatement(query_station_sql)
        prepareStatement.setLong(1, l_station_id)
        resultSet = prepareStatement.executeQuery()
        if(!resultSet.next()){
          l_station_id = 0
        }else{
          l_seller_id = resultSet.getLong("l_seller_id")
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
      if(l_station_id!=0&&l_seller_id!=0){
      //更新t_all_total
     try{ 
        connect = DruidUtil.getConnection.get
       val all_total_sql = new StringBuffer("update t_all_table set ")
                           .append(" total_charging_gun_count=total_charging_gun_count-1")
                           .append(" where l_seller_id=?")
             prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(1, l_seller_id.toInt)
             prepareStatement.execute()
             prepareStatement.close()
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
      //更新station_show_total_table
      try{
         connect = DruidUtil.getConnection.get
       val all_total_sql = new StringBuffer("update station_show_total_table set ")
                           .append(" total_station_gun_num=total_station_gun_num-1")
                           .append(" where l_seller_id=? and l_station_id=?")
             prepareStatement = connect.prepareStatement(all_total_sql.toString())
             prepareStatement.setInt(1, l_seller_id.toInt)
             prepareStatement.setLong(2, l_station_id)
             prepareStatement.execute()
             prepareStatement.close()
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
}