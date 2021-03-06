package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
import screen_analyze.util.{ConfigDruidUtil, DruidUtil}
//noinspection ScalaUnreachableCode
class OperationPileTypeUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var connect1: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //    log.info("********OperationPileTypeUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    val mapCount = scala.collection.mutable.Map[String, Long]()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      old = oldArr.getJSONObject(m)
      //新数据
      val id = data.getLongValue("id")
      val i_power = (data.getDoubleValue("power") * 100).toLong
      //旧数据
      val old_i_power = old.getString("power")
      if (old_i_power != null) {
        val old_power = (old_i_power.toDouble * 100).toLong
        val diff_power = i_power - old_power
        //t_all_total更新
        try {
          connect = ConfigDruidUtil.getConnection.get
          connect1 = DruidUtil.getConnection.get
        //查询桩类型的桩数量
          val query_pile = "select seller_id,count(id) pile_c from t_pile where type_id=? and deleted = 0 group by seller_id"
          prepareStatement = connect.prepareStatement(query_pile)
          prepareStatement.setLong(1, id)
          resultSet = prepareStatement.executeQuery()
          while (resultSet.next()) {
            mapCount += (resultSet.getString("seller_id") -> resultSet.getLong("pile_c"))
          }
          resultSet.close()
          prepareStatement.close()
          if(mapCount.size>0){
            mapCount.foreach{case(key,value)=>
              val all_total_sql = new StringBuffer("update t_all_table set")
            .append(" total_equipment_power=total_equipment_power+?")
            .append(" where l_seller_id=?")
            prepareStatement = connect1.prepareStatement(all_total_sql.toString())
            prepareStatement.setLong(1, value*diff_power)
            prepareStatement.setInt(2, key.toInt)
            prepareStatement.execute()
            prepareStatement.close()
            }
          }
          connect.close()
          connect1.close()
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
            if (connect1 != null && !connect1.isClosed()) {
              connect1.close()
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
          try {
            if (resultSet != null && !resultSet.isClosed()) {
              resultSet.close()
            }
          } catch {
            case ex: Exception => log.info("***t_all_table**关闭resultSet失败****")
          }
        }
         //station_show_total_table更新
        try {
          connect = DruidUtil.getConnection.get
        //查询桩类型的桩数量
          val query_pile = "select l_seller_id,l_station_id,count(id) pile_c from t_business_base_operation_pile where l_type_id=? and b_delete_flag = 0 group by l_seller_id,l_station_id"
          prepareStatement = connect.prepareStatement(query_pile)
          prepareStatement.setLong(1, id)
          resultSet = prepareStatement.executeQuery()
          while (resultSet.next()) {
            mapCount += (resultSet.getString("l_seller_id")+"|"+resultSet.getString("l_station_id") -> resultSet.getLong("pile_c"))
          }
          resultSet.close()
          prepareStatement.close()
          if(mapCount.size>0){
            log.info("mapCountSize大于零*******")
            mapCount.foreach{case(key,value)=>
              val l_seller_id = key.split("\\|")(0)
              val l_station_id = key.split("\\|")(1)
              log.info("l_seller_id："+l_seller_id+"l_station_id:"+l_station_id+key)
              val all_total_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_i_power_count=total_station_i_power_count+?")
            .append(" where l_seller_id=? and l_station_id =?")
            prepareStatement = connect.prepareStatement(all_total_sql.toString())
            prepareStatement.setLong(1, value*diff_power)
            prepareStatement.setInt(2, l_seller_id.toInt)
            prepareStatement.setInt(3, l_station_id.toInt)
            prepareStatement.execute()
            prepareStatement.close()
            }
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
           try {
            if (resultSet != null && !resultSet.isClosed()) {
              resultSet.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭resultSet失败****")
          }
        }
        
      }
    }
  }
}