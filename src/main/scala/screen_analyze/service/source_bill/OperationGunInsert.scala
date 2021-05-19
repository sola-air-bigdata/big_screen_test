package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
import screen_analyze.util.{ConfigDruidUtil, DruidUtil}
//noinspection ScalaUnreachableCode
class OperationGunInsert {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
//    log.info("********OperationGunInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val l_pile_id = data.getLongValue("pile_id")
      var l_station_id = 0L
      var l_seller_id = 0L
      //查询数据
      try {
        connect = ConfigDruidUtil.getConnection.get
        //根据桩号查询桩信息
        val query_pile_sql = "select station_id from t_pile where id=? and deleted=0"
        prepareStatement = connect.prepareStatement(query_pile_sql)
        prepareStatement.setLong(1, l_pile_id)
        resultSet = prepareStatement.executeQuery()
        if (resultSet.next()) {
          l_station_id = resultSet.getLong("station_id")
        }
        prepareStatement.close()
        resultSet.close()
        //根据站点号查询站点信息
        if (l_station_id != 0) {
          val query_station_sql = "select id,seller_id from t_station where id=? and deleted=0"
          prepareStatement = connect.prepareStatement(query_station_sql)
          prepareStatement.setLong(1, l_station_id)
          resultSet = prepareStatement.executeQuery()
          if (!resultSet.next()) {
            l_station_id = 0
          } else {
            l_seller_id = resultSet.getLong("seller_id")
          }
          prepareStatement.close()
          resultSet.close()
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
     log.info("l_station_id:"+l_station_id+"l_seller_id:"+l_seller_id)
      if (l_station_id != 0 && l_seller_id != 0) {
        //更新t_all_total
        try {
          connect = DruidUtil.getConnection.get
          val all_total_sql = new StringBuffer("insert into t_all_table(l_seller_id,total_charging_gun_count) ")
            .append(" values(?,1)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_charging_gun_count=total_charging_gun_count+1")
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
        try {
          connect = DruidUtil.getConnection.get
          val all_total_sql = new StringBuffer("insert into station_show_total_table(l_seller_id,l_station_id,total_station_gun_num) ")
            .append(" values(?,?,1)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_station_gun_num=total_station_gun_num+1")
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