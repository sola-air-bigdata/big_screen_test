package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
import screen_analyze.util.{ConfigDruidUtil, DruidUtil}
//noinspection ScalaUnreachableCode
class OperationGunUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
//    log.info("********OperationGunUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      old = oldArr.getJSONObject(m)
      var flag = true
      //新数据
      val l_pile_id = data.getLongValue("pile_id")
      var l_station_id = 0L
      var l_seller_id = 0L
      //旧数据
      val old_l_pile_id = old.getString("pile_id")
      var old_l_station_id = 0L
      var old_l_seller_id = 0L
      if (old_l_pile_id != null && old_l_pile_id.toLong != l_pile_id) {
        try {
          connect = ConfigDruidUtil.getConnection.get
          //查询旧桩号商家、站点
          val query_pile_sql = "select station_id,seller_id from t_pile where id=? and deleted=0"
          prepareStatement = connect.prepareStatement(query_pile_sql)
          prepareStatement.setLong(1, old_l_pile_id.toLong)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            old_l_station_id = resultSet.getLong("station_id")
            old_l_seller_id = resultSet.getLong("seller_id")
          }
          prepareStatement.close()
          resultSet.close()
          //查询新桩号商家、站点
          val query_pile_sql1 = "select station_id,seller_id from t_pile where id=? and deleted=0"
          prepareStatement = connect.prepareStatement(query_pile_sql1)
          prepareStatement.setLong(1, l_pile_id)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            l_station_id = resultSet.getLong("station_id")
            l_seller_id = resultSet.getLong("seller_id")
          }
          prepareStatement.close()
          resultSet.close()
          if (l_seller_id != old_l_seller_id) {
            //根据新站点号查询站点信息
            val query_station_sql = "select id,seller_id from t_station where id=? and deleted=0"
            prepareStatement = connect.prepareStatement(query_station_sql)
            prepareStatement.setLong(1, l_station_id)
            resultSet = prepareStatement.executeQuery()
            if (!resultSet.next()) {
              l_seller_id = 0
              l_station_id = 0
            }
            prepareStatement.close()
            resultSet.close()
            //根据旧站点号查询站点信息
            val query_station_sql1 = "select id,seller_id from t_station where id=? and deleted=0"
            prepareStatement = connect.prepareStatement(query_station_sql1)
            prepareStatement.setLong(1, old_l_station_id)
            resultSet = prepareStatement.executeQuery()
            if (!resultSet.next()) {
              old_l_seller_id = 0
              old_l_station_id = 0
            }
            prepareStatement.close()
            resultSet.close()
          } else {
            flag = false
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
        if (flag) {
          //更新t_all_total
          try {
            connect = DruidUtil.getConnection.get
            //减少旧枪数据
            if (old_l_seller_id != 0) {
              val all_total_sql = new StringBuffer("update t_all_table set ")
                .append(" total_charging_gun_count=total_charging_gun_count-1")
                .append(" where l_seller_id=?")
              prepareStatement = connect.prepareStatement(all_total_sql.toString())
              prepareStatement.setInt(1, old_l_seller_id.toInt)
              prepareStatement.execute()
              prepareStatement.close()
            }
            //增加新枪数据
            if (l_seller_id != 0) {
              val all_total_sql = new StringBuffer("insert into t_all_table(l_seller_id,total_charging_gun_count) ")
                .append(" values(?,1)")
                .append(" ON DUPLICATE KEY UPDATE")
                .append(" total_charging_gun_count=total_charging_gun_count+1")
              prepareStatement = connect.prepareStatement(all_total_sql.toString())
              prepareStatement.setInt(1, l_seller_id.toInt)
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
          //更新station_show_total_table
          try {
            connect = DruidUtil.getConnection.get
            //减少旧枪数据
            if (old_l_seller_id != 0 && old_l_station_id != 0) {
              val all_total_sql = new StringBuffer("update station_show_total_table set ")
                .append(" total_station_gun_num=total_station_gun_num-1")
                .append(" where l_seller_id=? and l_station_id=?")
              prepareStatement = connect.prepareStatement(all_total_sql.toString())
              prepareStatement.setInt(1, old_l_seller_id.toInt)
              prepareStatement.setLong(2, old_l_station_id)
              prepareStatement.execute()
              prepareStatement.close()
            }
            //增加新枪数据
            if (l_station_id != 0 && l_seller_id != 0) {
              val all_total_sql = new StringBuffer("insert into station_show_total_table(l_seller_id,l_station_id,total_station_gun_num) ")
                .append(" values(?,?,1)")
                .append(" ON DUPLICATE KEY UPDATE")
                .append(" total_station_gun_num=total_station_gun_num+1")
              prepareStatement = connect.prepareStatement(all_total_sql.toString())
              prepareStatement.setInt(1, l_seller_id.toInt)
              prepareStatement.setLong(2, l_station_id)
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
  }
}