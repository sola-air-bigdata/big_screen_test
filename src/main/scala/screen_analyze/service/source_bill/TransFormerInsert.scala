package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class TransFormerInsert {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeInsertTable(jsonSql: JSONObject) {
    //    log.info("********TransFormerInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      val b_delete_flag = data.getString("b_delete_flag")
      val t_power_rate = data.getLongValue("t_power_rate")
      val station_id = data.getLongValue("station_id")
      val l_seller_id = data.getLongValue("l_seller_id")
      if ("0".equals(b_delete_flag)) {

        //station_show_total_table
        try {
          connect = DruidUtil.getConnection.get
          val total_table_sql = new StringBuffer("insert into station_show_total_table(l_station_id,l_seller_id,")
            .append(" total_station_change_power_count)")
            .append(" values(?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_station_change_power_count=total_station_change_power_count+?")
          prepareStatement = connect.prepareStatement(total_table_sql.toString())
          prepareStatement.setLong(1, station_id)
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setLong(3, t_power_rate)
          prepareStatement.setLong(4, t_power_rate)
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