package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
class TransFormerDelete {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeDeleteTable(jsonSql: JSONObject) {
    //    log.info("********TransFormerDelete处理json:" + jsonSql.toJSONString())
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
          val total_table_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_change_power_count=total_station_change_power_count-?")
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(total_table_sql.toString())
          prepareStatement.setLong(2, station_id)
          prepareStatement.setInt(3, l_seller_id.toInt)
          prepareStatement.setLong(1, t_power_rate)
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