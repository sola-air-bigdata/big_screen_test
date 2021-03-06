package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{ConfigDruidUtil, DruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger
//noinspection ScalaUnreachableCode
class OperationPileAlarmHisUpdate {
   @transient private lazy val log = Logger.getLogger(this.getClass)
  var prepareStatement: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  def analyzeUpdateTable(jsonSql: JSONObject) {
    //    log.info("********OperationPileAlarmHisUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var arrSize = dataArr.size()
    val mapCount = scala.collection.mutable.Map[String, Long]()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      old = oldArr.getJSONObject(m)
      val l_alarm_define_id = data.getString("alarm_define_id")
      val old_l_alarm_define_id = old.getString("alarm_define_id")
      var alarm_define_id = l_alarm_define_id
      if(old_l_alarm_define_id!=null){ alarm_define_id=old_l_alarm_define_id}
      val old_d_add_time = old.getString("create_time")
      if(old_d_add_time!=null||old_l_alarm_define_id!=null){
      //新数据
      val b_delete_flag = data.getString("deleted")

      val d_add_time = data.getString("create_time")
      val l_seller_id = data.getLongValue("seller_id")
      val l_pile_id = data.getLongValue("pile_id")
      var current_time = ""
      var next_time = ""
      var day_month = ""
      var is_new_pile = 0
      var is_new_pile_row = -1
      if(!StringUtil.isBlank(d_add_time)){
       current_time = TimeConUtil.getMonth(d_add_time, "1")
       next_time = TimeConUtil.getMonth(d_add_time, "0")
       day_month = TimeConUtil.timeConversion(d_add_time, "%Y-%m")
      }
      //旧数据
      val old_b_delete_flag = old.getString("deleted")
      var old_current_time = ""
      var old_next_time = ""
      var old_day_month = ""
      var old_is_new_pile = 0
      var old_is_new_pile_row = -1
      if(old_d_add_time!=null){
       old_current_time = TimeConUtil.getMonth(old_d_add_time, "1")
       old_next_time = TimeConUtil.getMonth(old_d_add_time, "0")
       old_day_month = TimeConUtil.timeConversion(old_d_add_time, "%Y-%m")
      }else{
        old_day_month = day_month
        old_is_new_pile = is_new_pile
      }
      //查询数据
        try {

          connect = ConfigDruidUtil.getConnection.get
          //根据新数据查询
          val query_pile = new StringBuffer("select tah.pile_id from t_alarm tah LEFT JOIN t_pile tp ON tah.pile_id = tp.id" +
            " where tah.deleted = 0 AND tah.alarm_define_id <> 51 AND tp.deleted = 0")
            .append(" and timediff(tah.create_time,?)>0 and timediff(tah.create_time,?)<0 and tah.seller_id=? and tah.pile_id=?")
          prepareStatement = connect.prepareStatement(query_pile.toString())
          prepareStatement.setString(1, current_time)
          prepareStatement.setString(2, next_time)
          prepareStatement.setLong(3, l_seller_id)
          prepareStatement.setLong(4, l_pile_id)
          resultSet = prepareStatement.executeQuery()
          resultSet.last()
          is_new_pile_row = resultSet.getRow
          if (!resultSet.next()) {
            is_new_pile = 1
          }
          prepareStatement.close()
          resultSet.close()
          
           //根据旧数据查询
          val query_pile1 = new StringBuffer("select tah.pile_id from t_alarm tah LEFT JOIN t_pile tp ON tah.pile_id = tp.id" +
            " where tah.deleted = 0 AND tah.alarm_define_id <> 51 AND tp.deleted = 0")
            .append(" and timediff(tah.create_time,?)>0 and timediff(tah.create_time,?)<0 and tah.seller_id=? and tah.pile_id=?")
          prepareStatement = connect.prepareStatement(query_pile1.toString())
          prepareStatement.setString(1, old_current_time)
          prepareStatement.setString(2, old_next_time)
          prepareStatement.setLong(3, l_seller_id)
          prepareStatement.setLong(4, l_pile_id)
          resultSet = prepareStatement.executeQuery()
          resultSet.last()
          old_is_new_pile_row = resultSet.getRow
          if (!resultSet.next()) {
            old_is_new_pile = 1
          }
          prepareStatement.close()
          resultSet.close()
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
      
        //更新all_month_total
        try {
          connect = DruidUtil.getConnection.get
          //修改b_delete_flag从1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)&& !"51".equals(l_alarm_define_id)) {
          log.info("修改b_delete_flag从1到0  is_new_pile:"+is_new_pile)
          val month_total_sql = new StringBuffer("insert into t_all_month_table(l_seller_id,d_month_time,month_fault_pile)")
            .append(" values(?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_fault_pile=month_fault_pile+?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(1, l_seller_id.toInt)
          prepareStatement.setString(2, day_month)
          if(is_new_pile_row==1){
          prepareStatement.setLong(3, 1)
          prepareStatement.setLong(4, 1)
          }else{
          prepareStatement.setLong(3, 0)
          prepareStatement.setLong(4, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
          //修改b_delete_flag从0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag) && !"51".equals(alarm_define_id)) {
          log.info("修改b_delete_flag从0到1  old_is_new_pile:"+old_is_new_pile+"  old_day_month:"+old_day_month)
           val month_total_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_fault_pile=month_fault_pile-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setString(3, old_day_month)
          prepareStatement.setLong(1, old_is_new_pile)
          prepareStatement.execute()
          prepareStatement.close()
        }
          //修改d_add_time
        else if(old_d_add_time!=null && !old_day_month.equals(day_month)&& "0".equals(b_delete_flag)){
          //减少旧时间数据
          if(!"51".equals(alarm_define_id)){
            val month_total_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_fault_pile=month_fault_pile-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setString(3, old_day_month)
          prepareStatement.setLong(1, old_is_new_pile)
          prepareStatement.execute()
          prepareStatement.close()
          }
          //增加新时间数据
          if(!"51".equals(l_alarm_define_id)){
             val month_total_sql = new StringBuffer("insert into t_all_month_table(l_seller_id,d_month_time,month_fault_pile)")
            .append(" values(?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_fault_pile=month_fault_pile+?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(1, l_seller_id.toInt)
          prepareStatement.setString(2, day_month)
          if(is_new_pile_row==1){
          prepareStatement.setLong(3, 1)
          prepareStatement.setLong(4, 1)
          }else{
          prepareStatement.setLong(3, 0)
          prepareStatement.setLong(4, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          }
          
        }
        //修改l_alarm_define_id
        else if(old_l_alarm_define_id!=null){
          if("51".equals(l_alarm_define_id)&& !"51".equals(old_l_alarm_define_id)&& "0".equals(b_delete_flag)){
            val month_total_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_fault_pile=month_fault_pile-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(2, l_seller_id.toInt)
          prepareStatement.setString(3, day_month)
          prepareStatement.setLong(1, is_new_pile)
          prepareStatement.execute()
          prepareStatement.close()
          }else if( !"51".equals(l_alarm_define_id)&& "51".equals(old_l_alarm_define_id)&& "0".equals(b_delete_flag)){
             val month_total_sql = new StringBuffer("insert into t_all_month_table(l_seller_id,d_month_time,month_fault_pile)")
            .append(" values(?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_fault_pile=month_fault_pile+?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setInt(1, l_seller_id.toInt)
          prepareStatement.setString(2, day_month)
          if(is_new_pile_row==1){
          prepareStatement.setLong(3, 1)
          prepareStatement.setLong(4, 1)
          }else{
          prepareStatement.setLong(3, 0)
          prepareStatement.setLong(4, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          }
        }
          connect.close()
         } catch {
          case ex: Exception => log.info("*****t_all_month_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***t_all_month_table**关闭connect失败****")
          }
          try {
            if (prepareStatement != null && !prepareStatement.isClosed()) {
              prepareStatement.close()
            }
          } catch {
            case ex: Exception => log.info("***t_all_month_table**关闭prepareStatement失败****")
          }
        }
      }
      
    }
   }
}