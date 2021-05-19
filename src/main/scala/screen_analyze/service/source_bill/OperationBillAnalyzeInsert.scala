package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{BillDruidUtil, ConfigDruidUtil, DruidUtil, MemberDruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
/**
 * 监听t_bill表insert操作
 */
//noinspection ScalaUnreachableCode
class OperationBillAnalyzeInsert {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var l_station_id = 0L
  var l_pile_id_arr: scala.collection.mutable.ArrayBuffer[Long] = ArrayBuffer()
  var level = ""
  var carType = ""
  var carNo = ""
  var prepareStatement: PreparedStatement = _
  var prepareStatement01: PreparedStatement = _
  var prepareStatement02: PreparedStatement = _
  var prepareStatement03: PreparedStatement = _
  var prepareStatement04: PreparedStatement = _
  var connect: Connection = _
  var connect1: Connection = _
  var connect2: Connection = _
  var resultSet: ResultSet = _
  var resultSet01: ResultSet = _
  var resultSet02: ResultSet = _
  var resultSet03: ResultSet = _
  //分析增数据
  def analyzeInsertTable(jsonSql: JSONObject) {
//    log.info("********OperationBillAnalyzeInsert处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      //查询会员等级
      try {
        connect = MemberDruidUtil.getConnection.get
        val i_member_id = data.getString("member_id")
        val v_vin = data.getString("vin")
        if (!StringUtil.isBlank(i_member_id)) {
          val query_level_sql = "select level from t_member where id=?"
          prepareStatement02 = connect.prepareStatement(query_level_sql)
          prepareStatement02.setLong(1, i_member_id.toLong)
          resultSet01 = prepareStatement02.executeQuery()
          if (resultSet01.next()) {
            level = resultSet01.getString("level")
          }
          prepareStatement02.close()
          resultSet01.close()
        }

         if (!StringUtil.isBlank(v_vin)) {
          val queryCarSql = "select carrier_type from t_vehicle where vin=? and deleted=0"
          prepareStatement02 = connect.prepareStatement(queryCarSql)
          prepareStatement02.setString(1, v_vin)
          resultSet01 = prepareStatement02.executeQuery()
          if (resultSet01.next()) {
            carType = resultSet01.getString("carrier_type")
          }
          prepareStatement02.close()
          resultSet01.close()
          
          val queryCarNoSql = "select plate_no from t_vehicle where vin=? and deleted=0"
          prepareStatement02 = connect.prepareStatement(queryCarNoSql)
          prepareStatement02.setString(1, v_vin)
          resultSet01 = prepareStatement02.executeQuery()
          if (resultSet01.next()) {
            carNo = resultSet01.getString("plate_no")
          }
          prepareStatement02.close()
          resultSet01.close()
        }
          connect.close()
      } catch {
        case ex: Exception => log.info("*****查询数据异常**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***查询数据异常**关闭connect失败****")
        }
        try {
          if (resultSet01 != null && !resultSet01.isClosed()) {
            resultSet01.close()
          }
        } catch {
          case ex: Exception => log.info("***查询数据异常**关闭resultSet01失败****")
        }
        try {
          if (prepareStatement02 != null && !prepareStatement02.isClosed()) {
            prepareStatement02.close()
          }
        } catch {
          case ex: Exception => log.info("***查询数据异常**关闭prepareStatement02失败****")
        }
      }

      //更新t_all_day_member_table表
      try {
        connect = DruidUtil.getConnection.get
        val s_member_flag = data.getString("member_type")
        val b_finish = data.getString("finish")
        val i_member_id = data.getString("member_id")
        val day = data.getString("time_s")
        if (("1".equals(s_member_flag)|| !"GongJiao".equals(carType)||StringUtil.isBlank(carType))&&
            "1".equals(b_finish) && !StringUtil.isBlank(i_member_id) && !StringUtil.isBlank(day)) {
          val m_consume_balance = data.getLong("consume_balance")
          val i_actual_balance = data.getLong("actual_balance")
          val l_seller_id = data.getIntValue("seller_id")
          val dayd = TimeConUtil.timeConversion(day, "%Y-%m-%d")
          val insertSql = new StringBuffer("INSERT INTO t_all_day_member_table")
          val all_day_member_sql = insertSql
           .append("(l_seller_id,d_day_time,l_member_id,day_personal_charge_count,day_personal_m_consume_balance_count,day_personal_i_power_balance_count,day_nbus_i_actual_balance_count_rank)")
            .append(" values(?,?,?,1,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" day_personal_charge_count=day_personal_charge_count+1,")
            .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count+?,")
            .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count+?,")
            .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank+?")
          prepareStatement = connect.prepareStatement(all_day_member_sql.toString())
          prepareStatement.setInt(1, l_seller_id)
          prepareStatement.setString(2, dayd)
          prepareStatement.setLong(3, i_member_id.toLong)
          prepareStatement.setLong(4, m_consume_balance)
          prepareStatement.setLong(5, i_actual_balance)
          prepareStatement.setLong(6, i_actual_balance)
          prepareStatement.setLong(7, m_consume_balance)
          prepareStatement.setLong(8, i_actual_balance)
          prepareStatement.setLong(9, i_actual_balance)
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()
        }
      } catch {
        case ex: Exception => log.info("*****t_all_day_member_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_member_table**关闭connect失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_member_table**关闭prepareStatement失败****")
        }
      }
      //更新t_all_day_table表
      try {
        connect = MemberDruidUtil.getConnection.get
        val s_member_flag = data.getString("member_type")
        val b_finish = data.getString("finish")
        val l_seller_id = data.getIntValue("seller_id")
        val i_member_id = data.getString("member_id")
        val b_delete_flag = data.getString("deleted")
        val day = data.getString("time_s")
        val i_bus_type = data.getString("bus_type")
        val i_actual_balance = data.getLongValue("actual_balance")
        val i_power_balance = data.getLongValue("power_balance")
        val b_feesingle = data.getString("fee_single")
        val v_vin = data.getString("vin")
        var carType = ""
        var isnewC = 0
        if ("1".equals(b_finish) && "0".equals(b_delete_flag) && !StringUtil.isBlank(day)) {
          val dayd = TimeConUtil.timeConversion(day, "%Y-%m-%d")
          //查询车类型
          if (v_vin != null) {
            val queryCarSql = "select carrier_type from t_vehicle where vin=? and deleted=0"
            prepareStatement01 = connect.prepareStatement(queryCarSql.toString())
            prepareStatement01.setString(1, v_vin)
            resultSet = prepareStatement01.executeQuery()
            if (resultSet.next()) {
              carType = resultSet.getString("carrier_type")
            }
            prepareStatement01.close()
            resultSet.close()
          }

          connect1 = BillDruidUtil.getConnection.get
          val curT = TimeConUtil.getTime(day, "1")
          val nextT = TimeConUtil.getTime(day, "0")
          val querynewC = "select id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect1.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, curT)
          prepareStatement.setString(3, nextT)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          resultSet.last()
          if (resultSet.getRow == 1) {
            isnewC = 1
          }
          prepareStatement.close()
          resultSet.close()



          val t_all_day_sql = new StringBuffer("insert into t_all_day_table values(")
            .append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" day_team_active_count=day_team_active_count+?,")
            .append(" day_personal_active_count=day_personal_active_count+?,")
            .append(" day_team_service_user=day_team_service_user+?,") //26
            .append(" day_personal_service_user=day_personal_service_user+?,")
            .append(" day_service_app_user=day_service_app_user+?,")
            .append(" day_service_wechat_user=day_service_wechat_user+?,")
            .append(" day_i_actual_balance=day_i_actual_balance+?,") //30
            .append(" day_i_power_balance=day_i_power_balance+?,")
            .append(" day_i_actual_balance_balance_nteam=day_i_actual_balance_balance_nteam+?,")
            .append(" day_i_actual_balance_balance_WuLiu=day_i_actual_balance_balance_WuLiu+?,")
            .append(" day_i_actual_balance_balance_ChuZu=day_i_actual_balance_balance_ChuZu+?,")
            .append(" day_i_actual_balance_balance_KeYun=day_i_actual_balance_balance_KeYun+?,") //35
            .append(" day_i_actual_balance_balance_WangYue=day_i_actual_balance_balance_WangYue+?,")
            .append(" day_i_actual_balance_balance_QiTa=day_i_actual_balance_balance_QiTa+?,")
            .append(" day_i_power_balance_nteam=day_i_power_balance_nteam+?,")
            .append(" day_i_actual_balance_vin=day_i_actual_balance_vin+?,")
            .append(" day_i_actual_balance_card=day_i_actual_balance_card+?,")
            .append(" day_i_actual_balance_app=day_i_actual_balance_app+?,")
            .append(" day_i_actual_balance_wechat=day_i_actual_balance_wechat+?")
          connect2 = DruidUtil.getConnection.get
          prepareStatement = connect2.prepareStatement(t_all_day_sql.toString())
          prepareStatement.setInt(1, l_seller_id)
          prepareStatement.setString(2, dayd)
          prepareStatement.setLong(5, 0)
          prepareStatement.setLong(6, 0)
          //个人或车队更新
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(4, 1)
            prepareStatement.setLong(8, isnewC)
            prepareStatement.setLong(25, 1)
            prepareStatement.setLong(27, isnewC)

            prepareStatement.setLong(3, 0)
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(26, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(3, 1)
            prepareStatement.setLong(7, isnewC)
            prepareStatement.setLong(24, 1)
            prepareStatement.setLong(26, isnewC)

            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(27, 0)

          } else {
            prepareStatement.setLong(3, 0)
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(26, 0)

            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(27, 0)
          }
          //服务类型更新
          if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(9, isnewC)
            prepareStatement.setLong(28, isnewC)

            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(29, 0)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(10, isnewC)
            prepareStatement.setLong(29, isnewC)

            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(28, 0)
          } else {
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(29, 0)

            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(28, 0)
          }
          //总收入统计
          if ("0".equals(b_feesingle)) {
            prepareStatement.setLong(11, i_actual_balance)
            prepareStatement.setLong(12, i_power_balance)
            prepareStatement.setLong(30, i_actual_balance)
            prepareStatement.setLong(31, i_power_balance)
          } else {
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(30, 0)
            prepareStatement.setLong(31, 0)
          }
          //车类型统计
          if (!"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(13, i_actual_balance)
            prepareStatement.setLong(32, i_actual_balance)
            prepareStatement.setLong(19, i_power_balance)
            prepareStatement.setLong(38, i_power_balance)
          } else {
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(32, 0)
            prepareStatement.setLong(19, 0)
            prepareStatement.setLong(38, 0)
          }
          if ("WuLiu".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(14, i_power_balance)
            prepareStatement.setLong(33, i_power_balance)

            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(37, 0)
          } else if ("ChuZu".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(15, i_power_balance)
            prepareStatement.setLong(34, i_power_balance)

            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(37, 0)
          } else if ("KeYun".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(16, i_power_balance)
            prepareStatement.setLong(35, i_power_balance)

            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(37, 0)
          } else if ("WangYue".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(17, i_power_balance)
            prepareStatement.setLong(36, i_power_balance)

            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(37, 0)
          } else if ("QiTa".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(18, i_power_balance)
            prepareStatement.setLong(37, i_power_balance)

            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(33, 0)
          } else {
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(37, 0)

            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(33, 0)
          }
          //业务类型统计
          if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(20, i_actual_balance)
            prepareStatement.setLong(39, i_actual_balance)

            prepareStatement.setLong(21, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(41, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(42, 0)
          } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(21, i_actual_balance)
            prepareStatement.setLong(40, i_actual_balance)

            prepareStatement.setLong(20, 0)
            prepareStatement.setLong(39, 0)
            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(41, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(42, 0)
          } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(22, i_actual_balance)
            prepareStatement.setLong(41, i_actual_balance)

            prepareStatement.setLong(21, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(20, 0)
            prepareStatement.setLong(39, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(42, 0)
          } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(23, i_actual_balance)
            prepareStatement.setLong(42, i_actual_balance)

            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(41, 0)
            prepareStatement.setLong(21, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(20, 0)
            prepareStatement.setLong(39, 0)
          } else {
            prepareStatement.setLong(20, 0)
            prepareStatement.setLong(39, 0)

            prepareStatement.setLong(21, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(41, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(42, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()
          connect1.close()
          connect2.close()
        }
      } catch {
        case ex: Exception => log.info("*****t_all_day_table**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭connect失败****")
        }
        try {
          if (connect1 != null && !connect1.isClosed()) {
            connect1.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭connect失败****")
        }
        try {
          if (connect2 != null && !connect2.isClosed()) {
            connect2.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭connect失败****")
        }
        try {
          if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭resultSet失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭prepareStatement失败****")
        }
        try {
          if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
            prepareStatement01.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭prepareStatement01失败****")
        }
      }

      //更新t_all_month_total表
      try {
        connect = DruidUtil.getConnection.get
        val s_member_flag = data.getString("member_type")
        val b_finish = data.getString("finish")
        val i_member_id = data.getString("member_id")
        val day = data.getString("time_s")
        val l_seller_id = data.getIntValue("seller_id")
        val b_delete_flag = data.getString("deleted")
        if ("1".equals(b_finish) && !StringUtil.isBlank(day) && "0".equals(b_delete_flag)) {
//          val m_appoint_balance = data.getLongValue("m_appoint_balance")
          val i_power_balance = data.getLongValue("power_balance")
//          val m_parking_balance = data.getLongValue("m_parking_balance")
          val b_feesingle = data.getLongValue("fee_single")
          val b_fee_service = data.getLongValue("fee_service")
          val m_sum_power = data.getLongValue("sum_power")
          val i_aiscount_balance = data.getLongValue("discount_balance")
          val m_service_balance = data.getLongValue("service_balance")
           val i_deductible_balance = data.getLongValue("deductible_balance")
         var power_balance = i_power_balance
          var isfee = 0L
          if (1 == b_feesingle || 1 == b_fee_service) {
           isfee = m_service_balance - i_aiscount_balance
          }
          if(b_feesingle == 1){
            power_balance = 0
          }
//          val serviceban = (m_service_balance-isfee-i_aiscount_balance-i_deductible_balance)+m_appoint_balance+m_parking_balance
          val serviceban = m_service_balance-isfee-i_aiscount_balance-i_deductible_balance
          val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
          val t_all_month_sql = new StringBuffer("insert into t_all_month_table values(?,?,?,?,?,?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_team_charging_power=month_team_charging_power+?,")
            .append(" month_personal_charging_power=month_personal_charging_power+?,")
            .append(" month_i_charging_actual_balance=month_i_charging_actual_balance+?,")
            .append(" month_i_service_actual_balance=month_i_service_actual_balance+?,")
            .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance+?,")
            .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance+?")
          prepareStatement = connect.prepareStatement(t_all_month_sql.toString())
          prepareStatement.setInt(1, l_seller_id)
          prepareStatement.setString(2, dayd)
          prepareStatement.setLong(5, 0)
          prepareStatement.setLong(6, 0)
          prepareStatement.setLong(7, 0)
          prepareStatement.setLong(8, power_balance)
          prepareStatement.setLong(9,serviceban)
          prepareStatement.setLong(14, power_balance)
          prepareStatement.setLong(15, serviceban)
          //个人车队统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(4, m_sum_power)
            prepareStatement.setLong(13, m_sum_power)
            prepareStatement.setLong(10, power_balance)
            prepareStatement.setLong(16, power_balance)
            prepareStatement.setLong(11,serviceban)
            prepareStatement.setLong(17, serviceban)

            prepareStatement.setLong(3, 0)
            prepareStatement.setLong(12, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(3, m_sum_power)
            prepareStatement.setLong(12, m_sum_power)

            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(17, 0)
          } else {
            prepareStatement.setLong(3, 0)
            prepareStatement.setLong(12, 0)

            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(17, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()

        }
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
      //更新t_all_total表
      try {
        connect = DruidUtil.getConnection.get
        val b_finish = data.getString("finish")
        val b_delete_flag = data.getString("deleted")
        if ("1".equals(b_finish) && "0".equals(b_delete_flag)) {
          val t_all_sql = new StringBuffer("insert into t_all_table values(")
            .append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,")
            .append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" total_charging_power=total_charging_power+?,")
            .append(" team_charging_power=team_charging_power+?,")
            .append(" total_service_count=total_service_count+?,")
            .append(" team_service_count=team_service_count+?,") //40
            .append(" personal_service_count=personal_service_count+?,")
            .append(" total_service_time=total_service_time+?,")
            .append(" team_service_time=team_service_time+?,")
            .append(" personal_service_time=personal_service_time+?,")
            .append(" total_service_user_count=total_service_user_count+?,") //45
            .append(" total_service_team_user_count=total_service_team_user_count+?,")
            .append(" total_service_personal_user_count=total_service_personal_user_count+?,")
            .append(" total_service_silver_user_count=total_service_silver_user_count+?,")
            .append(" total_service_gold_user_count=total_service_gold_user_count+?,")
            .append(" total_service_platinum_user_count=total_service_platinum_user_count+?,") //50
            .append(" total_service_diamond_user_count=total_service_diamond_user_count+?,")
            .append(" total_service_blackgold_user_count=total_service_blackgold_user_count+?,")
            .append(" total_i_actual_balance=total_i_actual_balance+?,")
            .append(" team_i_actual_balance=team_i_actual_balance+?,")
            .append(" personal_i_actual_balance=personal_i_actual_balance+?,") //55
            .append(" total_i_power_balance=total_i_power_balance+?,")
            .append(" total_i_service_balance=total_i_service_balance+?,")
            .append(" total_vin_actual_balance=total_vin_actual_balance+?,")
            .append(" total_card_actual_balance=total_card_actual_balance+?,")
            .append(" total_wechat_actual_balance=total_wechat_actual_balance+?,") //60
            .append(" total_app_actual_balance=total_app_actual_balance+?")
          prepareStatement = connect.prepareStatement(t_all_sql.toString())
          val l_seller_id = data.getIntValue("seller_id")
          val i_power_balance = data.getLongValue("power_balance")
          val b_feesingle = data.getLongValue("fee_single")
          val b_fee_service = data.getLongValue("fee_service")
          val m_sum_power = data.getLongValue("sum_power")
          val d_time_s = data.getString("time_s")
          val d_time_e = data.getString("time_e")
          val diffT = TimeConUtil.timeDIFF(d_time_s, d_time_e)
          val i_member_id = data.getString("member_id")
          val l_leaguer_id = data.getString("leaguer_id")
          val i_actual_balance = data.getLong("actual_balance")
          val i_bus_type = data.getString("bus_type")
          val s_member_flag = data.getString("member_type")
          val i_aiscount_balance = data.getLongValue("discount_balance")
          var isnewC = 0
          if (!StringUtil.isBlank(i_member_id)) {
            val query_sql = "select id from t_bill where member_id=? and seller_id=? and finish=1 and deleted=0"
            prepareStatement01 = connect.prepareStatement(query_sql)
            prepareStatement01.setLong(1, i_member_id.toLong)
            prepareStatement01.setInt(2, l_seller_id)
            resultSet = prepareStatement01.executeQuery()
            resultSet.last()
            if (resultSet.getRow == 1) {
              isnewC = 1
            }
            prepareStatement01.close()
            resultSet.close()
          } else if (!StringUtil.isBlank(l_leaguer_id)) {
            val query_sql = "select id from t_bill where leaguer_id=? and seller_id=? and finish=1 and deleted=0 "
            prepareStatement01 = connect.prepareStatement(query_sql)
            prepareStatement01.setLong(1, l_leaguer_id.toLong)
            prepareStatement01.setInt(2, l_seller_id)
            resultSet = prepareStatement01.executeQuery()
            resultSet.last()
            if (resultSet.getRow == 1) {
              isnewC = 1
            }
            prepareStatement01.close()
            resultSet.close()
          }

          var m_service_balance = data.getLongValue("service_balance")
          if (1 == b_feesingle || 1 == b_fee_service) {
            m_service_balance = 0
          } else {
            m_service_balance = m_service_balance - i_aiscount_balance
          }
          prepareStatement.setInt(1, l_seller_id)
          prepareStatement.setLong(2, 0)
          prepareStatement.setLong(11, 0)
          prepareStatement.setLong(12, 0)
          prepareStatement.setLong(13, 0)
          prepareStatement.setLong(14, 0)
          prepareStatement.setLong(15, 0)
          prepareStatement.setLong(16, 0)
          prepareStatement.setLong(17, 0)
          prepareStatement.setLong(18, 0)
          prepareStatement.setLong(19, 0)
          prepareStatement.setLong(3, m_sum_power)
          prepareStatement.setLong(37, m_sum_power)
          prepareStatement.setLong(5, 1)
          prepareStatement.setLong(39, 1)
          prepareStatement.setLong(8, diffT)
          prepareStatement.setLong(42, diffT)
          prepareStatement.setLong(20, isnewC)
          prepareStatement.setLong(45, isnewC)
          prepareStatement.setLong(28, i_actual_balance)
          prepareStatement.setLong(53, i_actual_balance)
          prepareStatement.setLong(31, i_power_balance)
          prepareStatement.setLong(56, i_power_balance)
          prepareStatement.setLong(32, m_service_balance)
          prepareStatement.setLong(57, m_service_balance)
          //业务类型统计
          if ("3".equals(i_bus_type)) {
            prepareStatement.setLong(33, i_actual_balance)
            prepareStatement.setLong(58, i_actual_balance)

            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(59, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(60, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(61, 0)
          } else if ("1".equals(i_bus_type)) {
            prepareStatement.setLong(34, i_actual_balance)
            prepareStatement.setLong(59, i_actual_balance)

            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(58, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(60, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(61, 0)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(35, i_actual_balance)
            prepareStatement.setLong(60, i_actual_balance)

            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(58, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(59, 0)
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(61, 0)
          } else if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(36, i_actual_balance)
            prepareStatement.setLong(61, i_actual_balance)

            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(58, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(59, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(60, 0)
          } else {
            prepareStatement.setLong(36, 0)
            prepareStatement.setLong(61, 0)

            prepareStatement.setLong(33, 0)
            prepareStatement.setLong(58, 0)
            prepareStatement.setLong(34, 0)
            prepareStatement.setLong(59, 0)
            prepareStatement.setLong(35, 0)
            prepareStatement.setLong(60, 0)
          }
          //会员等级统计
          if ("1".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(23, 1)
            prepareStatement.setLong(48, 1)

            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(49, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(50, 0)
            prepareStatement.setLong(26, 0)
            prepareStatement.setLong(51, 0)
            prepareStatement.setLong(27, 0)
            prepareStatement.setLong(52, 0)
          } else if ("2".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(24, 1)
            prepareStatement.setLong(49, 1)

            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(48, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(50, 0)
            prepareStatement.setLong(26, 0)
            prepareStatement.setLong(51, 0)
            prepareStatement.setLong(27, 0)
            prepareStatement.setLong(52, 0)
          } else if ("3".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(25, 1)
            prepareStatement.setLong(50, 1)

            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(48, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(49, 0)
            prepareStatement.setLong(26, 0)
            prepareStatement.setLong(51, 0)
            prepareStatement.setLong(27, 0)
            prepareStatement.setLong(52, 0)
          } else if ("4".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(26, 1)
            prepareStatement.setLong(51, 1)

            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(48, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(49, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(50, 0)
            prepareStatement.setLong(27, 0)
            prepareStatement.setLong(52, 0)
          } else if ("5".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(27, 1)
            prepareStatement.setLong(52, 1)

            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(48, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(49, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(50, 0)
            prepareStatement.setLong(26, 0)
            prepareStatement.setLong(51, 0)
          } else {
            prepareStatement.setLong(27, 0)
            prepareStatement.setLong(52, 0)

            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(48, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(49, 0)
            prepareStatement.setLong(25, 0)
            prepareStatement.setLong(50, 0)
            prepareStatement.setLong(26, 0)
            prepareStatement.setLong(51, 0)
          }
          //车队个人类型统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(7, 1)
            prepareStatement.setLong(41, 1)
            prepareStatement.setLong(10, diffT)
            prepareStatement.setLong(44, diffT)
            prepareStatement.setLong(22, isnewC)
            prepareStatement.setLong(47, isnewC)
            prepareStatement.setLong(30, i_actual_balance)
            prepareStatement.setLong(55, i_actual_balance)

            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(38, 0)
            prepareStatement.setLong(6, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(43, 0)
            prepareStatement.setLong(21, 0)
            prepareStatement.setLong(46, 0)
            prepareStatement.setLong(29, 0)
            prepareStatement.setLong(54, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(4, m_sum_power)
            prepareStatement.setLong(38, m_sum_power)
            prepareStatement.setLong(6, 1)
            prepareStatement.setLong(40, 1)
            prepareStatement.setLong(9, diffT)
            prepareStatement.setLong(43, diffT)
            prepareStatement.setLong(21, isnewC)
            prepareStatement.setLong(46, isnewC)
            prepareStatement.setLong(29, i_actual_balance)
            prepareStatement.setLong(54, i_actual_balance)

            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(41, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(44, 0)
            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(47, 0)
            prepareStatement.setLong(30, 0)
            prepareStatement.setLong(55, 0)
          } else {
            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(38, 0)
            prepareStatement.setLong(6, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(43, 0)
            prepareStatement.setLong(21, 0)
            prepareStatement.setLong(46, 0)
            prepareStatement.setLong(29, 0)
            prepareStatement.setLong(54, 0)

            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(41, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(44, 0)
            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(47, 0)
            prepareStatement.setLong(30, 0)
            prepareStatement.setLong(55, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()
        }
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
          if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_table**关闭resultSet失败****")
        }
        try {
          if (resultSet01 != null && !resultSet01.isClosed()) {
            resultSet01.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_table**关闭resultSet01失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_table**关闭prepareStatement失败****")
        }
        try {
          if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
            prepareStatement01.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_table**关闭prepareStatement01失败****")
        }
      }
      //查询站点、桩信息
      try {
        connect = ConfigDruidUtil.getConnection.get
        val s_member_flag = data.getString("member_type")
        val l_pile_id = data.getLongValue("pile_id")
        val query_stid = "select station_id from t_pile where id=?"
        prepareStatement = connect.prepareStatement(query_stid)
        prepareStatement.setLong(1, l_pile_id)
        resultSet = prepareStatement.executeQuery()
        if (resultSet.next()) {
          l_station_id = resultSet.getLong("station_id")
        }
        prepareStatement.close()
        resultSet.close()
        if (l_station_id != 0) {
          val query_l_pile_arr = "select id from t_pile where station_id=?"
          prepareStatement01 = connect.prepareStatement(query_l_pile_arr)
          prepareStatement01.setLong(1, l_station_id)
          resultSet01 = prepareStatement01.executeQuery()
          while (resultSet01.next()) {
            l_pile_id_arr += resultSet01.getLong("id")
          }
          prepareStatement01.close()
          resultSet01.close()
        }
      } catch {
        case ex: Exception => log.info("*****query_station_pile_id**mysql处理异常****")
      } finally {
        try {
          if (connect != null && !connect.isClosed()) {
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("***query_station_pile_id**关闭connect失败****")
        }
        try {
          if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close()
          }
        } catch {
          case ex: Exception => log.info("***query_station_pile_id**关闭resultSet失败****")
        }
        try {
          if (resultSet01 != null && !resultSet01.isClosed()) {
            resultSet01.close()
          }
        } catch {
          case ex: Exception => log.info("***query_station_pile_id**关闭resultSet01失败****")
        }
        try {
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***query_station_pile_id**关闭prepareStatement失败****")
        }
        try {
          if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
            prepareStatement01.close()
          }
        } catch {
          case ex: Exception => log.info("***query_station_pile_id**关闭prepareStatement01失败****")
        }
      }
/****************************************************************************************************/
      //查询站点、桩号成功
      if (l_station_id != 0 && l_pile_id_arr.size > 0) {

        //更新station_show_day_table表
        try {
          connect = DruidUtil.getConnection.get
          connect1 = BillDruidUtil.getConnection.get
          connect2 = MemberDruidUtil.getConnection.get
          val s_member_flag = data.getString("member_type")
          val b_finish = data.getString("finish")
          val i_member_id = data.getString("member_id")
          val day = data.getString("time_s")
          val l_seller_id = data.getIntValue("seller_id")
          val b_delete_flag = data.getString("deleted")
          val l_pile_id = data.getLongValue("pile_id")
          val i_actual_balance = data.getLongValue("actual_balance")
          if ("1".equals(b_finish) && !StringUtil.isBlank(day) && "0".equals(b_delete_flag) && l_pile_id != 0) {
//            val m_appoint_balance = data.getLongValue("m_appoint_balance")
            val i_power_balance = data.getLongValue("power_balance")
//            val m_parking_balance = data.getLongValue("m_parking_balance")
            val b_feesingle = data.getLongValue("fee_single")
            val b_fee_service = data.getLongValue("fee_service")
            val m_sum_power = data.getLongValue("sum_power")
            val v_vin = data.getString("vin")
            val currentTime = TimeConUtil.getTime(day, "1")
            val nextTime = TimeConUtil.getTime(day, "0")
            val i_aiscount_balance = data.getLongValue("discount_balance")
            var carType = ""
            var m_service_balance = data.getLongValue("service_balance")
            var newcar = 0
            var newcus = 0
            var cartype = ""
            var flag = true
            var pile_id = 0L
            if (1 == b_feesingle || 1 == b_fee_service) {
              m_service_balance = 0
            } else {
              m_service_balance = m_service_balance - i_aiscount_balance
            }
            if (v_vin != null) {
              val query_isnewcar = "select vin,pile_id from t_bill where vin=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? AND (member_type=2 or member_type is null)"
              prepareStatement02 = connect1.prepareStatement(query_isnewcar)
              prepareStatement02.setString(1, v_vin)
              prepareStatement02.setString(2, currentTime)
              prepareStatement02.setString(3, nextTime)
              prepareStatement02.setInt(4, l_seller_id)

              resultSet01 = prepareStatement02.executeQuery()
              resultSet01.last()
              val row = resultSet01.getRow
              resultSet01.beforeFirst()
              var x = 0
              while (flag) {
                if (row == 1 || !resultSet01.next()) {
                  newcar = 1
                  flag = false
                } else {
                  pile_id = resultSet01.getLong("pile_id")
                  l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
                  if (x > 1) {
                    newcar = 0
                    flag = false
                  } else {
                    newcar = 1
                  }
                }
              }
              prepareStatement02.close()
              resultSet01.close()
              flag = true
              val queryCarSql = "select carrier_type from t_vehicle where vin=? and  deleted=0 "
              prepareStatement04 = connect2.prepareStatement(queryCarSql.toString())
              prepareStatement04.setString(1, v_vin)
              resultSet03 = prepareStatement04.executeQuery()
              if (resultSet03.next()) {
                carType = resultSet03.getString("carrier_type")
              }
              prepareStatement04.close()
              resultSet03.close()
            }
            val query_isnewcus = "select  member_id,pile_id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0 "
            prepareStatement03 = connect.prepareStatement(query_isnewcus)
            prepareStatement03.setLong(1, i_member_id.toLong)
            prepareStatement03.setString(2, currentTime)
            prepareStatement03.setString(3, nextTime)
            prepareStatement03.setInt(4, l_seller_id)
            resultSet02 = prepareStatement03.executeQuery()
            resultSet02.last()
            val row = resultSet02.getRow
            resultSet02.beforeFirst()
            var x = 0
            while (flag) {
              if (row == 1 || !resultSet02.next()) {
                newcus = 1
                flag = false
              } else {
                pile_id = resultSet02.getLong("pile_id")
                l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
                if (x > 1) {
                  newcus = 0
                  flag = false
                } else {
                  newcus = 1
                }
              }
            }
            prepareStatement03.close()
            resultSet02.close()
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m-%d")
            val station_show_day_sql = new StringBuffer("insert into station_show_day_table values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" day_station_power_count=day_station_power_count+?,") //16
              .append(" day_station_team_power_count=day_station_team_power_count+?,")
              .append(" day_station_balance_count=day_station_balance_count+?,")
              .append(" day_station_pile_count=day_station_pile_count+?,")
              .append(" day_station_service_bus_car_num=day_station_service_bus_car_num+?,") //20
              .append(" day_station_service_other_car_num=day_station_service_other_car_num+?,")
              .append(" day_station_service_team_car_num=day_station_service_team_car_num+?,")
              .append(" day_station_service_pile_team_count=day_station_service_pile_team_count+?,")
              .append(" day_station_service_team_num_count=day_station_service_team_num_count+?")
            prepareStatement = connect.prepareStatement(station_show_day_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            prepareStatement.setString(3, dayd)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(4, m_sum_power)
            prepareStatement.setLong(16, m_sum_power)
            prepareStatement.setLong(6, i_actual_balance)
            prepareStatement.setLong(18, i_actual_balance)
            prepareStatement.setLong(7, 1)
            prepareStatement.setLong(19, 1)
            //车队统计
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(5, m_sum_power)
              prepareStatement.setLong(17, m_sum_power)
              prepareStatement.setLong(10, newcar)
              prepareStatement.setLong(22, newcar)
              prepareStatement.setLong(11, 1)
              prepareStatement.setLong(23, 1)
              prepareStatement.setLong(12, newcus)
              prepareStatement.setLong(24, newcus)
            } else {
              prepareStatement.setLong(5, 0)
              prepareStatement.setLong(17, 0)
              prepareStatement.setLong(10, 0)
              prepareStatement.setLong(22, 0)
              prepareStatement.setLong(11, 0)
              prepareStatement.setLong(23, 0)
              prepareStatement.setLong(12, 0)
              prepareStatement.setLong(24, 0)
            }
            //车类型统计
            if ("GongJiao".equals(carType)) {
              prepareStatement.setLong(8, newcar)
              prepareStatement.setLong(20, newcar)

              prepareStatement.setLong(9, 0)
              prepareStatement.setLong(21, 0)
            } else {
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(20, 0)

              prepareStatement.setLong(9, newcar)
              prepareStatement.setLong(21, newcar)
            }
            prepareStatement.execute()
            prepareStatement.close()
            connect.close()
            connect1.close()
            connect2.close()

          }
        } catch {
          case ex: Exception => log.info("*****station_show_day_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭connect失败****")
          }
          try {
            if (connect1 != null && !connect1.isClosed()) {
              connect1.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭connect失败****")
          }
          try {
            if (connect2 != null && !connect2.isClosed()) {
              connect2.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭connect失败****")
          }
          try {
            if (resultSet01 != null && !resultSet01.isClosed()) {
              resultSet01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭resultSet01失败****")
          }
          try {
            if (resultSet02 != null && !resultSet02.isClosed()) {
              resultSet02.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭resultSet02失败****")
          }
          try {
            if (resultSet03 != null && !resultSet03.isClosed()) {
              resultSet03.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭resultSet03失败****")
          }
          try {
            if (prepareStatement != null && !prepareStatement.isClosed()) {
              prepareStatement.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭prepareStatement失败****")
          }
          try {
            if (prepareStatement02 != null && !prepareStatement02.isClosed()) {
              prepareStatement02.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭prepareStatement02失败****")
          }
          try {
            if (prepareStatement03 != null && !prepareStatement03.isClosed()) {
              prepareStatement03.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭prepareStatement03失败****")
          }
          try {
            if (prepareStatement04 != null && !prepareStatement04.isClosed()) {
              prepareStatement04.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_day_table**关闭prepareStatement04失败****")
          }
        }

        //更新station_show_member_balance_table表
        try {
          connect = DruidUtil.getConnection.get
          val b_delete_flag = data.getString("deleted")
          val b_finish = data.getString("finish")
          val i_member_id = data.getString("member_id")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(i_member_id)) {
            val i_actual_balance = data.getLong("actual_balance")
            val member_balance_sql = new StringBuffer("insert into station_show_member_balance_table values(?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" total_balance_count=total_balance_count+?")
            prepareStatement01 = connect.prepareStatement(member_balance_sql.toString())
            prepareStatement01.setLong(1, i_member_id.toLong)
            prepareStatement01.setLong(2, l_station_id)
            prepareStatement01.setLong(3, i_actual_balance)
            prepareStatement01.setLong(4, i_actual_balance)
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("*****station_show_member_balance_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_member_balance_table**关闭connect失败****")
          }
          try {
            if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
              prepareStatement01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_member_balance_table**关闭prepareStatement01失败****")
          }
        }
        //更新station_show_month_member_table表
        try {
          connect = DruidUtil.getConnection.get
          val b_delete_flag = data.getString("deleted")
          val b_finish = data.getString("finish")
          val i_member_id = data.getString("member_id")
          val l_seller_id = data.getIntValue("seller_id")
          val day = data.getString("time_s")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(i_member_id)
            && !StringUtil.isBlank(day) && l_seller_id != 0) {
            val i_actual_balance = data.getLong("actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val s_member_flag = data.getString("member_type")
            val m_sum_power = data.getLong("sum_power")
            val i_power_balance = data.getLong("power_balance")
            val month_member_sql = new StringBuffer("insert into station_show_month_member_table values(?,?,?,?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_member_charge_count=month_member_charge_count+?,") //11
              .append(" month_team_power_count=month_team_power_count+?,")
              .append(" month_team_balance_count=month_team_balance_count+?,")
              .append(" month_team_power_balance_count=month_team_power_balance_count+?")

            prepareStatement01 = connect.prepareStatement(month_member_sql.toString())
            prepareStatement01.setLong(1, l_station_id)
            prepareStatement01.setInt(2, l_seller_id)
            prepareStatement01.setString(3, dayd)
            prepareStatement01.setLong(4, i_member_id.toLong)
            prepareStatement01.setString(5, "")
            prepareStatement01.setLong(6, s_member_flag.toInt)
            prepareStatement01.setLong(11, 1)
            prepareStatement01.setLong(7, 1)
            prepareStatement01.setLong(9, i_actual_balance)
            prepareStatement01.setLong(13, i_actual_balance)
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement01.setLong(8, m_sum_power)
              prepareStatement01.setLong(12, m_sum_power)
              prepareStatement01.setLong(10, i_power_balance)
              prepareStatement01.setLong(14, i_power_balance)
            } else {
              prepareStatement01.setLong(8, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(14, 0)
            }
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
          }
        } catch {
          case ex: Exception => ex.printStackTrace()
          case ex: Exception => log.info("*****station_show_month_member_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_member_table**关闭connect失败****")
          }
          try {
            if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
              prepareStatement01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_member_table**关闭prepareStatement01失败****")
          }
        }
        
        
         //更新station_show_month_team_balance_table表
        try {
          connect = DruidUtil.getConnection.get
          val b_delete_flag = data.getString("deleted")
          val b_finish = data.getString("finish")
          val i_member_id = data.getString("member_id")
          val l_seller_id = data.getIntValue("seller_id")
          val v_vin = data.getString("vin")
          val day = data.getString("time_s")
            val s_member_flag = data.getString("member_type")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(i_member_id)
            && !StringUtil.isBlank(day) && l_seller_id != 0 && !StringUtil.isBlank(v_vin) 
            && (StringUtil.isBlank(s_member_flag) || "2".equals(s_member_flag))) {
            val i_actual_balance = data.getLong("actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val month_member_sql = new StringBuffer("insert into station_show_month_team_balance_table values(?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_team_actual_balance_count=month_team_actual_balance_count+?")

            prepareStatement01 = connect.prepareStatement(month_member_sql.toString())
            prepareStatement01.setLong(1, l_station_id)
            prepareStatement01.setInt(2, l_seller_id)
            prepareStatement01.setString(3, dayd)
            prepareStatement01.setString(4, v_vin)
            prepareStatement01.setString(5, carNo)
            prepareStatement01.setString(6, i_member_id)
            prepareStatement01.setLong(7, i_actual_balance)
            prepareStatement01.setLong(8, i_actual_balance)
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
          }
        } catch {
          case ex: Exception => log.info("*****station_show_month_team_balance_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_team_balance_table**关闭connect失败****")
          }
          try {
            if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
              prepareStatement01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_team_balance_table**关闭prepareStatement01失败****")
          }
        }
        
        
        
        

        //更新station_show_month_total表
        try {
          connect = DruidUtil.getConnection.get
          connect1 = BillDruidUtil.getConnection.get
          val b_delete_flag = data.getString("deleted")
          val b_finish = data.getString("finish")
          val i_member_id = data.getLong("member_id")
          val l_seller_id = data.getIntValue("seller_id")
          val day = data.getString("time_s")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(day) && l_seller_id != 0) {
            val l_pile_id = data.getLong("pile_id")
            val i_actual_balance = data.getLong("actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val s_member_flag = data.getString("member_type")
            val m_sum_power = data.getLong("sum_power")
            val i_power_balance = data.getLong("power_balance")
            val i_aiscount_balance = data.getLong("discount_balance")
            val b_feesingle = data.getLong("fee_single")
            val i_deductible_balance = data.getLong("deductible_balance")
            val b_fee_service = data.getLong("fee_service")
            val d_time_s = data.getString("time_s")
            val currentT = TimeConUtil.getMonth(day, "1")
            val newT = TimeConUtil.getMonth(day, "0")
            val dateHour = TimeConUtil.getHours(d_time_s)
            var pile_id = 0L
            var isnewC = 0
            var isgrouth = 0
            var flag = true
            val query_newC = "select id,pile_id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0"
            prepareStatement03 = connect1.prepareStatement(query_newC)
            prepareStatement03.setLong(1, i_member_id)
            prepareStatement03.setString(2, currentT)
            prepareStatement03.setString(3, newT)
            prepareStatement03.setLong(4, l_seller_id)
            resultSet01 = prepareStatement03.executeQuery()
            resultSet01.last()
            val row = resultSet01.getRow()
            resultSet01.beforeFirst
            var x = 0
            while (flag) {
              if (row == 1 || !resultSet01.next()) {
                isnewC = 1
                flag = false
              } else {
                pile_id = resultSet01.getLong("pile_id")
                l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
                if (x > 1) {
                  isnewC = 0
                  flag = false
                } else {
                  isnewC = 1
                }
              }
            }
            prepareStatement03.close()
            resultSet01.close()
            flag = true
            val query_grouthC = "select id,pile_id from t_bill where member_id=? and timediff(time_s,?)<=0 and seller_id=? and finish=1 and deleted=0"
            prepareStatement02 = connect1.prepareStatement(query_grouthC)
            prepareStatement02.setLong(1, i_member_id)
            prepareStatement02.setString(2, d_time_s)
            prepareStatement02.setLong(3, l_seller_id)
            resultSet02 = prepareStatement02.executeQuery()
            resultSet02.last()
            val grouth_row = resultSet02.getRow()
            resultSet02.beforeFirst
             x = 0
            while (flag) {
              if (grouth_row == 1 || !resultSet02.next()) {
                isgrouth = 1
                flag = false
              } else {
                pile_id = resultSet02.getLong("pile_id")
                l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
                if (x > 1) {
                  isgrouth = 0
                  flag = false
                } else {
                  isgrouth = 1
                }
              }
            }
            prepareStatement02.close()
            resultSet02.close()
            val month_total_sql = new StringBuffer("insert into station_show_month_table values(")
              .append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,")
              .append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_i_power_count=month_i_power_count+?,") //36
              .append(" month_i_power_balance_count=month_i_power_balance_count+?,")
              .append(" month_pile_num_count=month_pile_num_count+?,")
              .append(" month_pile_personal_num_count=month_pile_personal_num_count+?,")
              .append(" month_charge_personal_num_count=month_charge_personal_num_count+?,") //40
              .append(" month_personal_visit_num=month_personal_visit_num+?,")
              .append(" month_grouth_personal_num=month_grouth_personal_num+?,")
              .append(" month_hour_zero_charge_count=month_hour_zero_charge_count+?,")
              .append(" month_hour_one_charge_count=month_hour_one_charge_count+?,")
              .append(" month_hour_two_charge_count=month_hour_two_charge_count+?,") //45
              .append(" month_hour_three_charge_count=month_hour_three_charge_count+?,")
              .append(" month_hour_four_charge_count=month_hour_four_charge_count+?,")
              .append(" month_hour_five_charge_count=month_hour_five_charge_count+?,")
              .append(" month_hour_six_charge_count=month_hour_six_charge_count+?,")
              .append(" month_hour_seven_charge_count=month_hour_seven_charge_count+?,") //50
              .append(" month_hour_eight_charge_count=month_hour_eight_charge_count+?,")
              .append(" month_hour_nine_charge_count=month_hour_nine_charge_count+?,")
              .append(" month_hour_ten_charge_count=month_hour_ten_charge_count+?,")
              .append(" month_hour_eleven_charge_count=month_hour_eleven_charge_count+?,")
              .append(" month_hour_twelve_charge_count=month_hour_twelve_charge_count+?,") //55
              .append(" month_hour_thirteen_charge_count=month_hour_thirteen_charge_count+?,")
              .append(" month_hour_fourteen_charge_count=month_hour_fourteen_charge_count+?,")
              .append(" month_hour_fifteen_charge_count=month_hour_fifteen_charge_count+?,")
              .append(" month_hour_sixteen_charge_count=month_hour_sixteen_charge_count+?,")
              .append(" month_hour_seventeen_charge_count=month_hour_seventeen_charge_count+?,") //60
              .append(" month_hour_eighteen_charge_count=month_hour_eighteen_charge_count+?,")
              .append(" month_hour_nineteen_charge_count=month_hour_nineteen_charge_count+?,")
              .append(" month_hour_twenty_charge_count=month_hour_twenty_charge_count+?,")
              .append(" month_hour_twenty_one_charge_count=month_hour_twenty_one_charge_count+?,")
              .append(" month_hour_twenty_two_charge_count=month_hour_twenty_two_charge_count+?,") //65
              .append(" month_hour_twenty_three_charge_count=month_hour_twenty_three_charge_count+?")
            prepareStatement01 = connect.prepareStatement(month_total_sql.toString())
            prepareStatement01.setLong(1, l_station_id)
            prepareStatement01.setInt(2, l_seller_id)
            prepareStatement01.setString(3, dayd)
            prepareStatement01.setLong(9, 0)
            prepareStatement01.setLong(4, m_sum_power)
            prepareStatement01.setLong(36, m_sum_power)
            prepareStatement01.setLong(5, i_actual_balance)
            prepareStatement01.setLong(37, i_actual_balance)
            prepareStatement01.setLong(6, 1)
            prepareStatement01.setLong(38, 1)
            //是否参与活动
            if (i_aiscount_balance > 0 || (b_feesingle!=null&&b_feesingle > 0) || i_deductible_balance > 0 || (b_fee_service!=null&&b_fee_service > 0)) {
              prepareStatement01.setLong(10, 1)
              prepareStatement01.setLong(41, 1)
            } else {
              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(41, 0)
            }
            //是否个人会员
            if ("1".equals(s_member_flag)) {
              prepareStatement01.setLong(7, 1)
              prepareStatement01.setLong(39, 1)
              prepareStatement01.setLong(8, isnewC)
              prepareStatement01.setLong(40, isnewC)
              prepareStatement01.setLong(11, isgrouth)
              prepareStatement01.setLong(42, isgrouth)
            } else {
              prepareStatement01.setLong(7, 0)
              prepareStatement01.setLong(39, 0)
              prepareStatement01.setLong(8, 0)
              prepareStatement01.setLong(40, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(42, 0)
            }
            //小时分类统计
            for (i <- 12 to 35) {
              if (i == 12 + dateHour) {
                prepareStatement01.setLong(i, 1)
              } else {
                prepareStatement01.setLong(i, 0)
              }
            }

            for (j <- 43 to 66) {
              if (j == 43 + dateHour) {
                prepareStatement01.setLong(j, 1)
              } else {
                prepareStatement01.setLong(j, 0)
              }
            }
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
            connect1.close()

          }
        } catch {
          case ex: Exception => ex.printStackTrace()
          case ex: Exception => log.info("*****station_show_month_table**mysql处理异常****")
        } finally {
          try {
            if (connect != null && !connect.isClosed()) {
              connect.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭connect失败****")
          }
          try {
            if (connect1 != null && !connect1.isClosed()) {
              connect1.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭connect失败****")
          }

          try {
            if (resultSet01 != null && !resultSet01.isClosed()) {
              resultSet01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭resultSet01失败****")
          }
          try {
            if (resultSet02 != null && !resultSet02.isClosed()) {
              resultSet02.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭resultSet02失败****")
          }
          try {
            if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
              prepareStatement01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement01失败****")
          }
          try {
            if (prepareStatement02 != null && !prepareStatement02.isClosed()) {
              prepareStatement02.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement02失败****")
          }
          try {
            if (prepareStatement03 != null && !prepareStatement03.isClosed()) {
              prepareStatement03.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement03失败****")
          }
        }
        //更新station_show_total_table表
        try {
          connect = DruidUtil.getConnection.get
          connect1 = BillDruidUtil.getConnection.get
          val b_delete_flag = data.getString("deleted")
          val b_finish = data.getString("finish")
          val i_member_id = data.getLong("member_id")
          val l_seller_id = data.getIntValue("seller_id")
          val day = data.getString("time_s")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && i_member_id != 0 && l_seller_id != 0) {
            val l_pile_id = data.getLong("pile_id")
            val i_actual_balance = data.getLong("actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val s_member_flag = data.getString("member_flag")
            val m_sum_power = data.getLong("sum_power")
            val i_power_balance = data.getLong("power_balance")
            val d_time_s = data.getString("time_s")
            val d_time_e = data.getString("time_e")
            val diffT = TimeConUtil.timeDIFF(d_time_s, d_time_e)
            val v_vin = data.getString("vin")
            var isnewC = 0
            var isnewcar = 0
            var pile_id = 0L
            var flag = true
            val query_newC = "select id,pile_id from t_bill where member_id=? and finish=1 and deleted=0 and  seller_id=?"
            prepareStatement03 = connect1.prepareStatement(query_newC)
            prepareStatement03.setLong(1, i_member_id)
            prepareStatement03.setInt(2, l_seller_id)
            resultSet01 = prepareStatement03.executeQuery()
            resultSet01.last()
            val row = resultSet01.getRow
            resultSet01.beforeFirst()
            var x = 0
            while (flag) {
              if (row == 1 || !resultSet01.next()) {
                isnewC = 1
                flag = false
              } else {
                pile_id = resultSet01.getLong("pile_id")
                l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
                if (x > 1) {
                  isnewC = 0
                  flag = false
                } else {
                  isnewC = 1
                }
              }
            }
            prepareStatement03.close()
            resultSet01.close()
            flag = true
            if (!StringUtil.isBlank(v_vin)) {
              val query_newCar = "select vin,pile_id from t_bill where vin=? and finish=1 and deleted=0 and seller_id=?"
              prepareStatement02 = connect1.prepareStatement(query_newCar)
              prepareStatement02.setString(1, v_vin)
              prepareStatement02.setInt(2, l_seller_id)
              resultSet02 = prepareStatement02.executeQuery()
              resultSet02.last()
              val row = resultSet02.getRow
              resultSet02.beforeFirst()
              var x = 0
              while (flag) {
                if (row == 1 || !resultSet02.next()) {
                  isnewcar = 1
                  flag = false
                } else {
                  pile_id = resultSet02.getLong("pile_id")
                  l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
                  if (x > 1) {
                    isnewcar = 0
                    flag = false
                  } else {
                    isnewcar = 1
                  }
                }
              }
              prepareStatement02.close()
              resultSet02.close()
            }
            val station_show_total_sql = new StringBuffer("insert into station_show_total_table values(")
              .append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" total_station_service_count=total_station_service_count+?,") //26
              .append(" total_station_service_use_count=total_station_service_use_count+?,")
              .append(" total_station_service_team_count=total_station_service_team_count+?,")
              .append(" total_station_team_power_count=total_station_team_power_count+?,")
              .append(" total_station_team_bill_count=total_station_team_bill_count+?,") //30
              .append(" total_station_team_menoy_count=total_station_team_menoy_count+?,")
              .append(" total_station_service_team_cars=total_station_service_team_cars+?,")
              .append(" total_station_service_team_time=total_station_service_team_time+?,")
              .append(" total_station_service_silver_use_count=total_station_service_silver_use_count+?,")
              .append(" total_station_service_gold_use_count=total_station_service_gold_use_count+?,") //35
              .append(" total_station_service_platinum_use_count=total_station_service_platinum_use_count+?,")
              .append(" total_station_service_diamond_use_count=total_station_service_diamond_use_count+?,")
              .append(" total_station_service_blackgold_use_count=total_station_service_blackgold_use_count+?,")
              .append(" total_station_power_count=total_station_power_count+?,")
              .append(" total_station_money_count=total_station_money_count+?") //40
            prepareStatement01 = connect.prepareStatement(station_show_total_sql.toString())
            prepareStatement01.setLong(1, l_station_id)
            prepareStatement01.setInt(2, l_seller_id)
            prepareStatement01.setLong(3, 0)
            prepareStatement01.setLong(4, 0)
            prepareStatement01.setLong(5, 1)
            prepareStatement01.setLong(26, 1)
            prepareStatement01.setLong(18, m_sum_power)
            prepareStatement01.setLong(39, m_sum_power)
            prepareStatement01.setLong(19, i_actual_balance)
            prepareStatement01.setLong(40, i_actual_balance)
            prepareStatement01.setLong(20, 0)
            prepareStatement01.setLong(21, 0)
            prepareStatement01.setLong(22, 0)
            prepareStatement01.setLong(23, 0)
            prepareStatement01.setLong(24, 0)
            prepareStatement01.setLong(25, 0)

            //个人车队分类
            if ("1".equals(s_member_flag)) {
              prepareStatement01.setLong(6, 1)
              prepareStatement01.setLong(27, 1)

              prepareStatement01.setLong(7, 0)
              prepareStatement01.setLong(28, 0)
              prepareStatement01.setLong(8, 0)
              prepareStatement01.setLong(29, 0)
              prepareStatement01.setLong(9, 0)
              prepareStatement01.setLong(30, 0)
              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(31, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(32, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(33, 0)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement01.setLong(7, isnewC)
              prepareStatement01.setLong(28, isnewC)
              prepareStatement01.setLong(8, m_sum_power)
              prepareStatement01.setLong(29, m_sum_power)
              prepareStatement01.setLong(9, 1)
              prepareStatement01.setLong(30, 1)
              prepareStatement01.setLong(10, i_actual_balance)
              prepareStatement01.setLong(31, i_actual_balance)
              prepareStatement01.setLong(11, isnewcar)
              prepareStatement01.setLong(32, isnewcar)
              prepareStatement01.setLong(12, diffT)
              prepareStatement01.setLong(33, diffT)

              prepareStatement01.setLong(6, 0)
              prepareStatement01.setLong(27, 0)
            } else {
              prepareStatement01.setLong(7, 0)
              prepareStatement01.setLong(28, 0)
              prepareStatement01.setLong(8, 0)
              prepareStatement01.setLong(29, 0)
              prepareStatement01.setLong(9, 0)
              prepareStatement01.setLong(30, 0)
              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(31, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(32, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(33, 0)

              prepareStatement01.setLong(6, 0)
              prepareStatement01.setLong(27, 0)
            }
            //会员等级统计
            if (isnewC == 1 && "1".equals(level)) {
              prepareStatement01.setLong(13, 1)
              prepareStatement01.setLong(34, 1)

              prepareStatement01.setLong(14, 0)
              prepareStatement01.setLong(35, 0)
              prepareStatement01.setLong(15, 0)
              prepareStatement01.setLong(36, 0)
              prepareStatement01.setLong(16, 0)
              prepareStatement01.setLong(37, 0)
              prepareStatement01.setLong(17, 0)
              prepareStatement01.setLong(38, 0)
            } else if (isnewC == 1 && "2".equals(level)) {
              prepareStatement01.setLong(13, 0)
              prepareStatement01.setLong(34, 0)

              prepareStatement01.setLong(14, 1)
              prepareStatement01.setLong(35, 1)
              prepareStatement01.setLong(15, 0)
              prepareStatement01.setLong(36, 0)
              prepareStatement01.setLong(16, 0)
              prepareStatement01.setLong(37, 0)
              prepareStatement01.setLong(17, 0)
              prepareStatement01.setLong(38, 0)
            } else if (isnewC == 1 && "3".equals(level)) {
              prepareStatement01.setLong(13, 0)
              prepareStatement01.setLong(34, 0)

              prepareStatement01.setLong(14, 0)
              prepareStatement01.setLong(35, 0)
              prepareStatement01.setLong(15, 1)
              prepareStatement01.setLong(36, 1)
              prepareStatement01.setLong(16, 0)
              prepareStatement01.setLong(37, 0)
              prepareStatement01.setLong(17, 0)
              prepareStatement01.setLong(38, 0)
            } else if (isnewC == 1 && "4".equals(level)) {
              prepareStatement01.setLong(13, 0)
              prepareStatement01.setLong(34, 0)

              prepareStatement01.setLong(14, 0)
              prepareStatement01.setLong(35, 0)
              prepareStatement01.setLong(15, 0)
              prepareStatement01.setLong(36, 0)
              prepareStatement01.setLong(16, 1)
              prepareStatement01.setLong(37, 1)
              prepareStatement01.setLong(17, 0)
              prepareStatement01.setLong(38, 0)
            } else if (isnewC == 1 && "5".equals(level)) {
              prepareStatement01.setLong(13, 0)
              prepareStatement01.setLong(34, 0)

              prepareStatement01.setLong(14, 0)
              prepareStatement01.setLong(35, 0)
              prepareStatement01.setLong(15, 0)
              prepareStatement01.setLong(36, 0)
              prepareStatement01.setLong(16, 0)
              prepareStatement01.setLong(37, 0)
              prepareStatement01.setLong(17, 1)
              prepareStatement01.setLong(38, 1)
            } else {
              prepareStatement01.setLong(13, 0)
              prepareStatement01.setLong(34, 0)

              prepareStatement01.setLong(14, 0)
              prepareStatement01.setLong(35, 0)
              prepareStatement01.setLong(15, 0)
              prepareStatement01.setLong(36, 0)
              prepareStatement01.setLong(16, 0)
              prepareStatement01.setLong(37, 0)
              prepareStatement01.setLong(17, 0)
              prepareStatement01.setLong(38, 0)
            }
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
            connect1.close()
          }
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
            if (connect1 != null && !connect1.isClosed()) {
              connect1.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭connect失败****")
          }

          try {
            if (resultSet01 != null && !resultSet01.isClosed()) {
              resultSet01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭resultSet01失败****")
          }
          try {
            if (resultSet02 != null && !resultSet02.isClosed()) {
              resultSet02.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭resultSet02失败****")
          }
          try {
            if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
              prepareStatement01.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭prepareStatement01失败****")
          }
          try {
            if (prepareStatement02 != null && !prepareStatement02.isClosed()) {
              prepareStatement02.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭prepareStatement02失败****")
          }
          try {
            if (prepareStatement03 != null && !prepareStatement03.isClosed()) {
              prepareStatement03.close()
            }
          } catch {
            case ex: Exception => log.info("***station_show_total_table**关闭prepareStatement03失败****")
          }
        }

      } else {
        log.info("***查询站点或桩信息失败****l_station_id：" + l_station_id + " l_pile_id_arr:" + l_pile_id_arr.toString())
      }
    }

  }

}