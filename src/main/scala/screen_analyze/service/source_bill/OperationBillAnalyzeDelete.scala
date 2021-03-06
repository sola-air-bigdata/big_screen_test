package screen_analyze.service.source_bill
import com.alibaba.fastjson.JSONObject
import screen_analyze.util.StringUtil
import screen_analyze.util.TimeConUtil
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet
import org.apache.log4j.Logger
import screen_analyze.util.DruidUtil
import scala.collection.mutable.ArrayBuffer
/**
 * 监听t_origin_history_operation_bill表delete操作
 */
class OperationBillAnalyzeDelete {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var l_station_id = 0L
  var l_pile_id_arr: scala.collection.mutable.ArrayBuffer[Long] = ArrayBuffer()
  var level = ""
  var carType = ""
  var prepareStatement: PreparedStatement = _
  var prepareStatement01: PreparedStatement = _
  var prepareStatement02: PreparedStatement = _
  var prepareStatement03: PreparedStatement = _
  var prepareStatement04: PreparedStatement = _
  var connect: Connection = _
  var resultSet: ResultSet = _
  var resultSet01: ResultSet = _
  var resultSet02: ResultSet = _
  var resultSet03: ResultSet = _
  //分析删数据
  def analyzeDeleteTable(jsonSql: JSONObject) {
//    log.info("********OperationBillAnalyzeDelete处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    var data = new JSONObject
    var arrSize = dataArr.size()
    for (m <- 0 to arrSize - 1) {
      data = dataArr.getJSONObject(m)
      //查询会员等级
      try {
        connect = DruidUtil.getConnection.get
        val i_member_id = data.getString("i_member_id")
        val v_vin = data.getString("v_vin")
        if (!StringUtil.isBlank(i_member_id)) {
          val query_level_sql = "select i_level from t_business_base_operation_persional_member where id=?"
          prepareStatement02 = connect.prepareStatement(query_level_sql)
          prepareStatement02.setLong(1, i_member_id.toLong)
          resultSet01 = prepareStatement02.executeQuery()
          if (resultSet01.next()) {
            level = resultSet01.getString("i_level")
          }
          prepareStatement02.close()
          resultSet01.close()
          connect.close()
        }
        if (!StringUtil.isBlank(v_vin)) {
          val queryCarSql = "select v_carrier_type from t_business_base_operationl_vehicle where v_vin=? and b_delete_flag=0"
          prepareStatement02 = connect.prepareStatement(queryCarSql)
          prepareStatement02.setString(1, v_vin)
          resultSet01 = prepareStatement02.executeQuery()
          if (resultSet01.next()) {
            carType = resultSet01.getString("v_carrier_type")
          }
          prepareStatement02.close()
          resultSet01.close()
          connect.close()
        }
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
        val s_member_flag = data.getString("s_member_flag")
        val b_finish = data.getString("b_finish")
        val i_member_id = data.getString("i_member_id")
        val day = data.getString("d_time_s")
        if (("1".equals(s_member_flag)|| !"GongJiao".equals(carType)||StringUtil.isBlank(carType)) && "1".equals(b_finish) 
            && !StringUtil.isBlank(i_member_id) && !StringUtil.isBlank(day)) {
          val m_consume_balance = data.getLong("m_consume_balance")
          val i_actual_balance = data.getLong("i_actual_balance")
          val l_seller_id = data.getIntValue("l_seller_id")
          val dayd = TimeConUtil.timeConversion(day, "%Y-%m-%d")
          val insertSql = new StringBuffer("update t_all_day_member_table set")
          val all_day_member_sql = insertSql
            .append(" day_personal_charge_count=day_personal_charge_count-1,")
            .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count-?,")
            .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count-?,")
            .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank-?")
            .append(" where l_seller_id=? and d_day_time=? and l_member_id=?")
          prepareStatement = connect.prepareStatement(all_day_member_sql.toString())
          prepareStatement.setLong(1, m_consume_balance)
          prepareStatement.setLong(2, i_actual_balance)
          prepareStatement.setLong(3, i_actual_balance)
          prepareStatement.setInt(4, l_seller_id)
          prepareStatement.setString(5, dayd)
          prepareStatement.setLong(6, i_member_id.toLong)
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
        connect = DruidUtil.getConnection.get
        val s_member_flag = data.getString("s_member_flag")
        val b_finish = data.getString("b_finish")
        val l_seller_id = data.getIntValue("l_seller_id")
        val i_member_id = data.getString("i_member_id")
        val b_delete_flag = data.getString("b_delete_flag")
        val day = data.getString("d_time_s")
        val i_bus_type = data.getString("i_bus_type")
        val i_actual_balance = data.getLongValue("i_actual_balance")
        val i_power_balance = data.getLongValue("i_power_balance")
        val b_feesingle = data.getString("b_feesingle")
        val v_vin = data.getString("v_vin")
        var carType = ""
        var isnewC = 0
        if ("1".equals(b_finish) && "0".equals(b_delete_flag) && !StringUtil.isBlank(day)) {
          val dayd = TimeConUtil.timeConversion(day, "%Y-%m-%d")
          //查询车类型
          if (v_vin != null) {
            val queryCarSql = "select v_carrier_type from t_business_base_operationl_vehicle where v_vin=? and  b_delete_flag=0 "
            prepareStatement01 = connect.prepareStatement(queryCarSql.toString())
            prepareStatement01.setString(1, v_vin)
            resultSet = prepareStatement01.executeQuery()
            if (resultSet.next()) {
              carType = resultSet.getString("v_carrier_type")
            }
            prepareStatement01.close()
            resultSet.close()
          }
          val curT = TimeConUtil.getTime(day, "1")
          val nextT = TimeConUtil.getTime(day, "0")
          val querynewC = "select id from t_origin_history_operation_bill where i_member_id=? and timediff(d_time_s,?)>0 and timediff(d_time_s,?)<0 and l_seller_id=? and b_finish=1 and b_delete_flag=0"
          prepareStatement = connect.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, curT)
          prepareStatement.setString(3, nextT)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          if (!resultSet.next()) {
            isnewC = 1
          }
          prepareStatement.close()
          resultSet.close()

          val t_all_day_sql = new StringBuffer("update t_all_day_table set")
            .append(" day_team_active_count=day_team_active_count-?,")
            .append(" day_personal_active_count=day_personal_active_count-?,")
            .append(" day_team_service_user=day_team_service_user-?,") //26
            .append(" day_personal_service_user=day_personal_service_user-?,")
            .append(" day_service_app_user=day_service_app_user-?,")
            .append(" day_service_wechat_user=day_service_wechat_user-?,")
            .append(" day_i_actual_balance=day_i_actual_balance-?,") //30
            .append(" day_i_power_balance=day_i_power_balance-?,")
            .append(" day_i_actual_balance_balance_nteam=day_i_actual_balance_balance_nteam-?,")
            .append(" day_i_actual_balance_balance_WuLiu=day_i_actual_balance_balance_WuLiu-?,")
            .append(" day_i_actual_balance_balance_ChuZu=day_i_actual_balance_balance_ChuZu-?,")
            .append(" day_i_actual_balance_balance_KeYun=day_i_actual_balance_balance_KeYun-?,") //35
            .append(" day_i_actual_balance_balance_WangYue=day_i_actual_balance_balance_WangYue-?,")
            .append(" day_i_actual_balance_balance_QiTa=day_i_actual_balance_balance_QiTa-?,")
            .append(" day_i_power_balance_nteam=day_i_power_balance_nteam-?,")
            .append(" day_i_actual_balance_vin=day_i_actual_balance_vin-?,")
            .append(" day_i_actual_balance_card=day_i_actual_balance_card-?,")
            .append(" day_i_actual_balance_app=day_i_actual_balance_app-?,")
            .append(" day_i_actual_balance_wechat=day_i_actual_balance_wechat-?")
            .append(" where l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(t_all_day_sql.toString())
          prepareStatement.setInt(20, l_seller_id)
          prepareStatement.setString(21, dayd)
          //个人或车队更新
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(2, 1)
            prepareStatement.setLong(4, isnewC)

            prepareStatement.setLong(1, 0)
            prepareStatement.setLong(3, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(3, isnewC)

            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(4, 0)

          } else {
            prepareStatement.setLong(1, 0)
            prepareStatement.setLong(3, 0)

            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(4, 0)
          }
          //服务类型更新
          if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(5, isnewC)
            prepareStatement.setLong(6, 0)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(6, isnewC)

            prepareStatement.setLong(5, 0)
          } else {
            prepareStatement.setLong(6, 0)

            prepareStatement.setLong(5, 0)
          }
          //总收入统计
          if ("0".equals(b_feesingle)) {
            prepareStatement.setLong(7, i_actual_balance)
            prepareStatement.setLong(8, i_power_balance)
          } else {
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(8, 0)
          }
          //车类型统计
          if (!"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(9, i_actual_balance)
            prepareStatement.setLong(15, i_power_balance)
          } else {
            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(15, 0)
          }
          if ("WuLiu".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(10, i_power_balance)

            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
          } else if ("ChuZu".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(11, i_power_balance)

            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
          } else if ("KeYun".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(12, i_power_balance)

            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
          } else if ("WangYue".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(13, i_power_balance)

            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(14, 0)
          } else if ("QiTa".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(14, i_power_balance)

            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(10, 0)
          } else {
            prepareStatement.setLong(14, 0)

            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(10, 0)
          }
          //业务类型统计
          if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(16, i_actual_balance)

            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(19, 0)
          } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(17, i_actual_balance)

            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(19, 0)
          } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(18, i_actual_balance)

            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(16, 0)
            prepareStatement.setLong(19, 0)
          } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(19, i_actual_balance)

            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(16, 0)
          } else {
            prepareStatement.setLong(19, 0)

            prepareStatement.setLong(18, 0)
            prepareStatement.setLong(17, 0)
            prepareStatement.setLong(16, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          connect.close()
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
        val s_member_flag = data.getString("s_member_flag")
        val b_finish = data.getString("b_finish")
        val i_member_id = data.getString("i_member_id")
        val day = data.getString("d_time_s")
        val l_seller_id = data.getIntValue("l_seller_id")
        val b_delete_flag = data.getString("b_delete_flag")
        if ("1".equals(b_finish) && !StringUtil.isBlank(day) && "0".equals(b_delete_flag)) {
          val m_appoint_balance = data.getLongValue("m_appoint_balance")
          val i_power_balance = data.getLongValue("i_power_balance")
          val m_parking_balance = data.getLongValue("m_parking_balance")
          val b_feesingle = data.getLongValue("b_feesingle")
          val b_fee_service = data.getLongValue("b_fee_service")
          val m_sum_power = data.getLongValue("m_sum_power")
          val i_deductible_balance = data.getLongValue("i_deductible_balance")
          var power_balance = i_power_balance
          var isfee = 0L
          val i_aiscount_balance = data.getLongValue("i_aiscount_balance")
          val m_service_balance = data.getLongValue("m_service_balance")
          if (1 == b_feesingle || 1 == b_fee_service) {
           isfee = m_service_balance - i_aiscount_balance
          }
          if(b_feesingle == 1){
            power_balance = 0
          }
          val serviceban = (m_service_balance-isfee-i_aiscount_balance-i_deductible_balance)+m_appoint_balance+m_parking_balance
          val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
          val t_all_month_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_team_charging_power=month_team_charging_power-?,")
            .append(" month_personal_charging_power=month_personal_charging_power-?,")
            .append(" month_i_charging_actual_balance=month_i_charging_actual_balance-?,")
            .append(" month_i_service_actual_balance=month_i_service_actual_balance-?,")
            .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance-?,")
            .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(t_all_month_sql.toString())
          prepareStatement.setLong(3, power_balance)
          prepareStatement.setLong(4, serviceban)
          prepareStatement.setInt(7, l_seller_id)
          prepareStatement.setString(8, dayd)
          //个人车队统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(2, m_sum_power)
            prepareStatement.setLong(5, power_balance)
            prepareStatement.setLong(6, serviceban)

            prepareStatement.setLong(1, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(1, m_sum_power)

            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(6, 0)
          } else {
            prepareStatement.setLong(1, 0)

            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(6, 0)
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
        val b_finish = data.getString("b_finish")
        val b_delete_flag = data.getString("b_delete_flag")
        if ("1".equals(b_finish) && "0".equals(b_delete_flag)) {
          val l_seller_id = data.getIntValue("l_seller_id")
          val i_power_balance = data.getLongValue("i_power_balance")
          val b_feesingle = data.getLongValue("b_feesingle")
          val b_fee_service = data.getLongValue("b_fee_service")
          val m_sum_power = data.getLongValue("m_sum_power")
          val d_time_s = data.getString("d_time_s")
          val d_time_e = data.getString("d_time_e")
          val diffT = TimeConUtil.timeDIFF(d_time_s, d_time_e)
          val i_member_id = data.getString("i_member_id")
          val l_leaguer_id = data.getString("l_leaguer_id")
          val i_actual_balance = data.getLong("i_actual_balance")
          val i_bus_type = data.getString("i_bus_type")
          val s_member_flag = data.getString("s_member_flag")
          val i_aiscount_balance = data.getLongValue("i_aiscount_balance")
          var isnewC = 0

          val t_all_sql = new StringBuffer("update t_all_table set")
            .append(" total_charging_power=total_charging_power-?,")
            .append(" team_charging_power=team_charging_power-?,")
            .append(" total_service_count=total_service_count-?,")
            .append(" team_service_count=team_service_count-?,") //40
            .append(" personal_service_count=personal_service_count-?,")
            .append(" total_service_time=total_service_time-?,")
            .append(" team_service_time=team_service_time-?,")
            .append(" personal_service_time=personal_service_time-?,")
            .append(" total_service_user_count=total_service_user_count-?,") //45
            .append(" total_service_team_user_count=total_service_team_user_count-?,")
            .append(" total_service_personal_user_count=total_service_personal_user_count-?,")
            .append(" total_service_silver_user_count=total_service_silver_user_count-?,")
            .append(" total_service_gold_user_count=total_service_gold_user_count-?,")
            .append(" total_service_platinum_user_count=total_service_platinum_user_count-?,") //50
            .append(" total_service_diamond_user_count=total_service_diamond_user_count-?,")
            .append(" total_service_blackgold_user_count=total_service_blackgold_user_count-?,")
            .append(" total_i_actual_balance=total_i_actual_balance-?,")
            .append(" team_i_actual_balance=team_i_actual_balance-?,")
            .append(" personal_i_actual_balance=personal_i_actual_balance-?,") //55
            .append(" total_i_power_balance=total_i_power_balance-?,")
            .append(" total_i_service_balance=total_i_service_balance-?,")
            .append(" total_vin_actual_balance=total_vin_actual_balance-?,")
            .append(" total_card_actual_balance=total_card_actual_balance-?,")
            .append(" total_wechat_actual_balance=total_wechat_actual_balance-?,") //60
            .append(" total_app_actual_balance=total_app_actual_balance-?")
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(t_all_sql.toString())
          if (!StringUtil.isBlank(i_member_id)) {
            val query_sql = "select id from t_origin_history_operation_bill where i_member_id=? and l_seller_id=? and b_finish=1 and b_delete_flag=0"
            prepareStatement01 = connect.prepareStatement(query_sql)
            prepareStatement01.setLong(1, i_member_id.toLong)
            prepareStatement01.setInt(2, l_seller_id)
            resultSet = prepareStatement01.executeQuery()
            if (!resultSet.next()) {
              isnewC = 1
            }
            prepareStatement01.close()
            resultSet.close()
          } else if (!StringUtil.isBlank(l_leaguer_id)) {
            val query_sql = "select id from t_origin_history_operation_bill where l_leaguer_id=? and l_seller_id=? and b_finish=1 and b_delete_flag=0 "
            prepareStatement01 = connect.prepareStatement(query_sql)
            prepareStatement01.setLong(1, l_leaguer_id.toLong)
            prepareStatement01.setInt(2, l_seller_id)
            resultSet = prepareStatement01.executeQuery()
            if (!resultSet.next()) {
              isnewC = 1
            }
            prepareStatement01.close()
            resultSet.close()
          }

          var m_service_balance = data.getLongValue("m_service_balance")
          if (1 == b_feesingle || 1 == b_fee_service) {
            m_service_balance = 0
          } else {
            m_service_balance = m_service_balance - i_aiscount_balance
          }
          prepareStatement.setInt(26, l_seller_id)
          prepareStatement.setLong(1, m_sum_power)
          prepareStatement.setLong(3, 1)
          prepareStatement.setLong(6, diffT)
          prepareStatement.setLong(9, isnewC)
          prepareStatement.setLong(17, i_actual_balance)
          prepareStatement.setLong(20, i_power_balance)
          prepareStatement.setLong(21, m_service_balance)
          //业务类型统计
          if ("3".equals(i_bus_type)) {
            prepareStatement.setLong(22, i_actual_balance)

            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(25, 0)
          } else if ("1".equals(i_bus_type)) {
            prepareStatement.setLong(23, i_actual_balance)

            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(24, 0)
            prepareStatement.setLong(25, 0)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(24, i_actual_balance)

            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(25, 0)
          } else if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(25, i_actual_balance)

            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(24, 0)
          } else {
            prepareStatement.setLong(25, 0)

            prepareStatement.setLong(22, 0)
            prepareStatement.setLong(23, 0)
            prepareStatement.setLong(24, 0)
          }
          //会员等级统计
          if ("1".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(12, 1)

            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(16, 0)
          } else if ("2".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(13, 1)

            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(16, 0)
          } else if ("3".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(14, 1)

            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(15, 0)
            prepareStatement.setLong(16, 0)
          } else if ("4".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(15, 1)

            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(16, 0)
          } else if ("5".equals(level) && 1 == isnewC) {
            prepareStatement.setLong(16, 1)

            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(15, 0)
          } else {
            prepareStatement.setLong(16, 0)

            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(13, 0)
            prepareStatement.setLong(14, 0)
            prepareStatement.setLong(15, 0)
          }
          //车队个人类型统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(8, diffT)
            prepareStatement.setLong(11, isnewC)
            prepareStatement.setLong(19, i_actual_balance)

            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(18, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, m_sum_power)
            prepareStatement.setLong(4, 1)
            prepareStatement.setLong(7, diffT)
            prepareStatement.setLong(10, isnewC)
            prepareStatement.setLong(18, i_actual_balance)

            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(19, 0)
          } else {
            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(18, 0)

            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(19, 0)
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
        connect = DruidUtil.getConnection.get
        val s_member_flag = data.getString("s_member_flag")
        val l_pile_id = data.getLongValue("l_pile_id")
        val query_stid = "select l_station_id from t_business_base_operation_pile where id=?"
        prepareStatement = connect.prepareStatement(query_stid)
        prepareStatement.setLong(1, l_pile_id)
        resultSet = prepareStatement.executeQuery()
        if (resultSet.next()) {
          l_station_id = resultSet.getLong("l_station_id")
        }
        prepareStatement.close()
        resultSet.close()
        if (l_station_id != 0) {
          val query_l_pile_arr = "select id from t_business_base_operation_pile where l_station_id=?"
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
          val s_member_flag = data.getString("s_member_flag")
          val b_finish = data.getString("b_finish")
          val i_member_id = data.getString("i_member_id")
          val day = data.getString("d_time_s")
          val l_seller_id = data.getIntValue("l_seller_id")
          val b_delete_flag = data.getString("b_delete_flag")
          val l_pile_id = data.getLongValue("l_pile_id")
          val i_actual_balance = data.getLongValue("i_actual_balance")
          if ("1".equals(b_finish) && !StringUtil.isBlank(day) && "0".equals(b_delete_flag) && l_pile_id != 0) {
            val m_appoint_balance = data.getLongValue("m_appoint_balance")
            val i_power_balance = data.getLongValue("i_power_balance")
            val m_parking_balance = data.getLongValue("m_parking_balance")
            val b_feesingle = data.getLongValue("b_feesingle")
            val b_fee_service = data.getLongValue("b_fee_service")
            val m_sum_power = data.getLongValue("m_sum_power")
            val v_vin = data.getString("v_vin")
            val currentTime = TimeConUtil.getTime(day, "1")
            val i_aiscount_balance = data.getLongValue("i_aiscount_balance")
            val nextTime = TimeConUtil.getTime(day, "0")
            var carType = ""
            var m_service_balance = data.getLongValue("m_service_balance")
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
              val query_isnewcar = "select v_vin,l_pile_id from t_origin_history_operation_bill where v_vin=? and timediff(d_time_s,?)>0 and timediff(d_time_s,?)<0 and l_seller_id=? AND (s_member_flag=2 or s_member_flag is null)"
              prepareStatement02 = connect.prepareStatement(query_isnewcar)
              prepareStatement02.setString(1, v_vin)
              prepareStatement02.setString(2, currentTime)
              prepareStatement02.setString(3, nextTime)
              prepareStatement02.setInt(4, l_seller_id)

              resultSet01 = prepareStatement02.executeQuery()
              while (flag) {
                if (!resultSet01.next()) {
                  newcar = 1
                  flag = false
                } else {
                  pile_id = resultSet01.getLong("l_pile_id")
                  if (l_pile_id_arr.contains(pile_id)) {
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
              val queryCarSql = "select v_carrier_type from t_business_base_operationl_vehicle where v_vin=? and  b_delete_flag=0 "
              prepareStatement04 = connect.prepareStatement(queryCarSql.toString())
              prepareStatement04.setString(1, v_vin)
              resultSet03 = prepareStatement04.executeQuery()
              if (resultSet03.next()) {
                carType = resultSet03.getString("v_carrier_type")
              }
              prepareStatement04.close()
              resultSet03.close()
            }
            val query_isnewcus = "select distinct i_member_id,l_pile_id from t_origin_history_operation_bill where i_member_id=? and timediff(d_time_s,?)>0 and timediff(d_time_s,?)<0 and l_seller_id=? and b_finish=1 and b_delete_flag=0 "
            prepareStatement03 = connect.prepareStatement(query_isnewcus)
            prepareStatement03.setString(1, i_member_id)
            prepareStatement03.setString(2, currentTime)
            prepareStatement03.setString(3, nextTime)
            prepareStatement03.setInt(4, l_seller_id)
            resultSet02 = prepareStatement03.executeQuery()
            while (flag) {
              if (!resultSet02.next()) {
                newcus = 1
                flag = false
              } else {
                pile_id = resultSet02.getLong("l_pile_id")
                if (l_pile_id_arr.contains(pile_id)) {
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
            val station_show_day_sql = new StringBuffer("update station_show_day_table set")
              .append(" day_station_power_count=day_station_power_count-?,") //16
              .append(" day_station_team_power_count=day_station_team_power_count-?,")
              .append(" day_station_balance_count=day_station_balance_count-?,")
              .append(" day_station_pile_count=day_station_pile_count-?,")
              .append(" day_station_service_bus_car_num=day_station_service_bus_car_num-?,") //20
              .append(" day_station_service_other_car_num=day_station_service_other_car_num-?,")
              .append(" day_station_service_team_car_num=day_station_service_team_car_num-?,")
              .append(" day_station_service_pile_team_count=day_station_service_pile_team_count-?,")
              .append(" day_station_service_team_num_count=day_station_service_team_num_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
            prepareStatement = connect.prepareStatement(station_show_day_sql.toString())
            prepareStatement.setLong(10, l_station_id)
            prepareStatement.setInt(11, l_seller_id)
            prepareStatement.setString(12, dayd)
            prepareStatement.setLong(1, m_sum_power)
            prepareStatement.setLong(3, i_actual_balance)
            prepareStatement.setLong(4, 1)
            //车队统计
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, m_sum_power)
              prepareStatement.setLong(7, newcar)
              prepareStatement.setLong(8, 1)
              prepareStatement.setLong(9, newcus)
            } else {
              prepareStatement.setLong(2, 0)
              prepareStatement.setLong(7, 0)
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(9, 0)
            }
            //车类型统计
            if ("GongJiao".equals(carType)) {
              prepareStatement.setLong(5, newcar)

              prepareStatement.setLong(6, 0)
            } else {
              prepareStatement.setLong(5, 0)

              prepareStatement.setLong(6, newcar)
            }
            prepareStatement.execute()
            prepareStatement.close()
            connect.close()

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
          val b_delete_flag = data.getString("b_delete_flag")
          val b_finish = data.getString("b_finish")
          val i_member_id = data.getString("i_member_id")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(i_member_id)) {
            val i_actual_balance = data.getLong("i_actual_balance")
            val member_balance_sql = new StringBuffer("update station_show_member_balance_table set")
              .append(" total_balance_count=total_balance_count-?")
              .append(" where l_member_id=? and l_station_id=?")
            prepareStatement01 = connect.prepareStatement(member_balance_sql.toString())
            prepareStatement01.setLong(2, i_member_id.toLong)
            prepareStatement01.setLong(3, l_station_id)
            prepareStatement01.setLong(1, i_actual_balance)
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
          val b_delete_flag = data.getString("b_delete_flag")
          val b_finish = data.getString("b_finish")
          val i_member_id = data.getString("i_member_id")
          val l_seller_id = data.getIntValue("l_seller_id")
          val day = data.getString("d_time_s")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(i_member_id)
            && !StringUtil.isBlank(day) && l_seller_id != 0) {
            val i_actual_balance = data.getLong("i_actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val s_member_flag = data.getString("s_member_flag")
            val m_sum_power = data.getLong("m_sum_power")
            val i_power_balance = data.getLong("i_power_balance")
            val month_member_sql = new StringBuffer("update station_show_month_member_table set")
              .append(" month_member_charge_count=month_member_charge_count-?,") //12
              .append(" month_team_power_count=month_team_power_count-?,")
              .append(" month_team_balance_count=month_team_balance_count-?,")
              .append(" month_team_power_balance_count=month_team_power_balance_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=?")

            prepareStatement01 = connect.prepareStatement(month_member_sql.toString())
            prepareStatement01.setLong(5, l_station_id)
            prepareStatement01.setInt(6, l_seller_id)
            prepareStatement01.setString(7, dayd)
            prepareStatement01.setLong(8, i_member_id.toLong)
            prepareStatement01.setLong(1, 1)
            prepareStatement01.setLong(3, i_actual_balance)
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement01.setLong(2, m_sum_power)
              prepareStatement01.setLong(4, i_power_balance)
            } else {
              prepareStatement01.setLong(2, 0)
              prepareStatement01.setLong(4, 0)
            }
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
          }
        } catch {
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
          val b_delete_flag = data.getString("b_delete_flag")
          val b_finish = data.getString("b_finish")
          val i_member_id = data.getString("i_member_id")
          val l_seller_id = data.getIntValue("l_seller_id")
          val v_vin = data.getString("v_vin")
          val day = data.getString("d_time_s")
            val s_member_flag = data.getString("s_member_flag")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(i_member_id)
            && !StringUtil.isBlank(day) && l_seller_id != 0 && !StringUtil.isBlank(v_vin) 
            && (StringUtil.isBlank(s_member_flag) || "2".equals(s_member_flag))) {
            val i_actual_balance = data.getLong("i_actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val month_member_sql = new StringBuffer("update station_show_month_team_balance_table set")
              .append(" month_team_actual_balance_count=month_team_actual_balance_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=? and v_vin=?")
            prepareStatement01 = connect.prepareStatement(month_member_sql.toString())
            prepareStatement01.setLong(1, i_actual_balance)
            prepareStatement01.setLong(2, l_station_id)
            prepareStatement01.setInt(3, l_seller_id)
            prepareStatement01.setString(4, dayd)
            prepareStatement01.setLong(5, i_member_id.toLong)
            prepareStatement01.setString(6, v_vin)
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
          val b_delete_flag = data.getString("b_delete_flag")
          val b_finish = data.getString("b_finish")
          val i_member_id = data.getLong("i_member_id")
          val l_seller_id = data.getIntValue("l_seller_id")
          val day = data.getString("d_time_s")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && !StringUtil.isBlank(day) && l_seller_id != 0) {
            val l_pile_id = data.getLong("l_pile_id")
            val i_actual_balance = data.getLong("i_actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val s_member_flag = data.getString("s_member_flag")
            val m_sum_power = data.getLong("m_sum_power")
            val i_power_balance = data.getLong("i_power_balance")
            val i_aiscount_balance = data.getLong("i_aiscount_balance")
            val b_feesingle = data.getLong("b_feesingle")
            val i_deductible_balance = data.getLong("i_deductible_balance")
            val b_fee_service = data.getLong("b_fee_service")
            val d_time_s = data.getString("d_time_s")
            val currentT = TimeConUtil.getMonth(day, "1")
            val newT = TimeConUtil.getMonth(day, "0")
            val dateHour = TimeConUtil.getHours(d_time_s)
            var pile_id = 0L
            var isnewC = 0
            var isgrouth = 0
            var flag = true
            val query_newC = "select distinct id,l_pile_id from t_origin_history_operation_bill where i_member_id=? and timediff(d_time_s,?)>0 and timediff(d_time_s,?)<0 and l_seller_id=? and b_finish=1 and b_delete_flag=0"
            prepareStatement03 = connect.prepareStatement(query_newC)
            prepareStatement03.setLong(1, i_member_id)
            prepareStatement03.setString(2, currentT)
            prepareStatement03.setString(3, newT)
            prepareStatement03.setLong(4, l_seller_id)
            resultSet01 = prepareStatement03.executeQuery()
            while (flag) {
              if (!resultSet01.next()) {
                isnewC = 1
                flag = false
              } else {
                pile_id = resultSet01.getLong("l_pile_id")
                if (l_pile_id_arr.contains(pile_id)) {
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
            val query_grouthC = "select distinct id,l_pile_id from t_origin_history_operation_bill where i_member_id=? and timediff(d_time_s,?)<0 and l_seller_id=? and b_finish=1 and b_delete_flag=0"
            prepareStatement02 = connect.prepareStatement(query_grouthC)
            prepareStatement02.setLong(1, i_member_id)
            prepareStatement02.setString(2, d_time_s)
            prepareStatement02.setLong(3, l_seller_id)
            resultSet02 = prepareStatement02.executeQuery()
            while (flag) {
              if (!resultSet02.next()) {
                isgrouth = 1
                flag = false
              } else {
                pile_id = resultSet02.getLong("l_pile_id")
                if (l_pile_id_arr.contains(pile_id)) {
                  isgrouth = 0
                  flag = false
                } else {
                  isgrouth = 1
                }
              }
            }
            prepareStatement02.close()
            resultSet02.close()
            val month_total_sql = new StringBuffer("update station_show_month_table set")
              .append(" month_i_power_count=month_i_power_count-?,") //36
              .append(" month_i_power_balance_count=month_i_power_balance_count-?,")
              .append(" month_pile_num_count=month_pile_num_count-?,")
              .append(" month_pile_personal_num_count=month_pile_personal_num_count-?,")
              .append(" month_charge_personal_num_count=month_charge_personal_num_count-?,") //40
              .append(" month_personal_visit_num=month_personal_visit_num-?,")
              .append(" month_grouth_personal_num=month_grouth_personal_num-?,")
              .append(" month_hour_zero_charge_count=month_hour_zero_charge_count-?,")
              .append(" month_hour_one_charge_count=month_hour_one_charge_count-?,")
              .append(" month_hour_two_charge_count=month_hour_two_charge_count-?,") //45
              .append(" month_hour_three_charge_count=month_hour_three_charge_count-?,")
              .append(" month_hour_four_charge_count=month_hour_four_charge_count-?,")
              .append(" month_hour_five_charge_count=month_hour_five_charge_count-?,")
              .append(" month_hour_six_charge_count=month_hour_six_charge_count-?,")
              .append(" month_hour_seven_charge_count=month_hour_seven_charge_count-?,") //50
              .append(" month_hour_eight_charge_count=month_hour_eight_charge_count-?,")
              .append(" month_hour_nine_charge_count=month_hour_nine_charge_count-?,")
              .append(" month_hour_ten_charge_count=month_hour_ten_charge_count-?,")
              .append(" month_hour_eleven_charge_count=month_hour_eleven_charge_count-?,")
              .append(" month_hour_twelve_charge_count=month_hour_twelve_charge_count-?,") //55
              .append(" month_hour_thirteen_charge_count=month_hour_thirteen_charge_count-?,")
              .append(" month_hour_fourteen_charge_count=month_hour_fourteen_charge_count-?,")
              .append(" month_hour_fifteen_charge_count=month_hour_fifteen_charge_count-?,")
              .append(" month_hour_sixteen_charge_count=month_hour_sixteen_charge_count-?,")
              .append(" month_hour_seventeen_charge_count=month_hour_seventeen_charge_count-?,") //60
              .append(" month_hour_eighteen_charge_count=month_hour_eighteen_charge_count-?,")
              .append(" month_hour_nineteen_charge_count=month_hour_nineteen_charge_count-?,")
              .append(" month_hour_twenty_charge_count=month_hour_twenty_charge_count-?,")
              .append(" month_hour_twenty_one_charge_count=month_hour_twenty_one_charge_count-?,")
              .append(" month_hour_twenty_two_charge_count=month_hour_twenty_two_charge_count-?,") //65
              .append(" month_hour_twenty_three_charge_count=month_hour_twenty_three_charge_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=?")
            prepareStatement01 = connect.prepareStatement(month_total_sql.toString())
            prepareStatement01.setLong(32, l_station_id)
            prepareStatement01.setInt(33, l_seller_id)
            prepareStatement01.setString(34, dayd)
            prepareStatement01.setLong(1, m_sum_power)
            prepareStatement01.setLong(2, i_actual_balance)
            prepareStatement01.setLong(3, 1)
            //是否参与活动
            if (i_aiscount_balance > 0 || b_feesingle > 0 || i_deductible_balance > 0
              || b_fee_service > 0) {
              prepareStatement01.setLong(6, 1)
            } else {
              prepareStatement01.setLong(6, 0)
            }
            //是否个人会员
            if ("1".equals(s_member_flag)) {
              prepareStatement01.setLong(4, 1)
              prepareStatement01.setLong(5, isnewC)
              prepareStatement01.setLong(7, isgrouth)
            } else {
              prepareStatement01.setLong(4, 0)
              prepareStatement01.setLong(5, 0)
              prepareStatement01.setLong(7, 0)
            }
            //小时分类统计
            for (j <- 8 to 31) {
              if (j == 8 + dateHour) {
                prepareStatement01.setLong(j, 1)
              } else {
                prepareStatement01.setLong(j, 0)
              }
            }
            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()

          }
        } catch {
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
          val b_delete_flag = data.getString("b_delete_flag")
          val b_finish = data.getString("b_finish")
          val i_member_id = data.getLong("i_member_id")
          val l_seller_id = data.getIntValue("l_seller_id")
          val day = data.getString("d_time_s")
          if ("0".equals(b_delete_flag) && "1".equals(b_finish) && i_member_id != 0 && l_seller_id != 0) {
            val l_pile_id = data.getLong("l_pile_id")
            val i_actual_balance = data.getLong("i_actual_balance")
            val dayd = TimeConUtil.timeConversion(day, "%Y-%m")
            val s_member_flag = data.getString("s_member_flag")
            val m_sum_power = data.getLong("m_sum_power")
            val i_power_balance = data.getLong("i_power_balance")
            val d_time_s = data.getString("d_time_s")
            val d_time_e = data.getString("d_time_e")
            val diffT = TimeConUtil.timeDIFF(d_time_s, d_time_e)
            val v_vin = data.getString("v_vin")
            var isnewC = 0
            var isnewcar = 0
            var pile_id = 0L
            var flag = true
            val query_newC = "select distinct id,l_pile_id from t_origin_history_operation_bill where i_member_id=? and b_finish=1 and b_delete_flag=0 and l_seller_id=?"
            prepareStatement03 = connect.prepareStatement(query_newC)
            prepareStatement03.setLong(1, i_member_id)
            prepareStatement03.setInt(2, l_seller_id)
            resultSet01 = prepareStatement03.executeQuery()
            while (flag) {
              if (!resultSet01.next()) {
                isnewC = 1
                flag = false
              } else {
                pile_id = resultSet01.getLong("l_pile_id")
                if (l_pile_id_arr.contains(pile_id)) {
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
              val query_newCar = "select distinct v_vin,l_pile_id from t_origin_history_operation_bill where v_vin=? and b_finish=1 and b_delete_flag=0 and l_seller_id=?"
              prepareStatement02 = connect.prepareStatement(query_newCar)
              prepareStatement02.setString(1, v_vin)
              prepareStatement02.setInt(2, l_seller_id)
              resultSet02 = prepareStatement02.executeQuery()
              while (flag) {
                if (!resultSet02.next()) {
                  isnewcar = 1
                  flag = false
                } else {
                  pile_id = resultSet02.getLong("l_pile_id")
                  if (l_pile_id_arr.contains(pile_id)) {
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
            val station_show_total_sql = new StringBuffer("update station_show_total_table set")
              .append(" total_station_service_count=total_station_service_count-?,") //26
              .append(" total_station_service_use_count=total_station_service_use_count-?,")
              .append(" total_station_service_team_count=total_station_service_team_count-?,")
              .append(" total_station_team_power_count=total_station_team_power_count-?,")
              .append(" total_station_team_bill_count=total_station_team_bill_count-?,") //30
              .append(" total_station_team_menoy_count=total_station_team_menoy_count-?,")
              .append(" total_station_service_team_cars=total_station_service_team_cars-?,")
              .append(" total_station_service_team_time=total_station_service_team_time-?,")
              .append(" total_station_service_silver_use_count=total_station_service_silver_use_count-?,")
              .append(" total_station_service_gold_use_count=total_station_service_gold_use_count-?,") //35
              .append(" total_station_service_platinum_use_count=total_station_service_platinum_use_count-?,")
              .append(" total_station_service_diamond_use_count=total_station_service_diamond_use_count-?,")
              .append(" total_station_service_blackgold_use_count=total_station_service_blackgold_use_count-?,")
              .append(" total_station_power_count=total_station_power_count-?,")
              .append(" total_station_money_count=total_station_money_count-?") //40
              .append(" where l_station_id=? and l_seller_id=?")
            prepareStatement01 = connect.prepareStatement(station_show_total_sql.toString())
            prepareStatement01.setLong(16, l_station_id)
            prepareStatement01.setInt(17, l_seller_id)
            prepareStatement01.setLong(1, 1)
            prepareStatement01.setLong(14, m_sum_power)
            prepareStatement01.setLong(15, i_actual_balance)

            //个人车队分类
            if ("1".equals(s_member_flag)) {
              prepareStatement01.setLong(2, 1)

              prepareStatement01.setLong(3, 0)
              prepareStatement01.setLong(4, 0)
              prepareStatement01.setLong(5, 0)
              prepareStatement01.setLong(6, 0)
              prepareStatement01.setLong(7, 0)
              prepareStatement01.setLong(8, 0)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement01.setLong(3, isnewC)
              prepareStatement01.setLong(4, m_sum_power)
              prepareStatement01.setLong(5, 1)
              prepareStatement01.setLong(6, i_actual_balance)
              prepareStatement01.setLong(7, isnewcar)
              prepareStatement01.setLong(8, diffT)

              prepareStatement01.setLong(2, 0)
            } else {
              prepareStatement01.setLong(3, 0)
              prepareStatement01.setLong(4, 0)
              prepareStatement01.setLong(5, 0)
              prepareStatement01.setLong(6, 0)
              prepareStatement01.setLong(7, 0)
              prepareStatement01.setLong(8, 0)

              prepareStatement01.setLong(2, 0)
            }
            //会员等级统计
            if (isnewC == 1 && "1".equals(level)) {
              prepareStatement01.setLong(9, 1)

              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(13, 0)
            } else if (isnewC == 1 && "2".equals(level)) {
              prepareStatement01.setLong(9, 0)

              prepareStatement01.setLong(10, 1)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(13, 0)
            } else if (isnewC == 1 && "3".equals(level)) {
              prepareStatement01.setLong(9, 0)

              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(11, 1)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(13, 0)
            } else if (isnewC == 1 && "4".equals(level)) {
              prepareStatement01.setLong(9, 0)

              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(12, 1)
              prepareStatement01.setLong(13, 0)
            } else if (isnewC == 1 && "5".equals(level)) {
              prepareStatement01.setLong(9, 0)

              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(13, 1)
            } else {
              prepareStatement01.setLong(9, 0)

              prepareStatement01.setLong(10, 0)
              prepareStatement01.setLong(11, 0)
              prepareStatement01.setLong(12, 0)
              prepareStatement01.setLong(13, 0)
            }

            prepareStatement01.execute()
            prepareStatement01.close()
            connect.close()
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