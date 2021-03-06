package screen_analyze.service.source_bill

import com.alibaba.fastjson.JSONObject
import screen_analyze.util.{BillDruidUtil, ConfigDruidUtil, DruidUtil, MemberDruidUtil, StringUtil, TimeConUtil}
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.ResultSet

import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
/**
 * 监听t_bill表Update操作
 */
class OperationBillAnalyzeUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var level = ""
  var prepareStatement: PreparedStatement = _
  var prepareStatement01: PreparedStatement = _
  var connect: Connection = _
  var connect1: Connection = _
  var connect2: Connection = _

  var resultSet: ResultSet = _
  //分析更新数据
  def analyzeUpdateTable(jsonSql: JSONObject) {
//    log.info("********OperationBillAnalyzeUpdate处理json:" + jsonSql.toJSONString())
    val dataArr = jsonSql.getJSONArray("data")
    val oldArr = jsonSql.getJSONArray("old")
    var data = new JSONObject
    var old = new JSONObject
    var flag = true
    var arrSize = dataArr.size()
    for (x <- 0 to arrSize-1) {
      data = dataArr.getJSONObject(x)
      old = oldArr.getJSONObject(x)

      //新数据
      var l_station_id = 0L
      var l_pile_id_arr: scala.collection.mutable.ArrayBuffer[Long] = ArrayBuffer()
      val b_finish = data.getString("finish")
      val s_member_flag = data.getString("member_type")
      val i_member_id = data.getString("member_id")
      val d_time_s = data.getString("time_s")
      val l_seller_id = data.getIntValue("seller_id")
      val d_time_e = data.getString("time_e")
      val m_consume_balance = data.getLongValue("consume_balance")
      val i_power_balance = data.getLongValue("power_balance")
      val b_delete_flag = data.getString("deleted")
      val i_actual_balance = data.getLongValue("actual_balance")
      val i_bus_type = data.getString("bus_type")
      val b_feesingle = data.getString("fee_single")
      val b_fee_service = data.getLongValue("fee_service")
      val l_leaguer_id = data.getString("leaguer_id")
      val i_aiscount_balance = data.getLongValue("discount_balance")
//      val m_appoint_balance = data.getLongValue("m_appoint_balance")  //删除
//      val m_parking_balance = data.getLongValue("m_parking_balance")  //删除
      val i_deductible_balance = data.getLongValue("deductible_balance")
      val m_sum_power = data.getLongValue("sum_power")
      val v_vin = data.getString("vin")
      val l_pile_id = data.getString("pile_id")
      var is_newCus_day_o_z = 0  //日站点用户新增
      var is_newCus_day_z_o = 0   //日站点用户新增
      var is_newgrouthC_month_o_z = 0  //月站点新用户增长
      var is_newgrouthC_month_z_o = 0 //月站点新用户增长
      var is_newCus_month_o_z = 0 //日站点用户新增
      var is_newCus_month_z_o = 0 //日站点用户新增
      var is_newCar_day_o_z = 0  //日站点车辆新增
      var is_newCar_day_z_o = 0 //日站点车辆新增
      var is_newCus_total_o_z = 0 //站点用户新增
      var is_newCus_total_z_o = 0 //站点用户新增
      var is_newCar_total_o_z = 0 //站点车辆新增
      var is_newCar_total_z_o = 0 //站点车辆新增

      var diffT = 0L
      var isnewC_id = 0
      var isnewC_id_row = -1
      var m_service_balance = data.getLongValue("service_balance")
      if ("1".equals(b_feesingle) || "1".equals(b_fee_service)) {
        m_service_balance = 0
      } else {
        m_service_balance = m_service_balance - i_aiscount_balance
      }
      var carNo = ""
      var carType = "isdelete"
      var dayd = ""
      var dayd_month = ""
      var isnewC_row = -1
      var dateHour = 60
      if (!StringUtil.isBlank(d_time_s)) {
        dayd = TimeConUtil.timeConversion(d_time_s, "%Y-%m-%d")
        dayd_month = TimeConUtil.timeConversion(d_time_s, "%Y-%m")
        dateHour = TimeConUtil.getHours(d_time_s)
      }
      if (!StringUtil.isBlank(d_time_s) && !StringUtil.isBlank(d_time_e)) {
        diffT = TimeConUtil.timeDIFF(d_time_s, d_time_e)
      }

      //旧数据
      val old_b_finish = old.getString("finish")
      val old_d_time_s = old.getString("time_s")
      val old_d_time_e = old.getString("time_e")
      val old_m_consume_balance = old.getLongValue("consume_balance")
      val old_m_consume = old.getString("consume_balance")
      val old_i_power_balance = old.getLongValue("power_balance")
      val old_i_power = old.getString("power_balance")
      val old_b_delete_flag = old.getString("deleted")
      val old_i_actual_balance = old.getLongValue("actual_balance")
      val old_i_bus_type = old.getString("bus_type")
      val old_b_feesingle = old.getString("fee_single")
      val old_v_vin = old.getString("vin")
      val old_i_power_b = old.getString("power_balance")
      val old_m_consume_b = old.getString("consume_balance")
      val old_i_actual_b = old.getString("actual_balance")
      val old_b_fee_service = old.getLongValue("fee_service")
      val old_b_fee_s = old.getString("fee_service")
//      val old_m_appoint_balance = old.getLongValue("m_appoint_balance")    //删除
//      val old_m_parking_balance = old.getLongValue("m_parking_balance")    //删除
      val old_i_aiscount_balance = old.getLongValue("discount_balance")
      val old_i_deductible_b = old.getString("deductible_balance")
      val old_i_deductible_balance = old.getLongValue("deductible_balance")
      val old_i_aiscount_b = old.getString("discount_balance")
//      val old_m_appoint_b = old.getString("m_appoint_balance")
//      val old_m_parking_b = old.getString("m_parking_balance")
      val old_m_service_b = old.getString("service_balance")
      val old_m_sum_p = old.getString("sum_power")
      var is_oldCus_day_o_z = 0
      var is_oldCus_day_z_o = 0
      var is_oldgrouthC_month_o_z = 0
      var is_oldgrouthC_month_z_o = 0
      var is_oldCus_month_o_z = 0
      var is_oldCus_month_z_o = 0
      var is_oldCar_day_o_z = 0
      var is_oldCar_day_z_o = 0
      var is_oldCar_total_o_z = 0
      var is_oldCar_total_z_o = 0

      var time_s = d_time_s
      var time_e = d_time_e
      var i_aiscount_b = i_aiscount_balance
      if (old_d_time_s != null) { time_s = old_d_time_s }
      if (old_d_time_e != null) { time_e = old_d_time_e }
      if (old_i_aiscount_b != null) { i_aiscount_b = old_i_aiscount_balance }
      val old_m_sum_power = old.getLongValue("sum_power")
      var old_m_service_balance = data.getLongValue("service_balance")
      if (old_m_service_b != null) { old_m_service_balance = old.getLongValue("service_balance") }
      var old_diffT = diffT
      var old_dateHour = 60
      if ("1".equals(old_b_feesingle) || "1".equals(old_b_fee_service)) {
        old_m_service_balance = 0
      } else {
        old_m_service_balance = old_m_service_balance - i_aiscount_b
      }
      var old_carType = "isdelete"
      var old_dayd = ""
      var old_dayd_month = ""
      var is_old_newC_row = -1
      if (old_d_time_s != null) {
        old_dayd = TimeConUtil.timeConversion(old_d_time_s, "%Y-%m-%d")
        old_dayd_month = TimeConUtil.timeConversion(old_d_time_s, "%Y-%m")
        old_dateHour = TimeConUtil.getHours(old_d_time_s)
      }
      if ((old_d_time_s != null || old_d_time_e != null) && !StringUtil.isBlank(time_s) && !StringUtil.isBlank(time_e)) {
        old_diffT = TimeConUtil.timeDIFF(time_s, time_e)
      }

      //查询数据
      try {
        connect = MemberDruidUtil.getConnection.get
        //根据新vin查询车类型
        if (!StringUtil.isBlank(v_vin)) {
          val queryCarSql = "select carrier_type,plate_no from t_vehicle where vin=? and deleted=0"
          prepareStatement = connect.prepareStatement(queryCarSql)
          prepareStatement.setString(1, v_vin)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            carType = resultSet.getString("carrier_type")
            carNo = resultSet.getString("plate_no")
          }
          prepareStatement.close()
          resultSet.close()

        }
        //根据旧vin查询车类型
        if (old_v_vin != null && !StringUtil.isBlank(old_v_vin)) {
          val queryCarSql = "select carrier_type from t_vehicle where vin=? and deleted=0"
          prepareStatement = connect.prepareStatement(queryCarSql)
          prepareStatement.setString(1, old_v_vin)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            old_carType = resultSet.getString("carrier_type")
          }
          prepareStatement.close()
          resultSet.close()
        }
        //根据新桩号查询站点信息
        connect1 = ConfigDruidUtil.getConnection.get
        if (!StringUtil.isBlank(l_pile_id)) {
          val query_stid = "select station_id from t_pile where id=?"
          prepareStatement = connect1.prepareStatement(query_stid)
          prepareStatement.setLong(1, l_pile_id.toLong)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            l_station_id = resultSet.getLong("station_id")
          }
          prepareStatement.close()
          resultSet.close()
        }
        //根据新站点查询桩集合信息
        if (l_station_id != 0) {
          val query_l_pile_arr = "select id from t_pile where station_id=?"
          prepareStatement = connect1.prepareStatement(query_l_pile_arr)
          prepareStatement.setLong(1, l_station_id)
          resultSet = prepareStatement.executeQuery()
          while (resultSet.next()) {
            l_pile_id_arr += resultSet.getLong("id")
          }
          prepareStatement.close()
          resultSet.close()
        }
        //根据新日期查询是否为新用户

        if (!StringUtil.isBlank(d_time_s)) {
          connect2 = BillDruidUtil.getConnection.get
          val curT = TimeConUtil.getTime(d_time_s, "1")
          val nextT = TimeConUtil.getTime(d_time_s, "0")
          val querynewC = "select member_id,pile_id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, curT)
          prepareStatement.setString(3, nextT)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst
          var x = 0
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_newCus_day_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_newCus_day_z_o = 0
                flag = false
              } else {
                is_newCus_day_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_newCus_day_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_newCus_day_o_z = 0
                flag = false
              } else {
                is_newCus_day_o_z = 1
              }
            }
          }
          resultSet.last()
          isnewC_row = resultSet.getRow
          prepareStatement.close()
          resultSet.close()
        }
        //根据旧日期查询是否为新用户（日期有变动时）

        if (old_d_time_s != null) {
          connect2 = BillDruidUtil.getConnection.get
          val curT = TimeConUtil.getTime(old_d_time_s, "1")
          val nextT = TimeConUtil.getTime(old_d_time_s, "0")
          val querynewC = "select member_id,pile_id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, curT)
          prepareStatement.setString(3, nextT)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_oldCus_day_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_oldCus_day_z_o = 0
                flag = false
              } else {
                is_oldCus_day_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_oldCus_day_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_oldCus_day_o_z = 0
                flag = false
              } else {
                is_oldCus_day_o_z = 1
              }
            }
          }
          resultSet.last()
          is_old_newC_row = resultSet.getRow
          prepareStatement.close()
          resultSet.close()
        } else {
          is_old_newC_row = isnewC_row
          is_oldCus_day_o_z = is_newCus_day_o_z
          is_oldCus_day_z_o = is_newCus_day_z_o
        }
        //根据新月份查询是否为增长用户
        if (d_time_s != null) {
          val query_grouthC = "select id,pile_id from t_bill where member_id=? and timediff(time_s,?)<=0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(query_grouthC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, d_time_s)
          prepareStatement.setLong(3, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_newgrouthC_month_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_newgrouthC_month_z_o = 0
                flag = false
              } else {
                is_newgrouthC_month_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_newgrouthC_month_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_newgrouthC_month_o_z = 0
                flag = false
              } else {
                is_newgrouthC_month_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        }
        //根据旧月份查询是否为增长用户（日期有变动时）
        if (old_d_time_s != null) {
          val query_grouthC = "select id,pile_id from t_bill where member_id=? and timediff(time_s,?)<=0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(query_grouthC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, old_d_time_s)
          prepareStatement.setLong(3, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_oldgrouthC_month_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_oldgrouthC_month_z_o = 0
                flag = false
              } else {
                is_oldgrouthC_month_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_oldgrouthC_month_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }

              if (x > 0) {
                is_oldgrouthC_month_o_z = 0
                flag = false
              } else {
                is_oldgrouthC_month_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        } else {
          is_oldgrouthC_month_o_z = is_newgrouthC_month_o_z
          is_oldgrouthC_month_z_o = is_newgrouthC_month_z_o
        }
        //根据新月份查询是否为新用户
        if (!StringUtil.isBlank(d_time_s)) {
          val curT = TimeConUtil.getMonth(d_time_s, "1")
          val nextT = TimeConUtil.getMonth(d_time_s, "0")
          val querynewC = "select  member_id,pile_id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, curT)
          prepareStatement.setString(3, nextT)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_newCus_month_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_newCus_month_z_o = 0
                flag = false
              } else {
                is_newCus_month_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_newCus_month_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_newCus_month_o_z = 0
                flag = false
              } else {
                is_newCus_month_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        }
        //根据旧月份查询是否为新用户（日期有变动时）
        if (old_d_time_s != null && !dayd_month.equals(old_dayd_month)) {
          val curT = TimeConUtil.getMonth(old_d_time_s, "1")
          val nextT = TimeConUtil.getMonth(old_d_time_s, "0")
          val querynewC = "select member_id,pile_id from t_bill where member_id=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setString(2, curT)
          prepareStatement.setString(3, nextT)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_oldCus_month_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_oldCus_month_z_o = 0
                flag = false
              } else {
                is_oldCus_month_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_oldCus_month_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_oldCus_month_o_z = 0
                flag = false
              } else {
                is_oldCus_month_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        } else {
          is_oldCus_month_o_z = is_newCus_month_o_z
          is_oldCus_month_z_o = is_newCus_month_z_o
        }

        //根据新日期、桩号查询是否为新车辆
        if (!StringUtil.isBlank(d_time_s)) {
          val currentTime = TimeConUtil.getTime(d_time_s, "1")
          val nextTime = TimeConUtil.getTime(d_time_s, "0")
          val query_isnewcar = "select vin,pile_id from t_bill where vin=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=?  and finish=1 and deleted=0 "
          prepareStatement = connect2.prepareStatement(query_isnewcar)
          prepareStatement.setString(1, v_vin)
          prepareStatement.setString(2, currentTime)
          prepareStatement.setString(3, nextTime)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_newCar_day_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_newCar_day_z_o = 0
                flag = false
              } else {
                is_newCar_day_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_newCar_day_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_newCar_day_o_z = 0
                flag = false
              } else {
                is_newCar_day_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        }

        //根据旧日期、桩号、vin查询是否为新车辆（日期、桩号、vin有变动时）
        if (old_d_time_s != null || old_v_vin != null) {
          var is_old_v_vin = v_vin
          if (old_v_vin != null) { is_old_v_vin = old_v_vin }
          var time_s = d_time_s
          if (old_d_time_s != null) { time_s = old_d_time_s }
          val currentTime = TimeConUtil.getTime(time_s, "1")
          val nextTime = TimeConUtil.getTime(time_s, "0")
          val query_isnewcar = "select vin,pile_id from t_bill where vin=? and timediff(time_s,?)>0 and timediff(time_s,?)<0 and seller_id=? AND finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(query_isnewcar)
          prepareStatement.setString(1, is_old_v_vin)
          prepareStatement.setString(2, currentTime)
          prepareStatement.setString(3, nextTime)
          prepareStatement.setInt(4, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_oldCar_day_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_oldCar_day_z_o = 0
                flag = false
              } else {
                is_oldCar_day_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_oldCar_day_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_oldCar_day_o_z = 0
                flag = false
              } else {
                is_oldCar_day_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        } else {
          is_oldCar_day_o_z = is_newCar_day_o_z
          is_oldCar_day_z_o = is_newCar_day_z_o
        }

        //根据会员id、子成员id查询是否为新用户
        if (!StringUtil.isBlank(i_member_id)) {
          val query_sql = "select id from t_bill where member_id=? and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(query_sql)
          prepareStatement.setLong(1, i_member_id.toLong)
          prepareStatement.setInt(2, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          resultSet.last()
          if (resultSet.getRow == 1) {
            isnewC_id = 1
          }
          isnewC_id_row = resultSet.getRow
          prepareStatement.close()
          resultSet.close()
        } else if (!StringUtil.isBlank(l_leaguer_id)) {
          val query_sql = "select id from t_bill where leaguer_id=? and seller_id=? and finish=1 and deleted=0 "
          prepareStatement = connect2.prepareStatement(query_sql)
          prepareStatement.setLong(1, l_leaguer_id.toLong)
          prepareStatement.setInt(2, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          resultSet.last()
          if (resultSet.getRow == 1) {
            isnewC_id = 1
          }
          isnewC_id_row = resultSet.getRow
          prepareStatement.close()
          resultSet.close()
        }
        //根据memberid、桩号查询是否为新用户
        if (!StringUtil.isBlank(i_member_id)) {
          val querynewC = "select member_id,pile_id from t_bill where member_id=? and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(querynewC)
          prepareStatement.setInt(1, i_member_id.toInt)
          prepareStatement.setInt(2, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_newCus_total_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_newCus_total_z_o = 0
                flag = false
              } else {
                is_newCus_total_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_newCus_total_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_newCus_total_o_z = 0
                flag = false
              } else {
                is_newCus_total_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        }

        //根据新vin查询是否为新车辆
        if (v_vin != null) {
          val query_isnewcar = "select vin,pile_id from t_bill where vin=? and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(query_isnewcar)
          prepareStatement.setString(1, v_vin)
          prepareStatement.setInt(2, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_newCar_total_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_newCar_total_z_o = 0
                flag = false
              } else {
                is_newCar_total_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_newCar_total_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_newCar_total_o_z = 0
                flag = false
              } else {
                is_newCar_total_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        }
        //根据旧vin查询是否为新车辆
        if (old_v_vin != null) {
          flag = true
          val query_isnewcar = "select vin,pile_id from t_bill where vin=? and seller_id=? and finish=1 and deleted=0"
          prepareStatement = connect2.prepareStatement(query_isnewcar)
          prepareStatement.setString(1, old_v_vin)
          prepareStatement.setInt(2, l_seller_id)
          resultSet = prepareStatement.executeQuery()
          var x = 0
          resultSet.last()
          flag = true
          val row = resultSet.getRow()
          resultSet.beforeFirst()
          while (flag && resultSet.next()) {
            //z_o
            if (row == 1) {
              is_oldCar_total_z_o = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 1) {
                is_oldCar_total_z_o = 0
                flag = false
              } else {
                is_oldCar_total_z_o = 1
              }
            }

          }
          resultSet.beforeFirst
          x = 0
          flag = true
          while (flag) {
            //o_z
            if (!resultSet.next()) {
              is_oldCar_total_o_z = 1
              flag = false
            } else {
              var pile_id = resultSet.getLong("pile_id")
              l_pile_id_arr.foreach { f => if (f == pile_id) x = x + 1 }
              if (x > 0) {
                is_oldCar_total_o_z = 0
                flag = false
              } else {
                is_oldCar_total_o_z = 1
              }
            }
          }
          prepareStatement.close()
          resultSet.close()
        } else {
          is_oldCar_total_z_o = is_newCar_total_z_o
          is_oldCar_total_o_z = is_newCar_total_o_z
        }

        //查询会员等级
        if (!StringUtil.isBlank(i_member_id)) {
          val query_level_sql = "select level from t_member where id=?"
          prepareStatement = connect.prepareStatement(query_level_sql)
          prepareStatement.setLong(1, i_member_id.toLong)
          resultSet = prepareStatement.executeQuery()
          if (resultSet.next()) {
            level = resultSet.getString("level")
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
          if (connect1 != null && !connect1.isClosed()) {
            connect1.close()
          }
        } catch {
          case ex: Exception => log.info("***查询数据**关闭connect失败****")
        }
        try {
          if (connect2 != null && !connect2.isClosed()) {
            connect2.close()
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

      //更新t_all_day_member_table表
      try {
        connect = DruidUtil.getConnection.get
        if (!StringUtil.isBlank(i_member_id) && ("1".equals(s_member_flag) || !"GongJiao".equals(carType) ||
          StringUtil.isBlank(carType) || "isdelete".equals(carType))) {
          var actual_balance = i_actual_balance
          var consume_balance = m_consume_balance
          var isold_dayd = dayd
          if (old_i_actual_b != null) { actual_balance = old_i_power_balance }
          if (old_m_consume_b != null) { consume_balance = old_m_consume_balance }
          if (old_d_time_s != null) { isold_dayd = old_dayd }
          val diff_power_balance = i_actual_balance - actual_balance
          val diff_consume_balance = m_consume_balance - consume_balance

          //修改b_delete_flag操作1到0
          if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
            if ("1".equals(b_finish)) {
              val update_member_table = new StringBuffer("insert into t_all_day_member_table values(?,?,?,?,1,?,?,?)")
                .append(" ON DUPLICATE KEY UPDATE")
                .append(" day_personal_charge_count=day_personal_charge_count+1,")
                .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count+?,")
                .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count+?,")
                .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank+?")
              prepareStatement = connect.prepareStatement(update_member_table.toString())
              prepareStatement.setInt(1, l_seller_id)
              prepareStatement.setString(2, dayd)
              prepareStatement.setLong(3, i_member_id.toLong)
              prepareStatement.setString(4, "")
              prepareStatement.setLong(5, m_consume_balance)
              prepareStatement.setLong(6, i_actual_balance)
              prepareStatement.setLong(7, i_actual_balance)
              prepareStatement.setLong(8, m_consume_balance)
              prepareStatement.setLong(9, i_actual_balance)
              prepareStatement.setLong(10, i_actual_balance)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //修改b_delete_flag操作0到1
          else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
            if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
              val update_member_table = new StringBuffer("update t_all_day_member_table set ")
                .append(" day_personal_charge_count=day_personal_charge_count-1,")
                .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count-?,")
                .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count-?,")
                .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank-?")
                .append(" where l_seller_id=? and d_day_time=? and l_member_id=?")
              prepareStatement = connect.prepareStatement(update_member_table.toString())
              prepareStatement.setLong(1, consume_balance)
              prepareStatement.setLong(2, actual_balance)
              prepareStatement.setLong(3, actual_balance)
              prepareStatement.setInt(4, l_seller_id)
              prepareStatement.setString(5, isold_dayd)
              prepareStatement.setLong(6, i_member_id.toLong)
              prepareStatement.execute()
              prepareStatement.close()
            }

          } //修改b_finish操作0到1
          else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
            if ("0".equals(b_delete_flag)) {
              val update_member_table = new StringBuffer("insert into t_all_day_member_table values(?,?,?,?,1,?,?,?)")
                .append(" ON DUPLICATE KEY UPDATE")
                .append(" day_personal_charge_count=day_personal_charge_count+1,")
                .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count+?,")
                .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count+?,")
                .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank+?")
              prepareStatement = connect.prepareStatement(update_member_table.toString())
              prepareStatement.setInt(1, l_seller_id)
              prepareStatement.setString(2, dayd)
              prepareStatement.setLong(3, i_member_id.toLong)
              prepareStatement.setString(4, "")
              prepareStatement.setLong(5, m_consume_balance)
              prepareStatement.setLong(6, i_actual_balance)
              prepareStatement.setLong(7, i_actual_balance)
              prepareStatement.setLong(8, m_consume_balance)
              prepareStatement.setLong(9, i_actual_balance)
              prepareStatement.setLong(10, i_actual_balance)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //修改b_finish操作1到0
          else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
            if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
              val update_member_table = new StringBuffer("update t_all_day_member_table set ")
                .append(" day_personal_charge_count=day_personal_charge_count-1,")
                .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count-?,")
                .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count-?,")
                .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank-?")
                .append(" where l_seller_id=? and d_day_time=? and l_member_id=?")
              prepareStatement = connect.prepareStatement(update_member_table.toString())
              prepareStatement.setLong(1, consume_balance)
              prepareStatement.setLong(2, actual_balance)
              prepareStatement.setLong(3, actual_balance)
              prepareStatement.setInt(4, l_seller_id)
              prepareStatement.setString(5, isold_dayd)
              prepareStatement.setLong(6, i_member_id.toLong)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //修改i_power_balance或m_consume_balance或d_time_s
          else if ("1".equals(b_finish) && "0".equals(b_delete_flag)
            && (old_i_power != null || old_m_consume != null || old_d_time_s != null)) {
            if (old_d_time_s == null || old_dayd.equals(dayd)) {
              val update_member_table = new StringBuffer("update t_all_day_member_table set ")
                .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count+?,")
                .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count+?,")
                .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank+?")
                .append(" where l_seller_id=? and d_day_time=? and l_member_id=?")
              prepareStatement = connect.prepareStatement(update_member_table.toString())
              prepareStatement.setLong(1, diff_power_balance)
              prepareStatement.setLong(2, diff_consume_balance)
              prepareStatement.setLong(3, diff_power_balance)
              prepareStatement.setInt(4, l_seller_id)
              prepareStatement.setString(5, dayd)
              prepareStatement.setLong(6, i_member_id.toLong)
              prepareStatement.execute()
              prepareStatement.close()
            } else {
              if (!old_dayd.equals(dayd)) {
                val update_member_table = new StringBuffer("update t_all_day_member_table set ")
                  .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count-?,")
                  .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count-?,")
                  .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank-?,")
                  .append(" day_personal_charge_count=day_personal_charge_count-1")
                  .append(" where l_seller_id=? and d_day_time=? and l_member_id=?")
                prepareStatement = connect.prepareStatement(update_member_table.toString())
                prepareStatement.setLong(2, consume_balance)
                prepareStatement.setLong(1, actual_balance)
                prepareStatement.setLong(3, actual_balance)
                prepareStatement.setInt(4, l_seller_id)
                prepareStatement.setString(5, old_dayd)
                prepareStatement.setLong(6, i_member_id.toLong)
                prepareStatement.execute()
                prepareStatement.close()
                //增加新日期数量
                val update_member_table_new = new StringBuffer("insert into t_all_day_member_table values(?,?,?,?,1,?,?,?)")
                  .append(" ON DUPLICATE KEY UPDATE")
                  .append(" day_personal_charge_count=day_personal_charge_count+1,")
                  .append(" day_personal_m_consume_balance_count=day_personal_m_consume_balance_count+?,")
                  .append(" day_personal_i_power_balance_count=day_personal_i_power_balance_count+?,")
                  .append(" day_nbus_i_actual_balance_count_rank=day_nbus_i_actual_balance_count_rank+?")
                prepareStatement = connect.prepareStatement(update_member_table_new.toString())
                prepareStatement.setInt(1, l_seller_id)
                prepareStatement.setString(2, dayd)
                prepareStatement.setLong(3, i_member_id.toLong)
                prepareStatement.setString(4, "")
                prepareStatement.setLong(5, m_consume_balance)
                prepareStatement.setLong(6, i_actual_balance)
                prepareStatement.setLong(7, i_actual_balance)
                prepareStatement.setLong(8, m_consume_balance)
                prepareStatement.setLong(9, i_actual_balance)
                prepareStatement.setLong(10, i_actual_balance)
                prepareStatement.execute()
                prepareStatement.close()
              }
            }
          }
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
        try {
          if (prepareStatement01 != null && !prepareStatement01.isClosed()) {
            prepareStatement01.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_member_table**关闭prepareStatement01失败****")
        }
      }
/**************************************************************************************************/
      //更新t_all_day_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
            var isnewC = 0
            if (isnewC_row == 1) {
              isnewC = 1
            }
            val day_total_sql = new StringBuffer("insert into t_all_day_table values(")
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
            prepareStatement = connect.prepareStatement(day_total_sql.toString())
            prepareStatement.setInt(1, l_seller_id)
            prepareStatement.setString(2, dayd)
            for (m <- 3 to 42) {
              prepareStatement.setLong(m, 0L)
            }
            //个人或车队更新
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(4, 1)
              prepareStatement.setLong(8, isnewC)
              prepareStatement.setLong(25, 1)
              prepareStatement.setLong(27, isnewC)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(3, 1)
              prepareStatement.setLong(7, isnewC)
              prepareStatement.setLong(24, 1)
              prepareStatement.setLong(26, isnewC)
            }
            //服务类型更新
            if ("2".equals(i_bus_type)) {
              prepareStatement.setLong(9, isnewC)
              prepareStatement.setLong(28, isnewC)
            } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
              prepareStatement.setLong(10, isnewC)
              prepareStatement.setLong(29, isnewC)
            }
            //总收入统计
            if ("0".equals(b_feesingle)) {
              prepareStatement.setLong(11, i_actual_balance)
              prepareStatement.setLong(12, i_power_balance)
              prepareStatement.setLong(30, i_actual_balance)
              prepareStatement.setLong(31, i_power_balance)
            }
            //车类型统计
            if (!"isdelete".equals(carType) && !"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(13, i_actual_balance)
              prepareStatement.setLong(32, i_actual_balance)
              prepareStatement.setLong(19, i_power_balance)
              prepareStatement.setLong(38, i_power_balance)
            }
            if (!"isdelete".equals(carType) && "WuLiu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(14, i_power_balance)
              prepareStatement.setLong(33, i_power_balance)
            } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(15, i_power_balance)
              prepareStatement.setLong(34, i_power_balance)
            } else if (!"isdelete".equals(carType) && "KeYun".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(16, i_power_balance)
              prepareStatement.setLong(35, i_power_balance)
            } else if (!"isdelete".equals(carType) && "WangYue".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(17, i_power_balance)
              prepareStatement.setLong(36, i_power_balance)
            } else if (!"isdelete".equals(carType) && "QiTa".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(18, i_power_balance)
              prepareStatement.setLong(37, i_power_balance)
            }
            //业务类型统计
            if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(20, i_actual_balance)
              prepareStatement.setLong(39, i_actual_balance)
            } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(21, i_actual_balance)
              prepareStatement.setLong(40, i_actual_balance)
            } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(22, i_actual_balance)
              prepareStatement.setLong(41, i_actual_balance)
            } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(23, i_actual_balance)
              prepareStatement.setLong(42, i_actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()

          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            var carT = carType
            var bus_type = i_bus_type.toString()
            var isoday = dayd
            var power_balance = i_power_balance
            var actual_balance = i_actual_balance
            var b_feesingle_n = b_feesingle
            if (old_v_vin != null) { carT = old_carType }
            if (old_d_time_s != null) { isoday = old_dayd }
            if (old_i_bus_type != null) { bus_type = old_i_bus_type.toString() }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_b_feesingle != null) { b_feesingle_n = old_b_feesingle }
            var isnewC = 0
            if (old_d_time_s != null) {
              if (is_old_newC_row == 0) {
                isnewC = 1
              }
            } else {
              if (isnewC_row == 0) {
                isnewC = 1
              }
            }
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
            prepareStatement.setString(21, isoday)
            for (m <- 1 to 19) {
              prepareStatement.setLong(m, 0L)
            }
            //个人或车队更新
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(2, 1)
              prepareStatement.setLong(4, isnewC)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(1, 1)
              prepareStatement.setLong(3, isnewC)

            }
            //服务类型更新
            if ("2".equals(bus_type)) {
              prepareStatement.setLong(5, isnewC)
            } else if ("4".equals(bus_type) || "5".equals(bus_type)) {
              prepareStatement.setLong(6, isnewC)
            }
            //总收入统计
            if ("0".equals(b_feesingle_n)) {
              prepareStatement.setLong(7, actual_balance)
              prepareStatement.setLong(8, power_balance)
            }
            //车类型统计
            if (!"isdelete".equals(carT) && !"GongJiao".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(9, actual_balance)
              prepareStatement.setLong(15, power_balance)
            }
            if (!"isdelete".equals(carT) && "WuLiu".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(10, power_balance)
            } else if (!"isdelete".equals(carT) && "ChuZu".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(11, power_balance)
            } else if (!"isdelete".equals(carT) && "KeYun".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(12, power_balance)
            } else if (!"isdelete".equals(carT) && "WangYue".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(13, power_balance)
            } else if (!"isdelete".equals(carT) && "QiTa".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(14, power_balance)
            }
            //业务类型统计
            if ("3".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(16, actual_balance)
            } else if ("1".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(17, actual_balance)
            } else if ("2".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(18, actual_balance)
            } else if (("4".equals(bus_type) || "5".equals(bus_type)) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(19, actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()

          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
            var isnewC = 0
            if (isnewC_row == 1) {
              isnewC = 1
            }
            val day_total_sql = new StringBuffer("insert into t_all_day_table values(")
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
            prepareStatement = connect.prepareStatement(day_total_sql.toString())
            prepareStatement.setInt(1, l_seller_id)
            prepareStatement.setString(2, dayd)
            for (m <- 3 to 42) {
              prepareStatement.setLong(m, 0L)
            }
            //个人或车队更新
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(4, 1)
              prepareStatement.setLong(8, isnewC)
              prepareStatement.setLong(25, 1)
              prepareStatement.setLong(27, 1)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(3, 1)
              prepareStatement.setLong(7, isnewC)
              prepareStatement.setLong(24, 1)
              prepareStatement.setLong(26, 1)
            }
            //服务类型更新
            if ("2".equals(i_bus_type)) {
              prepareStatement.setLong(9, isnewC)
              prepareStatement.setLong(28, isnewC)
            } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
              prepareStatement.setLong(10, isnewC)
              prepareStatement.setLong(29, isnewC)
            }
            //总收入统计
            if ("0".equals(b_feesingle)) {
              prepareStatement.setLong(11, i_actual_balance)
              prepareStatement.setLong(12, i_power_balance)
              prepareStatement.setLong(30, i_actual_balance)
              prepareStatement.setLong(31, i_power_balance)
            }
            //车类型统计
            if (!"isdelete".equals(carType) && !"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(13, i_actual_balance)
              prepareStatement.setLong(32, i_actual_balance)
              prepareStatement.setLong(19, i_power_balance)
              prepareStatement.setLong(38, i_power_balance)
            }
            if (!"isdelete".equals(carType) && "WuLiu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(14, i_power_balance)
              prepareStatement.setLong(33, i_power_balance)
            } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(15, i_power_balance)
              prepareStatement.setLong(34, i_power_balance)
            } else if (!"isdelete".equals(carType) && "KeYun".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(16, i_power_balance)
              prepareStatement.setLong(35, i_power_balance)
            } else if (!"isdelete".equals(carType) && "WangYue".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(17, i_power_balance)
              prepareStatement.setLong(36, i_power_balance)
            } else if (!"isdelete".equals(carType) && "QiTa".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(18, i_power_balance)
              prepareStatement.setLong(37, i_power_balance)
            }
            //业务类型统计
            if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(20, i_actual_balance)
              prepareStatement.setLong(39, i_actual_balance)
            } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(21, i_actual_balance)
              prepareStatement.setLong(40, i_actual_balance)
            } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(22, i_actual_balance)
              prepareStatement.setLong(41, i_actual_balance)
            } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(23, i_actual_balance)
              prepareStatement.setLong(42, i_actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            var carT = carType
            var bus_type = i_bus_type.toString()
            var isoday = dayd
            var power_balance = i_power_balance
            var actual_balance = i_actual_balance
            var b_feesingle_n = b_feesingle
            if (old_v_vin != null) { carT = old_carType }
            if (old_d_time_s != null) { isoday = old_dayd }
            if (old_i_bus_type != null) { bus_type = old_i_bus_type.toString() }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_b_feesingle != null) { b_feesingle_n = old_b_feesingle }
            var isnewC = 0
            if (old_d_time_s != null) {
              if (is_old_newC_row == 0) {
                isnewC = 1
              }
            } else {
              if (isnewC_row == 0) {
                isnewC = 1
              }
            }
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
            prepareStatement.setString(21, isoday)
            for (m <- 1 to 19) {
              prepareStatement.setLong(m, 0L)
            }
            //个人或车队更新
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(2, 1)
              prepareStatement.setLong(4, isnewC)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(1, 1)
              prepareStatement.setLong(3, isnewC)

            }
            //服务类型更新
            if ("2".equals(bus_type)) {
              prepareStatement.setLong(5, isnewC)
            } else if ("4".equals(bus_type) || "5".equals(bus_type)) {
              prepareStatement.setLong(6, isnewC)
            }
            //总收入统计
            if ("0".equals(b_feesingle_n)) {
              prepareStatement.setLong(7, actual_balance)
              prepareStatement.setLong(8, power_balance)
            }
            //车类型统计
            if (!"isdelete".equals(carT) && !"GongJiao".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(9, actual_balance)
              prepareStatement.setLong(15, power_balance)
            }
            if (!"isdelete".equals(carT) && "WuLiu".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(10, power_balance)
            } else if (!"isdelete".equals(carT) && "ChuZu".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(11, power_balance)
            } else if (!"isdelete".equals(carT) && "KeYun".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(12, power_balance)
            } else if (!"isdelete".equals(carT) && "WangYue".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(13, power_balance)
            } else if (!"isdelete".equals(carT) && "QiTa".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(14, power_balance)
            }
            //业务类型统计
            if ("3".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(16, actual_balance)
            } else if ("1".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(17, actual_balance)
            } else if ("2".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(18, actual_balance)
            } else if (("4".equals(bus_type) || "5".equals(bus_type)) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(19, actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //d_time_s修改操作
        else if (old_d_time_s != null && !dayd.equals(old_dayd) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var carT = carType
          var bus_type = i_bus_type.toString()
          var power_balance = i_power_balance
          var actual_balance = i_actual_balance
          var b_feesingle_n = b_feesingle
          var isnewC_old = 0
          var isnewC_new = 0
          if (old_v_vin != null) { carT = old_carType }
          if (old_i_bus_type != null) { bus_type = old_i_bus_type.toString() }
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_b_feesingle != null) { b_feesingle_n = old_b_feesingle }
          if (is_old_newC_row == 0) { isnewC_old = 1 }
          if (isnewC_row == 1) { isnewC_new = 1 }

          //减少旧日期数据
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
          prepareStatement.setString(21, old_dayd)
          for (m <- 1 to 19) {
            prepareStatement.setLong(m, 0L)
          }
          //个人或车队更新
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(2, 1)
            prepareStatement.setLong(4, isnewC_old)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(3, isnewC_old)

          }
          //服务类型更新
          if ("2".equals(bus_type)) {
            prepareStatement.setLong(5, isnewC_old)
          } else if ("4".equals(bus_type) || "5".equals(bus_type)) {
            prepareStatement.setLong(6, isnewC_old)
          }
          //总收入统计
          if ("0".equals(b_feesingle_n)) {
            prepareStatement.setLong(7, actual_balance)
            prepareStatement.setLong(8, power_balance)
          }
          //车类型统计
          if (!"isdelete".equals(carT) && !"GongJiao".equals(carT) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(9, actual_balance)
            prepareStatement.setLong(15, power_balance)
          }
          if (!"isdelete".equals(carT) && "WuLiu".equals(carT) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(10, power_balance)
          } else if (!"isdelete".equals(carT) && "ChuZu".equals(carT) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(11, power_balance)
          } else if (!"isdelete".equals(carT) && "KeYun".equals(carT) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(12, power_balance)
          } else if (!"isdelete".equals(carT) && "WangYue".equals(carT) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(13, power_balance)
          } else if (!"isdelete".equals(carT) && "QiTa".equals(carT) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(14, power_balance)
          }
          //业务类型统计
          if ("3".equals(bus_type) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(16, actual_balance)
          } else if ("1".equals(bus_type) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(17, actual_balance)
          } else if ("2".equals(bus_type) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(18, actual_balance)
          } else if (("4".equals(bus_type) || "5".equals(bus_type)) && "0".equals(b_feesingle_n)) {
            prepareStatement.setLong(19, actual_balance)
          }
          prepareStatement.execute()
          prepareStatement.close()

          //增加新日期数据
          val t_all_day_sql2 = new StringBuffer("insert into t_all_day_table values(")
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
          prepareStatement = connect.prepareStatement(t_all_day_sql2.toString())
          prepareStatement.setInt(1, l_seller_id)
          prepareStatement.setString(2, dayd)
          for (m <- 3 to 42) {
            prepareStatement.setLong(m, 0L)
          }
          //个人或车队更新
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(4, 1)
            prepareStatement.setLong(8, isnewC_new)
            prepareStatement.setLong(25, 1)
            prepareStatement.setLong(27, isnewC_new)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(3, 1)
            prepareStatement.setLong(7, isnewC_new)
            prepareStatement.setLong(24, 1)
            prepareStatement.setLong(26, isnewC_new)

          }
          //服务类型更新
          if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(9, isnewC_new)
            prepareStatement.setLong(28, isnewC_new)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(10, isnewC_new)
            prepareStatement.setLong(29, isnewC_new)
          }
          //总收入统计
          if ("0".equals(b_feesingle)) {
            prepareStatement.setLong(11, i_actual_balance)
            prepareStatement.setLong(12, i_power_balance)
            prepareStatement.setLong(30, i_actual_balance)
            prepareStatement.setLong(31, i_power_balance)
          }
          //车类型统计
          if (!"isdelete".equals(carType) && !"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(13, i_actual_balance)
            prepareStatement.setLong(32, i_actual_balance)
            prepareStatement.setLong(19, i_power_balance)
            prepareStatement.setLong(38, i_power_balance)
          }
          if (!"isdelete".equals(carType) && "WuLiu".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(14, i_power_balance)
            prepareStatement.setLong(33, i_power_balance)
          } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(15, i_power_balance)
            prepareStatement.setLong(34, i_power_balance)
          } else if (!"isdelete".equals(carType) && "KeYun".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(16, i_power_balance)
            prepareStatement.setLong(35, i_power_balance)
          } else if (!"isdelete".equals(carType) && "WangYue".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(17, i_power_balance)
            prepareStatement.setLong(36, i_power_balance)
          } else if (!"isdelete".equals(carType) && "QiTa".equals(carType) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(18, i_power_balance)
            prepareStatement.setLong(37, i_power_balance)
          }
          //业务类型统计
          if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(20, i_actual_balance)
            prepareStatement.setLong(39, i_actual_balance)
          } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(21, i_actual_balance)
            prepareStatement.setLong(40, i_actual_balance)
          } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(22, i_actual_balance)
            prepareStatement.setLong(41, i_actual_balance)
          } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
            prepareStatement.setLong(23, i_actual_balance)
            prepareStatement.setLong(42, i_actual_balance)
          }
          prepareStatement.execute()
          prepareStatement.close()

        } //v_vin修改或i_bus_type修改操作
        else if ((old_v_vin != null || old_i_bus_type != null) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var carT = carType
          var bus_type = i_bus_type
          var power_balance = i_power_balance
          var actual_balance = i_actual_balance
          var isnewC_new = 0
          var b_feesingle_n = b_feesingle
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_i_bus_type != null) { bus_type = old_i_bus_type }
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_b_feesingle != null) { b_feesingle_n = old_b_feesingle }
          if (isnewC_row == 1) { isnewC_new = 1 }
          val t_all_day_sql = new StringBuffer("update t_all_day_table set")
            .append(" day_service_app_user=day_service_app_user+?,")
            .append(" day_service_wechat_user=day_service_wechat_user+?,")
            .append(" day_i_actual_balance=day_i_actual_balance+?,")
            .append(" day_i_power_balance=day_i_power_balance+?,")
            .append(" day_i_actual_balance_balance_nteam=day_i_actual_balance_balance_nteam+?,") //5
            .append(" day_i_actual_balance_balance_WuLiu=day_i_actual_balance_balance_WuLiu+?,")
            .append(" day_i_actual_balance_balance_ChuZu=day_i_actual_balance_balance_ChuZu+?,")
            .append(" day_i_actual_balance_balance_KeYun=day_i_actual_balance_balance_KeYun+?,")
            .append(" day_i_actual_balance_balance_WangYue=day_i_actual_balance_balance_WangYue+?,")
            .append(" day_i_actual_balance_balance_QiTa=day_i_actual_balance_balance_QiTa+?,") //10
            .append(" day_i_power_balance_nteam=day_i_power_balance_nteam+?,")
            .append(" day_i_actual_balance_vin=day_i_actual_balance_vin+?,")
            .append(" day_i_actual_balance_card=day_i_actual_balance_card+?,")
            .append(" day_i_actual_balance_app=day_i_actual_balance_app+?,")
            .append(" day_i_actual_balance_wechat=day_i_actual_balance_wechat+?") //15
            .append(" where l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(t_all_day_sql.toString())
          prepareStatement.setInt(16, l_seller_id)
          prepareStatement.setString(17, dayd)
          for (m <- 1 to 15) {
            prepareStatement.setLong(m, 0L)
          }
          //修改了vin
          if (old_v_vin != null && !old_v_vin.equals(v_vin)) {
            var diff_actual_balance = 0L
            var diff_power_balance = 0L
            carT = old_carType
            //减少旧数据
            if (!"isdelete".equals(carT) && !"GongJiao".equals(carT) && "0".equals(b_feesingle_n)) {
              diff_actual_balance = -actual_balance
              diff_power_balance = -power_balance
            }
            if (!"isdelete".equals(carT) && "WuLiu".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(6, -power_balance)
            } else if (!"isdelete".equals(carT) && "ChuZu".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(7, -power_balance)
            } else if (!"isdelete".equals(carT) && "KeYun".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(8, -power_balance)
            } else if (!"isdelete".equals(carT) && "WangYue".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(9, -power_balance)
            } else if (!"isdelete".equals(carT) && "QiTa".equals(carT) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(10, -power_balance)
            }
            //增加新数据
            if (!"isdelete".equals(carType) && !"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
              diff_actual_balance = diff_actual_balance + i_actual_balance
              diff_power_balance = diff_power_balance + i_power_balance
            }
            if (!"isdelete".equals(carType) && "WuLiu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(6, i_power_balance)
            } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(7, i_power_balance)
            } else if (!"isdelete".equals(carType) && "KeYun".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(8, i_power_balance)
            } else if (!"isdelete".equals(carType) && "WangYue".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(9, i_power_balance)
            } else if (!"isdelete".equals(carType) && "QiTa".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(10, i_power_balance)
            }
            prepareStatement.setLong(5, diff_actual_balance)
            prepareStatement.setLong(11, diff_power_balance)
          }
          //修改了业务类型
          if (old_i_bus_type != null && !old_i_bus_type.equals(i_bus_type)) {
            bus_type = old_i_bus_type.toString()
            //减少旧数据
            if ("3".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(12, -actual_balance)
            } else if ("1".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(13, -actual_balance)
            } else if ("2".equals(bus_type) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(14, -actual_balance)
              prepareStatement.setLong(1, -isnewC_new)
            } else if (("4".equals(bus_type) || "5".equals(bus_type)) && "0".equals(b_feesingle_n)) {
              prepareStatement.setLong(15, -actual_balance)
              prepareStatement.setLong(2, -isnewC_new)
            }
            //增加新数据
            if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(12, i_actual_balance)
            } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(13, i_actual_balance)
            } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(14, i_actual_balance)
              prepareStatement.setLong(1, isnewC_new)
            } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(15, i_actual_balance)
              prepareStatement.setLong(2, isnewC_new)
            }
          }
          //总金额的更新修改
          //old_b_feesingle没有进行更新则加金额差值
          if (old_b_feesingle == null) {

            prepareStatement.setLong(3, i_actual_balance - actual_balance)
            prepareStatement.setLong(4, i_power_balance - power_balance)
          } //有进行更新
          else if (old_b_feesingle != null) {
            if ("0".equals(old_b_feesingle) && "1".equals(b_feesingle)) {
              prepareStatement.setLong(3, -actual_balance)
              prepareStatement.setLong(4, -power_balance)
            } else if ("1".equals(old_b_feesingle) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(3, i_actual_balance)
              prepareStatement.setLong(4, i_power_balance)
            }
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改了 i_power_balance或i_actual_balance或b_feesingle
        else if ((old_b_feesingle != null || old_i_power_b != null || old_i_actual_b != null)
          && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var power_balance = i_power_balance
          var actual_balance = i_actual_balance
          var b_feesingle_n = b_feesingle
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_b_feesingle != null) { b_feesingle_n = old_b_feesingle }
          val diff_actual_balance = i_actual_balance - actual_balance
          val diff_power_balance = i_power_balance - power_balance
          val t_all_day_sql = new StringBuffer("update t_all_day_table set")
            .append(" day_i_actual_balance=day_i_actual_balance+?,")
            .append(" day_i_power_balance=day_i_power_balance+?,")
            .append(" day_i_actual_balance_balance_nteam=day_i_actual_balance_balance_nteam+?,")
            .append(" day_i_actual_balance_balance_WuLiu=day_i_actual_balance_balance_WuLiu+?,")
            .append(" day_i_actual_balance_balance_ChuZu=day_i_actual_balance_balance_ChuZu+?,") //5
            .append(" day_i_actual_balance_balance_KeYun=day_i_actual_balance_balance_KeYun+?,")
            .append(" day_i_actual_balance_balance_WangYue=day_i_actual_balance_balance_WangYue+?,")
            .append(" day_i_actual_balance_balance_QiTa=day_i_actual_balance_balance_QiTa+?,")
            .append(" day_i_power_balance_nteam=day_i_power_balance_nteam+?,")
            .append(" day_i_actual_balance_vin=day_i_actual_balance_vin+?,") //10
            .append(" day_i_actual_balance_card=day_i_actual_balance_card+?,")
            .append(" day_i_actual_balance_app=day_i_actual_balance_app+?,")
            .append(" day_i_actual_balance_wechat=day_i_actual_balance_wechat+?") //13
            .append(" where l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(t_all_day_sql.toString())
          prepareStatement.setInt(14, l_seller_id)
          prepareStatement.setString(15, dayd)
          for (m <- 1 to 13) {
            prepareStatement.setLong(m, 0L)
          }
          //b_feesingle无改变则统计差值
          if (old_b_feesingle == null) {
            //日总金额
            prepareStatement.setLong(1, diff_actual_balance)
            prepareStatement.setLong(2, diff_power_balance)
            //业务类型数据更新
            if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(10, diff_actual_balance)
            } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(11, diff_actual_balance)
            } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(12, diff_actual_balance)
            } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(13, diff_actual_balance)
            }
            //车类型是数据更新
            if (!"isdelete".equals(carType) && !"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(3, diff_actual_balance)
              prepareStatement.setLong(9, diff_power_balance)
            }
            if (!"isdelete".equals(carType) && "WuLiu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(4, diff_power_balance)
            } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(5, diff_power_balance)
            } else if (!"isdelete".equals(carType) && "KeYun".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(6, diff_power_balance)
            } else if (!"isdelete".equals(carType) && "WangYue".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(7, diff_power_balance)
            } else if (!"isdelete".equals(carType) && "QiTa".equals(carType) && "0".equals(b_feesingle)) {
              prepareStatement.setLong(8, diff_power_balance)
            }
          } //b_feesingle更新了
          else if (old_b_feesingle != null) {
            log.info("b_feesingle更新了" + "old_b_feesingle:" + old_b_feesingle + " b_feesingle:" + b_feesingle)
            if ("0".equals(old_b_feesingle) && "1".equals(b_feesingle)) {
              //日总金额
              prepareStatement.setLong(1, -actual_balance)
              prepareStatement.setLong(2, -power_balance)
              //业务类型数据更新
              if ("3".equals(i_bus_type)) {
                prepareStatement.setLong(10, -actual_balance)
              } else if ("1".equals(i_bus_type)) {
                prepareStatement.setLong(11, -actual_balance)
              } else if ("2".equals(i_bus_type)) {
                prepareStatement.setLong(12, -actual_balance)
              } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type))) {
                prepareStatement.setLong(13, -actual_balance)
              }
              //车类型是数据更新
              if (!"isdelete".equals(carType) && !"GongJiao".equals(carType)) {
                prepareStatement.setLong(3, -actual_balance)
                prepareStatement.setLong(9, -power_balance)
              }
              if (!"isdelete".equals(carType) && "WuLiu".equals(carType)) {
                prepareStatement.setLong(4, -power_balance)
              } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType)) {
                prepareStatement.setLong(5, -power_balance)
              } else if (!"isdelete".equals(carType) && "KeYun".equals(carType)) {
                prepareStatement.setLong(6, -power_balance)
              } else if (!"isdelete".equals(carType) && "WangYue".equals(carType)) {
                prepareStatement.setLong(7, -power_balance)
              } else if (!"isdelete".equals(carType) && "QiTa".equals(carType)) {
                prepareStatement.setLong(8, -power_balance)
              }
            } else if ("1".equals(old_b_feesingle) && "0".equals(b_feesingle)) {
              //日总金额
              prepareStatement.setLong(1, i_actual_balance)
              prepareStatement.setLong(2, i_power_balance)
              //业务类型数据更新
              if ("3".equals(i_bus_type) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(10, i_actual_balance)
              } else if ("1".equals(i_bus_type) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(11, i_actual_balance)
              } else if ("2".equals(i_bus_type) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(12, i_actual_balance)
              } else if (("4".equals(i_bus_type) || "5".equals(i_bus_type)) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(13, i_actual_balance)
              }
              //车类型是数据更新
              if (!"isdelete".equals(carType) && !"GongJiao".equals(carType) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(3, i_actual_balance)
                prepareStatement.setLong(9, i_power_balance)
              }
              if (!"isdelete".equals(carType) && "WuLiu".equals(carType) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(4, i_power_balance)
              } else if (!"isdelete".equals(carType) && "ChuZu".equals(carType) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(5, i_power_balance)
              } else if (!"isdelete".equals(carType) && "KeYun".equals(carType) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(6, i_power_balance)
              } else if (!"isdelete".equals(carType) && "WangYue".equals(carType) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(7, i_power_balance)
              } else if (!"isdelete".equals(carType) && "QiTa".equals(carType) && "0".equals(b_feesingle)) {
                prepareStatement.setLong(8, i_power_balance)
              }
            }
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
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
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***t_all_day_table**关闭prepareStatement失败****")
        }
      }
/*****************************************************************************************************/
      //t_all_month_total更新
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
            var power_balance0 = i_power_balance
            var isfee = 0L
            if (1 == b_feesingle || 1 == b_fee_service) {
              isfee = m_service_balance - i_aiscount_balance
            }
            if (b_feesingle == 1) {
              power_balance0 = 0
            }
//            val serviceban = (m_service_balance - isfee - i_aiscount_balance - i_deductible_balance) + m_appoint_balance + m_parking_balance
            val serviceban = m_service_balance - isfee - i_aiscount_balance - i_deductible_balance
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
            prepareStatement.setString(2, dayd_month)
            for (m <- 3 to 17) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(8, power_balance0)
            prepareStatement.setLong(9, serviceban)
            prepareStatement.setLong(14, power_balance0)
            prepareStatement.setLong(15, serviceban)
            //个人车队统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(4, m_sum_power)
              prepareStatement.setLong(13, m_sum_power)
              prepareStatement.setLong(10, power_balance0)
              prepareStatement.setLong(16, power_balance0)
              prepareStatement.setLong(11, serviceban)
              prepareStatement.setLong(17, serviceban)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(3, m_sum_power)
              prepareStatement.setLong(12, m_sum_power)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
//            var parking_balance = m_parking_balance
            var power_balance = i_power_balance
//            var appoint_balance = m_appoint_balance
            var sum_power = m_sum_power
            var service_balance = m_service_balance
            var deductible_balance = i_deductible_balance
            var aiscount_balance = i_aiscount_balance
            var bi_feesingle = b_feesingle.toLong
            var isoday = dayd_month
            var b_fee = b_fee_service
            var isfee = 0L
            val old_b_fee = old.getString("b_fee_service")
            if (old_b_fee != null) { b_fee = old_b_fee_service }
            if (old_b_feesingle != null) { bi_feesingle = old_b_feesingle.toLong }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
//            if (old_m_appoint_b != null) { appoint_balance = old_m_appoint_balance }
//            if (old_m_parking_b != null) { parking_balance = old_m_parking_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
            if (old_i_aiscount_b != null) { aiscount_balance = old_i_aiscount_balance }
            if (old_d_time_s != null) { isoday = old_dayd_month }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (1 == bi_feesingle || 1 == b_fee) {
              isfee = service_balance - aiscount_balance
            }
            if (bi_feesingle == 1) {
              power_balance = 0
            }
//            val serviceban = (m_service_balance - isfee - aiscount_balance - deductible_balance) + appoint_balance + parking_balance
            val serviceban = m_service_balance - isfee - aiscount_balance - deductible_balance
            val t_all_month_sql = new StringBuffer("update t_all_month_table set")
              .append(" month_team_charging_power=month_team_charging_power-?,")
              .append(" month_personal_charging_power=month_personal_charging_power-?,")
              .append(" month_i_charging_actual_balance=month_i_charging_actual_balance-?,")
              .append(" month_i_service_actual_balance=month_i_service_actual_balance-?,")
              .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance-?,")
              .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance-?")
              .append(" where l_seller_id=? and d_month_time=?")
            prepareStatement = connect.prepareStatement(t_all_month_sql.toString())
            prepareStatement.setInt(7, l_seller_id)
            prepareStatement.setString(8, isoday)
            for (m <- 1 to 6) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(3, power_balance)
            prepareStatement.setLong(4, serviceban)
            //个人车队统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(5, power_balance)
              prepareStatement.setLong(6, serviceban)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(1, sum_power)
            }
            prepareStatement.execute()
            prepareStatement.close()

          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
             var power_balance0 = i_power_balance
            var isfee = 0L
            if (1 == b_feesingle || 1 == b_fee_service) {
              isfee = m_service_balance - i_aiscount_balance
            }
            if (b_feesingle == 1) {
              power_balance0 = 0
            }
            val serviceban = m_service_balance - isfee - i_aiscount_balance - i_deductible_balance
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
            prepareStatement.setString(2, dayd_month)
            for (m <- 3 to 17) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(8, power_balance0)
            prepareStatement.setLong(9, serviceban)
            prepareStatement.setLong(14, power_balance0)
            prepareStatement.setLong(15, serviceban)
            //个人车队统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(4, m_sum_power)
              prepareStatement.setLong(13, m_sum_power)
              prepareStatement.setLong(10, power_balance0)
              prepareStatement.setLong(16, power_balance0)
              prepareStatement.setLong(11, serviceban)
              prepareStatement.setLong(17, serviceban)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(3, m_sum_power)
              prepareStatement.setLong(12, m_sum_power)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
//            var parking_balance = m_parking_balance
            var power_balance = i_power_balance
//            var appoint_balance = m_appoint_balance
            var sum_power = m_sum_power
            var service_balance = m_service_balance
            var deductible_balance = i_deductible_balance
            var aiscount_balance = i_aiscount_balance
            var bi_feesingle = b_feesingle.toLong
            var isoday = dayd_month
            var b_fee = b_fee_service
            var isfee = 0L
            val old_b_fee = old.getString("b_fee_service")
            if (old_b_fee != null) { b_fee = old_b_fee_service }
            if (old_b_feesingle != null) { bi_feesingle = old_b_feesingle.toLong }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
//            if (old_m_appoint_b != null) { appoint_balance = old_m_appoint_balance }
//            if (old_m_parking_b != null) { parking_balance = old_m_parking_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
            if (old_i_aiscount_b != null) { aiscount_balance = old_i_aiscount_balance }
            if (old_d_time_s != null) { isoday = old_dayd_month }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (1 == bi_feesingle || 1 == b_fee) {
              isfee = service_balance - aiscount_balance
            }
            if (bi_feesingle == 1) {
              power_balance = 0
            }
            val serviceban = m_service_balance - isfee - aiscount_balance - deductible_balance
//            val serviceban = (m_service_balance - isfee - aiscount_balance - deductible_balance) + appoint_balance + parking_balance
            val t_all_month_sql = new StringBuffer("update t_all_month_table set")
              .append(" month_team_charging_power=month_team_charging_power-?,")
              .append(" month_personal_charging_power=month_personal_charging_power-?,")
              .append(" month_i_charging_actual_balance=month_i_charging_actual_balance-?,")
              .append(" month_i_service_actual_balance=month_i_service_actual_balance-?,")
              .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance-?,")
              .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance-?")
              .append(" where l_seller_id=? and d_month_time=?")
            prepareStatement = connect.prepareStatement(t_all_month_sql.toString())
            prepareStatement.setInt(7, l_seller_id)
            prepareStatement.setString(8, isoday)
            for (m <- 1 to 6) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(3, power_balance)
            prepareStatement.setLong(4, serviceban)
            //个人车队统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(5, power_balance)
              prepareStatement.setLong(6, serviceban)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(1, sum_power)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //d_time_s修改操作
        else if (old_d_time_s != null && !dayd_month.equals(old_dayd_month) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
//          var parking_balance = m_parking_balance
            var power_balance = i_power_balance
//            var appoint_balance = m_appoint_balance
            var sum_power = m_sum_power
            var service_balance = m_service_balance
            var deductible_balance = i_deductible_balance
            var aiscount_balance = i_aiscount_balance
            var bi_feesingle = b_feesingle.toLong
            var isoday = dayd_month
            var b_fee = b_fee_service
            var isfee = 0L
            val old_b_fee = old.getString("b_fee_service")
            if (old_b_fee != null) { b_fee = old_b_fee_service }
            if (old_b_feesingle != null) { bi_feesingle = old_b_feesingle.toLong }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
//            if (old_m_appoint_b != null) { appoint_balance = old_m_appoint_balance }
//            if (old_m_parking_b != null) { parking_balance = old_m_parking_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
            if (old_i_aiscount_b != null) { aiscount_balance = old_i_aiscount_balance }
            if (old_d_time_s != null) { isoday = old_dayd_month }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (1 == bi_feesingle || 1 == b_fee) {
              isfee = service_balance - aiscount_balance
            }
            if (bi_feesingle == 1) {
              power_balance = 0
            }
//            val serviceban = (m_service_balance - isfee - aiscount_balance - deductible_balance) + appoint_balance + parking_balance
          val serviceban = m_service_balance - isfee - aiscount_balance - deductible_balance

          //减少旧日期数据
          val t_all_month_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_team_charging_power=month_team_charging_power-?,")
            .append(" month_personal_charging_power=month_personal_charging_power-?,")
            .append(" month_i_charging_actual_balance=month_i_charging_actual_balance-?,")
            .append(" month_i_service_actual_balance=month_i_service_actual_balance-?,")
            .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance-?,")
            .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance-?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(t_all_month_sql.toString())
          prepareStatement.setInt(7, l_seller_id)
          prepareStatement.setString(8, old_dayd_month)
          for (m <- 1 to 6) {
            prepareStatement.setLong(m, 0L)
          }
          prepareStatement.setLong(3, power_balance)
          prepareStatement.setLong(4, serviceban)
          //个人车队统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(2, sum_power)
            prepareStatement.setLong(5, power_balance)
            prepareStatement.setLong(6,serviceban)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(1, sum_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
         var power_balance0 = i_power_balance
            var isfee_new = 0L
            if (1 == b_feesingle || 1 == b_fee_service) {
              isfee = m_service_balance - i_aiscount_balance
            }
            if (b_feesingle == 1) {
              power_balance0 = 0
            }
//            val serviceban_new = (m_service_balance - isfee_new - i_aiscount_balance - i_deductible_balance) + m_appoint_balance + m_parking_balance
          val serviceban_new = m_service_balance - isfee_new - i_aiscount_balance - i_deductible_balance
          //增加新日期数据
          val t_all_month_sql1 = new StringBuffer("insert into t_all_month_table values(?,?,?,?,?,?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_team_charging_power=month_team_charging_power+?,")
            .append(" month_personal_charging_power=month_personal_charging_power+?,")
            .append(" month_i_charging_actual_balance=month_i_charging_actual_balance+?,")
            .append(" month_i_service_actual_balance=month_i_service_actual_balance+?,")
            .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance+?,")
            .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance+?")
          prepareStatement = connect.prepareStatement(t_all_month_sql1.toString())
          prepareStatement.setInt(1, l_seller_id)
          prepareStatement.setString(2, dayd_month)
          for (m <- 3 to 17) {
            prepareStatement.setLong(m, 0L)
          }
          prepareStatement.setLong(8, power_balance0)
          prepareStatement.setLong(9, serviceban_new)
          prepareStatement.setLong(14, power_balance0)
          prepareStatement.setLong(15, serviceban_new)
          //个人车队统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(4, m_sum_power)
            prepareStatement.setLong(13, m_sum_power)
            prepareStatement.setLong(10, power_balance0)
            prepareStatement.setLong(16, power_balance0)
            prepareStatement.setLong(11,serviceban_new)
            prepareStatement.setLong(17, serviceban_new)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(3, m_sum_power)
            prepareStatement.setLong(12, m_sum_power)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改了m_sum_power或m_service_balance或m_appoint_balance或m_parking_balance或i_power_balance
        else if ((old_m_service_b != null || old_m_sum_p != null
          || old_i_power_b != null || old_i_aiscount_b != null)
          && "1".equals(b_finish) && "0".equals(b_delete_flag) && "0".equals(b_feesingle)) {
//         var parking_balance = m_parking_balance
            var power_balance = i_power_balance
//            var appoint_balance = m_appoint_balance
            var sum_power = m_sum_power
            var service_balance = m_service_balance
            var deductible_balance = i_deductible_balance
            var aiscount_balance = i_aiscount_balance
            var bi_feesingle = b_feesingle.toLong
            var isoday = dayd_month
            var b_fee = b_fee_service
            var isfee = 0L
            val old_b_fee = old.getString("b_fee_service")
            if (old_b_fee != null) { b_fee = old_b_fee_service }
            if (old_b_feesingle != null) { bi_feesingle = old_b_feesingle.toLong }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
//            if (old_m_appoint_b != null) { appoint_balance = old_m_appoint_balance }
//            if (old_m_parking_b != null) { parking_balance = old_m_parking_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
            if (old_i_aiscount_b != null) { aiscount_balance = old_i_aiscount_balance }
            if (old_d_time_s != null) { isoday = old_dayd_month }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          var diff_power_balance = i_power_balance - power_balance
//          val diff_parking_balance = m_parking_balance - parking_balance
//          val diff_appoint_balance = m_appoint_balance - appoint_balance
          val diff_sum_power = m_sum_power - sum_power
          val diff_service_balance = m_service_balance - service_balance
          val diff_aiscount_balance = i_aiscount_balance-aiscount_balance
          val diff_i_deductible_balance = i_deductible_balance-deductible_balance
            var isfee_new = 0L
            if (1 == b_feesingle || 1 == b_fee_service) {
              isfee = diff_service_balance - diff_aiscount_balance
            }
            if (b_feesingle == 1) {
              diff_power_balance = power_balance
            }
//            val serviceban = (diff_service_balance - isfee_new - diff_aiscount_balance - diff_i_deductible_balance) + diff_appoint_balance + diff_parking_balance
          val serviceban = diff_service_balance - isfee_new - diff_aiscount_balance - diff_i_deductible_balance
          val t_all_month_sql = new StringBuffer("update t_all_month_table set")
            .append(" month_team_charging_power=month_team_charging_power+?,")
            .append(" month_personal_charging_power=month_personal_charging_power+?,")
            .append(" month_i_charging_actual_balance=month_i_charging_actual_balance+?,")
            .append(" month_i_service_actual_balance=month_i_service_actual_balance+?,")
            .append(" month_personal_i_charging_actual_balance=month_personal_i_charging_actual_balance+?,")
            .append(" month_personal_i_service_actual_balance=month_personal_i_service_actual_balance+?")
            .append(" where l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(t_all_month_sql.toString())
          prepareStatement.setInt(7, l_seller_id)
          prepareStatement.setString(8, dayd_month)
          prepareStatement.setLong(3, diff_power_balance)
          prepareStatement.setLong(4, serviceban)
          //个人车队统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(2, diff_sum_power)
            prepareStatement.setLong(5, diff_power_balance)
            prepareStatement.setLong(6, serviceban)

            prepareStatement.setLong(1, 0)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(1, diff_sum_power)

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
/**************************************************************************************************/
      //t_all_total更新
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
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

            prepareStatement.setInt(1, l_seller_id)
            for (m <- 2 to 61) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(3, m_sum_power)
            prepareStatement.setLong(37, m_sum_power)
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(39, 1)
            prepareStatement.setLong(8, diffT)
            prepareStatement.setLong(42, diffT)
            prepareStatement.setLong(20, isnewC_id)
            prepareStatement.setLong(45, isnewC_id)
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
            } else if ("1".equals(i_bus_type)) {
              prepareStatement.setLong(34, i_actual_balance)
              prepareStatement.setLong(59, i_actual_balance)
            } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
              prepareStatement.setLong(35, i_actual_balance)
              prepareStatement.setLong(60, i_actual_balance)
            } else if ("2".equals(i_bus_type)) {
              prepareStatement.setLong(36, i_actual_balance)
              prepareStatement.setLong(61, i_actual_balance)
            }
            //会员等级统计
            if ("1".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(23, 1)
              prepareStatement.setLong(48, 1)
            } else if ("2".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(24, 1)
              prepareStatement.setLong(49, 1)
            } else if ("3".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(25, 1)
              prepareStatement.setLong(50, 1)
            } else if ("4".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(26, 1)
              prepareStatement.setLong(51, 1)
            } else if ("5".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(27, 1)
              prepareStatement.setLong(52, 1)
            }
            //车队个人类型统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(7, 1)
              prepareStatement.setLong(41, 1)
              prepareStatement.setLong(10, diffT)
              prepareStatement.setLong(44, diffT)
              prepareStatement.setLong(22, isnewC_id)
              prepareStatement.setLong(47, isnewC_id)
              prepareStatement.setLong(30, i_actual_balance)
              prepareStatement.setLong(55, i_actual_balance)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(4, m_sum_power)
              prepareStatement.setLong(38, m_sum_power)
              prepareStatement.setLong(6, 1)
              prepareStatement.setLong(40, 1)
              prepareStatement.setLong(9, diffT)
              prepareStatement.setLong(43, diffT)
              prepareStatement.setLong(21, isnewC_id)
              prepareStatement.setLong(46, isnewC_id)
              prepareStatement.setLong(29, i_actual_balance)
              prepareStatement.setLong(54, i_actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()

          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            var power_balance = i_power_balance
            var sum_power = m_sum_power
            var service_balance = m_service_balance
            var bus_type = i_bus_type
            var actual_balance = i_actual_balance
            if (old_i_bus_type != null) { bus_type = old_i_bus_type }
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
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
            prepareStatement.setInt(26, l_seller_id)
            for (m <- 1 to 25) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(1, sum_power)
            prepareStatement.setLong(3, 1)
            prepareStatement.setLong(6, old_diffT)
            if (isnewC_id_row == 0) {
              prepareStatement.setLong(9, 1)
            }
            prepareStatement.setLong(17, actual_balance)
            prepareStatement.setLong(20, power_balance)
            prepareStatement.setLong(21, old_m_service_balance)
            //业务类型统计
            if ("3".equals(bus_type)) {
              prepareStatement.setLong(22, actual_balance)
            } else if ("1".equals(bus_type)) {
              prepareStatement.setLong(23, actual_balance)
            } else if ("4".equals(bus_type) || "5".equals(bus_type)) {
              prepareStatement.setLong(24, actual_balance)
            } else if ("2".equals(bus_type)) {
              prepareStatement.setLong(25, actual_balance)
            }
            //会员等级统计
            if ("1".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(12, 1)
            } else if ("2".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(13, 1)
            } else if ("3".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(14, 1)
            } else if ("4".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(15, 1)
            } else if ("5".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(16, 1)
            }
            //车队个人类型统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(5, 1)
              prepareStatement.setLong(8, old_diffT)
              if (isnewC_id_row == 0) {
                prepareStatement.setLong(11, 1)
              }
              prepareStatement.setLong(19, actual_balance)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(4, 1)
              prepareStatement.setLong(7, old_diffT)
              if (isnewC_id_row == 0) {
                prepareStatement.setLong(10, 1)
              }
              prepareStatement.setLong(18, actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()

          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
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

            prepareStatement.setInt(1, l_seller_id)
            for (m <- 2 to 61) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(3, m_sum_power)
            prepareStatement.setLong(37, m_sum_power)
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(39, 1)
            prepareStatement.setLong(8, diffT)
            prepareStatement.setLong(42, diffT)
            prepareStatement.setLong(20, isnewC_id)
            prepareStatement.setLong(45, isnewC_id)
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
            } else if ("1".equals(i_bus_type)) {
              prepareStatement.setLong(34, i_actual_balance)
              prepareStatement.setLong(59, i_actual_balance)
            } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
              prepareStatement.setLong(35, i_actual_balance)
              prepareStatement.setLong(60, i_actual_balance)
            } else if ("2".equals(i_bus_type)) {
              prepareStatement.setLong(36, i_actual_balance)
              prepareStatement.setLong(61, i_actual_balance)
            }
            //会员等级统计
            if ("1".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(23, 1)
              prepareStatement.setLong(48, 1)
            } else if ("2".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(24, 1)
              prepareStatement.setLong(49, 1)
            } else if ("3".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(25, 1)
              prepareStatement.setLong(50, 1)
            } else if ("4".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(26, 1)
              prepareStatement.setLong(51, 1)
            } else if ("5".equals(level) && 1 == isnewC_id) {
              prepareStatement.setLong(27, 1)
              prepareStatement.setLong(52, 1)
            }
            //车队个人类型统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(7, 1)
              prepareStatement.setLong(41, 1)
              prepareStatement.setLong(10, diffT)
              prepareStatement.setLong(44, diffT)
              prepareStatement.setLong(22, isnewC_id)
              prepareStatement.setLong(47, isnewC_id)
              prepareStatement.setLong(30, i_actual_balance)
              prepareStatement.setLong(55, i_actual_balance)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(4, m_sum_power)
              prepareStatement.setLong(38, m_sum_power)
              prepareStatement.setLong(6, 1)
              prepareStatement.setLong(40, 1)
              prepareStatement.setLong(9, diffT)
              prepareStatement.setLong(43, diffT)
              prepareStatement.setLong(21, isnewC_id)
              prepareStatement.setLong(46, isnewC_id)
              prepareStatement.setLong(29, i_actual_balance)
              prepareStatement.setLong(54, i_actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            var power_balance = i_power_balance
            var sum_power = m_sum_power
            var service_balance = m_service_balance
            var bus_type = i_bus_type
            var actual_balance = i_actual_balance
            if (old_i_bus_type != null) { bus_type = old_i_bus_type }
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
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
            prepareStatement.setInt(26, l_seller_id)
            for (m <- 1 to 25) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(1, sum_power)
            prepareStatement.setLong(3, 1)
            prepareStatement.setLong(6, old_diffT)
            if (isnewC_id_row == 0) {
              prepareStatement.setLong(9, 1)
            }
            prepareStatement.setLong(17, actual_balance)
            prepareStatement.setLong(20, power_balance)
            prepareStatement.setLong(21, old_m_service_balance)
            //业务类型统计
            if ("3".equals(bus_type)) {
              prepareStatement.setLong(22, actual_balance)
            } else if ("1".equals(bus_type)) {
              prepareStatement.setLong(23, actual_balance)
            } else if ("4".equals(bus_type) || "5".equals(bus_type)) {
              prepareStatement.setLong(24, actual_balance)
            } else if ("2".equals(bus_type)) {
              prepareStatement.setLong(25, actual_balance)
            }
            //会员等级统计
            if ("1".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(12, 1)
            } else if ("2".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(13, 1)
            } else if ("3".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(14, 1)
            } else if ("4".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(15, 1)
            } else if ("5".equals(level) && 0 == isnewC_id_row) {
              prepareStatement.setLong(16, 1)
            }
            //车队个人类型统计
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(5, 1)
              prepareStatement.setLong(8, old_diffT)
              if (isnewC_id_row == 0) {
                prepareStatement.setLong(11, 1)
              }
              prepareStatement.setLong(19, actual_balance)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(4, 1)
              prepareStatement.setLong(7, old_diffT)
              if (isnewC_id_row == 0) {
                prepareStatement.setLong(10, 1)
              }
              prepareStatement.setLong(18, actual_balance)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //i_bus_type修改操作
        else if (old_i_bus_type != null && !old_i_bus_type.equals(i_bus_type) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var power_balance = i_power_balance
          var sum_power = m_sum_power
          var service_balance = m_service_balance
          var bus_type = i_bus_type
          var actual_balance = i_actual_balance
          if (old_i_bus_type != null) { bus_type = old_i_bus_type }
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_m_service_b != null) { service_balance = old_m_service_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          val diff_sum_power = m_sum_power - sum_power
          val diff_service_balance = m_service_balance - service_balance
          val diff_power_balance = i_power_balance - power_balance
          val diff_actual_balance = i_actual_balance - actual_balance
          val diff_diffT = diffT - old_diffT

          val t_all_sql_diff = new StringBuffer("update t_all_table set")
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
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(t_all_sql_diff.toString())
          prepareStatement.setInt(26, l_seller_id)
          for (m <- 1 to 25) {
            prepareStatement.setLong(m, 0L)
          }
          prepareStatement.setLong(1, diff_sum_power)
          prepareStatement.setLong(6, diff_diffT)
          prepareStatement.setLong(17, diff_actual_balance)
          prepareStatement.setLong(20, diff_power_balance)
          prepareStatement.setLong(21, diff_service_balance)
          //车队个人类型统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(8, diff_diffT)
            prepareStatement.setLong(19, diff_actual_balance)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, diff_sum_power)
            prepareStatement.setLong(7, diff_diffT)
            prepareStatement.setLong(18, diff_actual_balance)
          }
          //业务类型统计-减少旧数据
          if ("3".equals(old_i_bus_type)) {
            prepareStatement.setLong(22, -actual_balance)
          } else if ("1".equals(old_i_bus_type)) {
            prepareStatement.setLong(23, -actual_balance)
          } else if ("4".equals(old_i_bus_type) || "5".equals(old_i_bus_type)) {
            prepareStatement.setLong(24, -actual_balance)
          } else if ("2".equals(old_i_bus_type)) {
            prepareStatement.setLong(25, -actual_balance)
          }
          //业务类型统计-增加新数据
          if ("3".equals(i_bus_type)) {
            prepareStatement.setLong(22, i_actual_balance)
          } else if ("1".equals(i_bus_type)) {
            prepareStatement.setLong(23, i_actual_balance)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(24, i_actual_balance)
          } else if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(25, i_actual_balance)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //i_actual_balance或m_sum_power或i_power_balance或m_service_balance或d_time_s或d_time_e修改
        else if ((old_i_actual_b != null || old_m_sum_p != null || old_i_power_b != null || old_m_service_b != null ||
          old_d_time_s != null || old_d_time_e != null)
          && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var power_balance = i_power_balance
          var sum_power = m_sum_power
          var service_balance = m_service_balance
          var actual_balance = i_actual_balance
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_m_service_b != null) { service_balance = old_m_service_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          val diff_sum_power = m_sum_power - sum_power
          val diff_service_balance = m_service_balance - service_balance
          val diff_power_balance = i_power_balance - power_balance
          val diff_actual_balance = i_actual_balance - actual_balance
          val diff_diffT = diffT - old_diffT

          val t_all_sql_diff = new StringBuffer("update t_all_table set")
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
            .append(" where l_seller_id=?")
          prepareStatement = connect.prepareStatement(t_all_sql_diff.toString())
          prepareStatement.setInt(26, l_seller_id)
          for (m <- 1 to 25) {
            prepareStatement.setLong(m, 0L)
          }
          prepareStatement.setLong(1, diff_sum_power)
          prepareStatement.setLong(6, diff_diffT)
          prepareStatement.setLong(17, diff_actual_balance)
          prepareStatement.setLong(20, diff_power_balance)
          prepareStatement.setLong(21, diff_service_balance)
          //车队个人类型统计
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(8, diff_diffT)
            prepareStatement.setLong(19, diff_actual_balance)
          } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, diff_sum_power)
            prepareStatement.setLong(7, diff_diffT)
            prepareStatement.setLong(18, diff_actual_balance)
          }
          //业务类型统计-减少旧数据
          if ("3".equals(i_bus_type)) {
            prepareStatement.setLong(22, diff_actual_balance)
          } else if ("1".equals(i_bus_type)) {
            prepareStatement.setLong(23, diff_actual_balance)
          } else if ("4".equals(i_bus_type) || "5".equals(i_bus_type)) {
            prepareStatement.setLong(24, diff_actual_balance)
          } else if ("2".equals(i_bus_type)) {
            prepareStatement.setLong(25, diff_actual_balance)
          }
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
/*****************************************************************************************************/
      //更新station_show_day_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
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
              prepareStatement.setLong(10, is_newCar_day_z_o)
              prepareStatement.setLong(22, is_newCar_day_z_o)
              prepareStatement.setLong(11, 1)
              prepareStatement.setLong(23, 1)
              prepareStatement.setLong(12, is_newCus_month_z_o)
              prepareStatement.setLong(24, is_newCus_month_z_o)
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
            if (!"isdelete".equals(carType) && "GongJiao".equals(carType)) {
              prepareStatement.setLong(8, is_newCar_day_z_o)
              prepareStatement.setLong(20, is_newCar_day_z_o)

              prepareStatement.setLong(9, 0)
              prepareStatement.setLong(21, 0)
            } else {
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(20, 0)

              prepareStatement.setLong(9, is_newCar_day_z_o)
              prepareStatement.setLong(21, is_newCar_day_z_o)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            var sum_power = m_sum_power
            var actual_balance = i_actual_balance
            var service_balance = m_service_balance
            var isoday = dayd
            var carT = carType
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (old_d_time_s != null) { isoday = old_dayd }
            if (old_v_vin != null) { carT = old_carType }
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
            prepareStatement = connect.prepareStatement(station_show_day_sql.toString)
            prepareStatement.setLong(10, l_station_id)
            prepareStatement.setInt(11, l_seller_id)
            prepareStatement.setString(12, isoday)
            prepareStatement.setLong(1, sum_power)
            prepareStatement.setLong(3, actual_balance)
            prepareStatement.setLong(4, 1)
            //车队统计
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(7, is_oldCar_day_o_z)
              prepareStatement.setLong(8, 1)
              prepareStatement.setLong(9, is_oldCus_day_o_z)
            } else {
              prepareStatement.setLong(2, 0)
              prepareStatement.setLong(7, 0)
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(9, 0)
            }
            //车类型统计
            if (!"isdelete".equals(carT) && "GongJiao".equals(carT)) {
              prepareStatement.setLong(5, is_oldCar_day_o_z)

              prepareStatement.setLong(6, 0)
            } else {
              prepareStatement.setLong(5, 0)

              prepareStatement.setLong(6, is_oldCar_day_o_z)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
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
              prepareStatement.setLong(10, is_newCar_day_z_o)
              prepareStatement.setLong(22, is_newCar_day_z_o)
              prepareStatement.setLong(11, 1)
              prepareStatement.setLong(23, 1)
              prepareStatement.setLong(12, is_newCus_month_z_o)
              prepareStatement.setLong(24, is_newCus_month_z_o)
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
            if (!"isdelete".equals(carType) && "GongJiao".equals(carType)) {
              prepareStatement.setLong(8, is_newCar_day_z_o)
              prepareStatement.setLong(20, is_newCar_day_z_o)

              prepareStatement.setLong(9, 0)
              prepareStatement.setLong(21, 0)
            } else {
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(20, 0)

              prepareStatement.setLong(9, is_newCar_day_z_o)
              prepareStatement.setLong(21, is_newCar_day_z_o)
            }
            prepareStatement.execute()
            prepareStatement.close()

          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            var sum_power = m_sum_power
            var actual_balance = i_actual_balance
            var service_balance = m_service_balance
            var isoday = dayd
            var carT = carType
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_m_service_b != null) { service_balance = old_m_service_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (old_d_time_s != null) { isoday = old_dayd }
            if (old_v_vin != null) { carT = old_carType }
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
            prepareStatement.setString(12, isoday)
            prepareStatement.setLong(1, sum_power)
            prepareStatement.setLong(3, actual_balance)
            prepareStatement.setLong(4, 1)
            //车队统计
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(7, is_oldCar_day_o_z)
              prepareStatement.setLong(8, 1)
              prepareStatement.setLong(9, is_oldCus_day_o_z)
            } else {
              prepareStatement.setLong(2, 0)
              prepareStatement.setLong(7, 0)
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(9, 0)
            }
            //车类型统计
            if (!"isdelete".equals(carT) && "GongJiao".equals(carT)) {
              prepareStatement.setLong(5, is_oldCar_day_o_z)

              prepareStatement.setLong(6, 0)
            } else {
              prepareStatement.setLong(5, 0)

              prepareStatement.setLong(6, is_oldCar_day_o_z)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //d_time_s修改操作
        else if (old_d_time_s != null && !dayd.equals(old_dayd) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          //增加新日期数据
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
            prepareStatement.setLong(10, is_newCar_day_z_o)
            prepareStatement.setLong(22, is_newCar_day_z_o)
            prepareStatement.setLong(11, 1)
            prepareStatement.setLong(23, 1)
            prepareStatement.setLong(12, is_newCus_day_z_o)
            prepareStatement.setLong(24, is_newCus_day_z_o)
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
          if (!"isdelete".equals(carType) && "GongJiao".equals(carType)) {
            prepareStatement.setLong(8, is_newCar_day_z_o)
            prepareStatement.setLong(20, is_newCar_day_z_o)

            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(21, 0)
          } else {
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(20, 0)

            prepareStatement.setLong(9, is_newCar_day_z_o)
            prepareStatement.setLong(21, is_newCar_day_z_o)
          }
          prepareStatement.execute()
          prepareStatement.close()

          //减少旧日期数据
          var sum_power = m_sum_power
          var actual_balance = i_actual_balance
          var service_balance = m_service_balance
          var isoday = dayd
          var carT = carType
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_m_service_b != null) { service_balance = old_m_service_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          if (old_d_time_s != null) { isoday = old_dayd }
          if (old_v_vin != null) { carT = old_carType }
          val station_show_day_sql1 = new StringBuffer("update station_show_day_table set")
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
          prepareStatement = connect.prepareStatement(station_show_day_sql1.toString())
          prepareStatement.setLong(10, l_station_id)
          prepareStatement.setInt(11, l_seller_id)
          prepareStatement.setString(12, old_dayd)
          prepareStatement.setLong(1, sum_power)
          prepareStatement.setLong(3, actual_balance)
          prepareStatement.setLong(4, 1)
          //车队统计
          if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, sum_power)
            prepareStatement.setLong(7, is_oldCar_day_o_z)
            prepareStatement.setLong(8, 1)
            prepareStatement.setLong(9, is_oldCus_day_o_z)
          } else {
            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(9, 0)
          }
          //车类型统计
          if (!"isdelete".equals(carT) && "GongJiao".equals(carT)) {
            prepareStatement.setLong(5, is_oldCar_day_o_z)

            prepareStatement.setLong(6, 0)
          } else {
            prepareStatement.setLong(5, 0)

            prepareStatement.setLong(6, is_oldCar_day_o_z)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //v_vin修改操作
        else if (old_v_vin != null && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var sum_power = m_sum_power
          var actual_balance = i_actual_balance
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          val diffold_isnewCar = is_newCar_day_z_o - is_oldCar_day_o_z
          val diff_sum_power = m_sum_power - sum_power
          val diff_actual_balance = i_actual_balance - actual_balance
          val station_show_day_sql1 = new StringBuffer("update station_show_day_table set")
            .append(" day_station_power_count=day_station_power_count+?,") //16
            .append(" day_station_team_power_count=day_station_team_power_count+?,")
            .append(" day_station_balance_count=day_station_balance_count+?,")
            .append(" day_station_pile_count=day_station_pile_count+?,")
            .append(" day_station_service_bus_car_num=day_station_service_bus_car_num+?,") //20
            .append(" day_station_service_other_car_num=day_station_service_other_car_num+?,")
            .append(" day_station_service_team_car_num=day_station_service_team_car_num+?,")
            .append(" day_station_service_pile_team_count=day_station_service_pile_team_count+?,")
            .append(" day_station_service_team_num_count=day_station_service_team_num_count+?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(station_show_day_sql1.toString())
          prepareStatement.setLong(10, l_station_id)
          prepareStatement.setInt(11, l_seller_id)
          prepareStatement.setString(12, dayd)
          for (m <- 1 to 9) {
            prepareStatement.setLong(m, 0L)
          }
          prepareStatement.setLong(1, diff_sum_power)
          prepareStatement.setLong(3, diff_actual_balance)
          prepareStatement.setLong(7, diffold_isnewCar)
          //车队统计
          if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, diff_sum_power)
          } else {
            prepareStatement.setLong(2, 0)
          }
          //修改车辆数据
          if (!"isdelete".equals(carType) && "GongJiao".equals(carType)) {
            prepareStatement.setLong(5, diffold_isnewCar)

            prepareStatement.setLong(6, 0)
          } else {
            prepareStatement.setLong(5, 0)

            prepareStatement.setLong(6, diffold_isnewCar)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //m_sum_power、i_actual_balance修改操作
        else if ((old_m_sum_p != null || old_i_actual_b != null) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var sum_power = m_sum_power
          var actual_balance = i_actual_balance
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          val diff_sum_power = m_sum_power - sum_power
          val diff_actual_balance = i_actual_balance - actual_balance

          val station_show_day_sql1 = new StringBuffer("update station_show_day_table set")
            .append(" day_station_power_count=day_station_power_count+?,") //16
            .append(" day_station_team_power_count=day_station_team_power_count+?,")
            .append(" day_station_balance_count=day_station_balance_count+?,")
            .append(" day_station_pile_count=day_station_pile_count+?,")
            .append(" day_station_service_bus_car_num=day_station_service_bus_car_num+?,") //20
            .append(" day_station_service_other_car_num=day_station_service_other_car_num+?,")
            .append(" day_station_service_team_car_num=day_station_service_team_car_num+?,")
            .append(" day_station_service_pile_team_count=day_station_service_pile_team_count+?,")
            .append(" day_station_service_team_num_count=day_station_service_team_num_count+?")
            .append(" where l_station_id=? and l_seller_id=? and d_day_time=?")
          prepareStatement = connect.prepareStatement(station_show_day_sql1.toString())
          prepareStatement.setLong(10, l_station_id)
          prepareStatement.setInt(11, l_seller_id)
          prepareStatement.setString(12, dayd)
          for (m <- 1 to 9) {
            prepareStatement.setLong(m, 0L)
          }
          prepareStatement.setLong(1, diff_sum_power)
          prepareStatement.setLong(3, diff_actual_balance)
          //车队统计
          if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, diff_sum_power)
          } else {
            prepareStatement.setLong(2, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
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
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_day_table**关闭prepareStatement失败****")
        }
      }
/************************************************************************************************/
      //更新station_show_member_balance_table
      try {
        connect = DruidUtil.getConnection.get
        var actual_balance = i_actual_balance
        if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
            val member_balance_sql = new StringBuffer("insert into station_show_member_balance_table values(?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" total_balance_count=total_balance_count+?")
            prepareStatement = connect.prepareStatement(member_balance_sql.toString())
            prepareStatement.setLong(1, i_member_id.toLong)
            prepareStatement.setLong(2, l_station_id)
            prepareStatement.setLong(3, i_actual_balance)
            prepareStatement.setLong(4, i_actual_balance)
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            val member_balance_sql = new StringBuffer("update station_show_member_balance_table set")
              .append(" total_balance_count=total_balance_count-?")
              .append(" where l_member_id=? and l_station_id=?")
            prepareStatement = connect.prepareStatement(member_balance_sql.toString())
            prepareStatement.setLong(2, i_member_id.toLong)
            prepareStatement.setLong(3, l_station_id)
            prepareStatement.setLong(1, actual_balance)
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
            val member_balance_sql = new StringBuffer("insert into station_show_member_balance_table values(?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" total_balance_count=total_balance_count+?")
            prepareStatement = connect.prepareStatement(member_balance_sql.toString())
            prepareStatement.setLong(1, i_member_id.toLong)
            prepareStatement.setLong(2, l_station_id)
            prepareStatement.setLong(3, i_actual_balance)
            prepareStatement.setLong(4, i_actual_balance)
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            val member_balance_sql = new StringBuffer("update station_show_member_balance_table set")
              .append(" total_balance_count=total_balance_count-?")
              .append(" where l_member_id=? and l_station_id=?")
            prepareStatement = connect.prepareStatement(member_balance_sql.toString())
            prepareStatement.setLong(2, i_member_id.toLong)
            prepareStatement.setLong(3, l_station_id)
            prepareStatement.setLong(1, actual_balance)
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改i_actual_balance
        else if (old_i_actual_b != null) {
          val diff_actual_balance = i_actual_balance - old_i_actual_balance
          val member_balance_sql = new StringBuffer("update station_show_member_balance_table set")
            .append(" total_balance_count=total_balance_count+?")
            .append(" where l_member_id=? and l_station_id=?")
          prepareStatement = connect.prepareStatement(member_balance_sql.toString())
          prepareStatement.setLong(2, i_member_id.toLong)
          prepareStatement.setLong(3, l_station_id)
          prepareStatement.setLong(1, diff_actual_balance)
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
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
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_member_balance_table**关闭prepareStatement失败****")
        }
      }
/*****************************************************************************************************/
      //更新station_show_month_member_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
            val month_member_sql = new StringBuffer("insert into station_show_month_member_table values(?,?,?,?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_member_charge_count=month_member_charge_count+?,") //11
              .append(" month_team_power_count=month_team_power_count+?,")
              .append(" month_team_balance_count=month_team_balance_count+?,")
              .append(" month_team_power_balance_count=month_team_power_balance_count+?")

            prepareStatement = connect.prepareStatement(month_member_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            prepareStatement.setString(3, dayd_month)
            prepareStatement.setLong(4, i_member_id.toLong)
            prepareStatement.setString(5, "")
            prepareStatement.setLong(6, s_member_flag.toInt)
            prepareStatement.setLong(11, 1)
            prepareStatement.setLong(7, 1)
            prepareStatement.setLong(9, i_actual_balance)
            prepareStatement.setLong(13, i_actual_balance)
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(8, m_sum_power)
              prepareStatement.setLong(12, m_sum_power)
              prepareStatement.setLong(10, i_power_balance)
              prepareStatement.setLong(14, i_power_balance)
            } else {
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(12, 0)
              prepareStatement.setLong(10, 0)
              prepareStatement.setLong(14, 0)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            var dayd_m = dayd_month
            var actual_balance = i_actual_balance
            var power_balance = i_power_balance
            var sum_power = m_sum_power
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (old_d_time_s != null) { dayd_m = old_dayd_month }
            val month_member_sql = new StringBuffer("update station_show_month_member_table set")
              .append(" month_member_charge_count=month_member_charge_count-?,") //12
              .append(" month_team_power_count=month_team_power_count-?,")
              .append(" month_team_balance_count=month_team_balance_count-?,")
              .append(" month_team_power_balance_count=month_team_power_balance_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=?")

            prepareStatement = connect.prepareStatement(month_member_sql.toString())
            prepareStatement.setLong(5, l_station_id)
            prepareStatement.setInt(6, l_seller_id)
            prepareStatement.setString(7, dayd_m)
            prepareStatement.setLong(8, i_member_id.toLong)
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(3, actual_balance)
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(4, power_balance)
            } else {
              prepareStatement.setLong(2, 0)
              prepareStatement.setLong(4, 0)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
            val month_member_sql = new StringBuffer("insert into station_show_month_member_table values(?,?,?,?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_member_charge_count=month_member_charge_count+?,") //11
              .append(" month_team_power_count=month_team_power_count+?,")
              .append(" month_team_balance_count=month_team_balance_count+?,")
              .append(" month_team_power_balance_count=month_team_power_balance_count+?")

            prepareStatement = connect.prepareStatement(month_member_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            prepareStatement.setString(3, dayd_month)
            prepareStatement.setLong(4, i_member_id.toLong)
            prepareStatement.setString(5, "")
            prepareStatement.setLong(6, s_member_flag.toInt)
            prepareStatement.setLong(11, 1)
            prepareStatement.setLong(7, 1)
            prepareStatement.setLong(9, i_actual_balance)
            prepareStatement.setLong(13, i_actual_balance)
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(8, m_sum_power)
              prepareStatement.setLong(12, m_sum_power)
              prepareStatement.setLong(10, i_power_balance)
              prepareStatement.setLong(14, i_power_balance)
            } else {
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(12, 0)
              prepareStatement.setLong(10, 0)
              prepareStatement.setLong(14, 0)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            var dayd_m = dayd_month
            var actual_balance = i_actual_balance
            var power_balance = i_power_balance
            var sum_power = m_sum_power
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (old_d_time_s != null) { dayd_m = old_dayd_month }
            val month_member_sql = new StringBuffer("update station_show_month_member_table set")
              .append(" month_member_charge_count=month_member_charge_count-?,") //12
              .append(" month_team_power_count=month_team_power_count-?,")
              .append(" month_team_balance_count=month_team_balance_count-?,")
              .append(" month_team_power_balance_count=month_team_power_balance_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=?")

            prepareStatement = connect.prepareStatement(month_member_sql.toString())
            prepareStatement.setLong(5, l_station_id)
            prepareStatement.setInt(6, l_seller_id)
            prepareStatement.setString(7, dayd_m)
            prepareStatement.setLong(8, i_member_id.toLong)
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(3, actual_balance)
            if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(2, sum_power)
              prepareStatement.setLong(4, power_balance)
            } else {
              prepareStatement.setLong(2, 0)
              prepareStatement.setLong(4, 0)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //d_time_s修改操作
        else if (old_d_time_s != null && !dayd_month.equals(old_dayd_month) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var actual_balance = i_actual_balance
          var power_balance = i_power_balance
          var sum_power = m_sum_power
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          //减少旧日期数据
          val month_member_sql = new StringBuffer("update station_show_month_member_table set")
            .append(" month_member_charge_count=month_member_charge_count-?,") //12
            .append(" month_team_power_count=month_team_power_count-?,")
            .append(" month_team_balance_count=month_team_balance_count-?,")
            .append(" month_team_power_balance_count=month_team_power_balance_count-?")
            .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=?")

          prepareStatement = connect.prepareStatement(month_member_sql.toString())
          prepareStatement.setLong(5, l_station_id)
          prepareStatement.setInt(6, l_seller_id)
          prepareStatement.setString(7, old_dayd_month)
          prepareStatement.setLong(8, i_member_id.toLong)
          prepareStatement.setLong(1, 1)
          prepareStatement.setLong(3, actual_balance)
          if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, sum_power)
            prepareStatement.setLong(4, power_balance)
          } else {
            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(4, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
          //增加新日期数据
          val month_member_sql1 = new StringBuffer("insert into station_show_month_member_table values(?,?,?,?,?,?,?,?,?,?)")
            .append(" ON DUPLICATE KEY UPDATE")
            .append(" month_member_charge_count=month_member_charge_count+?,") //11
            .append(" month_team_power_count=month_team_power_count+?,")
            .append(" month_team_balance_count=month_team_balance_count+?,")
            .append(" month_team_power_balance_count=month_team_power_balance_count+?")

          prepareStatement = connect.prepareStatement(month_member_sql1.toString())
          prepareStatement.setLong(1, l_station_id)
          prepareStatement.setInt(2, l_seller_id)
          prepareStatement.setString(3, dayd_month)
          prepareStatement.setLong(4, i_member_id.toLong)
          prepareStatement.setString(5, "")
          prepareStatement.setLong(6, s_member_flag.toInt)
          prepareStatement.setLong(11, 1)
          prepareStatement.setLong(7, 1)
          prepareStatement.setLong(9, i_actual_balance)
          prepareStatement.setLong(13, i_actual_balance)
          if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(8, m_sum_power)
            prepareStatement.setLong(12, m_sum_power)
            prepareStatement.setLong(10, i_power_balance)
            prepareStatement.setLong(14, i_power_balance)
          } else {
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(12, 0)
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(14, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改i_actual_balance或m_sum_power或i_power_balance
        else if ((old_m_sum_p != null || old_i_actual_b != null || old_i_power_b != null) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var actual_balance = i_actual_balance
          var power_balance = i_power_balance
          var sum_power = m_sum_power
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          val diff_actual_balance = i_actual_balance - actual_balance
          val diff_power_balance = i_power_balance - power_balance
          val diff_sum_power = m_sum_power - sum_power
          val month_member_sql = new StringBuffer("update station_show_month_member_table set")
            .append(" month_member_charge_count=month_member_charge_count+?,") //12
            .append(" month_team_power_count=month_team_power_count+?,")
            .append(" month_team_balance_count=month_team_balance_count+?,")
            .append(" month_team_power_balance_count=month_team_power_balance_count+?")
            .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=?")

          prepareStatement = connect.prepareStatement(month_member_sql.toString())
          prepareStatement.setLong(5, l_station_id)
          prepareStatement.setInt(6, l_seller_id)
          prepareStatement.setString(7, dayd_month)
          prepareStatement.setLong(8, i_member_id.toLong)
          prepareStatement.setLong(1, s_member_flag.toInt)
          prepareStatement.setLong(3, diff_actual_balance)
          if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
            prepareStatement.setLong(2, diff_sum_power)
            prepareStatement.setLong(4, diff_power_balance)
          } else {
            prepareStatement.setLong(2, 0)
            prepareStatement.setLong(4, 0)
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
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
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_member_table**关闭prepareStatement失败****")
        }
      }

/*****************************************************************************************************/
      //更新station_show_month_team_balance_table
      try {
        if (!StringUtil.isBlank(v_vin)
          && (StringUtil.isBlank(s_member_flag) || "2".equals(s_member_flag))) {
          connect = DruidUtil.getConnection.get
          //修改b_delete_flag操作1到0
          if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
            if ("1".equals(b_finish)) {
              val month_member_sql = new StringBuffer("insert into station_show_month_team_balance_table values(?,?,?,?,?,?,?)")
                .append(" ON DUPLICATE KEY UPDATE")
                .append(" month_team_actual_balance_count=month_team_actual_balance_count+?")

              prepareStatement = connect.prepareStatement(month_member_sql.toString())
              prepareStatement.setLong(1, l_station_id)
              prepareStatement.setInt(2, l_seller_id)
              prepareStatement.setString(3, dayd)
              prepareStatement.setString(4, v_vin)
              prepareStatement.setString(5, carNo)
              prepareStatement.setString(6, i_member_id)
              prepareStatement.setLong(7, i_actual_balance)
              prepareStatement.setLong(8, i_actual_balance)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //修改b_delete_flag操作0到1
          else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
            if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
              var dayd_m = dayd_month
              var actual_balance = i_actual_balance

              if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
              if (old_d_time_s != null) { dayd_m = old_dayd_month }
              val month_member_sql = new StringBuffer("update station_show_month_team_balance_table set")
                .append(" month_team_actual_balance_count=month_team_actual_balance_count-?")
                .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=? and v_vin=?")
              prepareStatement = connect.prepareStatement(month_member_sql.toString())
              prepareStatement.setLong(1, actual_balance)
              prepareStatement.setLong(2, l_station_id)
              prepareStatement.setInt(3, l_seller_id)
              prepareStatement.setString(4, dayd)
              prepareStatement.setLong(5, i_member_id.toLong)
              prepareStatement.setString(6, v_vin)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //修改b_finish操作0到1
          else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
            if ("0".equals(b_delete_flag)) {
              val month_member_sql = new StringBuffer("insert into station_show_month_team_balance_table values(?,?,?,?,?,?,?)")
                .append(" ON DUPLICATE KEY UPDATE")
                .append(" month_team_actual_balance_count=month_team_actual_balance_count+?")

              prepareStatement = connect.prepareStatement(month_member_sql.toString())
              prepareStatement.setLong(1, l_station_id)
              prepareStatement.setInt(2, l_seller_id)
              prepareStatement.setString(3, dayd)
              prepareStatement.setString(4, v_vin)
              prepareStatement.setString(5, carNo)
              prepareStatement.setString(6, i_member_id)
              prepareStatement.setLong(7, i_actual_balance)
              prepareStatement.setLong(8, i_actual_balance)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //修改b_finish操作1到0
          else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
            if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
              var dayd_m = dayd_month
              var actual_balance = i_actual_balance

              if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
              if (old_d_time_s != null) { dayd_m = old_dayd_month }
              val month_member_sql = new StringBuffer("update station_show_month_team_balance_table set")
                .append(" month_team_actual_balance_count=month_team_actual_balance_count-?")
                .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=? and v_vin=?")
              prepareStatement = connect.prepareStatement(month_member_sql.toString())
              prepareStatement.setLong(1, actual_balance)
              prepareStatement.setLong(2, l_station_id)
              prepareStatement.setInt(3, l_seller_id)
              prepareStatement.setString(4, dayd)
              prepareStatement.setLong(5, i_member_id.toLong)
              prepareStatement.setString(6, v_vin)
              prepareStatement.execute()
              prepareStatement.close()
            }
          } //d_time_s修改操作
          else if (old_d_time_s != null && !dayd_month.equals(old_dayd_month) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
            var actual_balance = i_actual_balance
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            //减少旧日期数据
            val month_member_sql = new StringBuffer("update station_show_month_team_balance_table set")
              .append(" month_team_actual_balance_count=month_team_actual_balance_count-?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=? and v_vin=?")
            prepareStatement = connect.prepareStatement(month_member_sql.toString())
            prepareStatement.setLong(1, actual_balance)
            prepareStatement.setLong(2, l_station_id)
            prepareStatement.setInt(3, l_seller_id)
            prepareStatement.setString(4, old_dayd_month)
            prepareStatement.setLong(5, i_member_id.toLong)
            prepareStatement.setString(6, v_vin)
            prepareStatement.execute()
            prepareStatement.close()

            //增加新日期数据
            val month_member_sql1 = new StringBuffer("insert into station_show_month_team_balance_table values(?,?,?,?,?,?,?)")
              .append(" ON DUPLICATE KEY UPDATE")
              .append(" month_team_actual_balance_count=month_team_actual_balance_count+?")

            prepareStatement = connect.prepareStatement(month_member_sql1.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            prepareStatement.setString(3, dayd)
            prepareStatement.setString(4, v_vin)
            prepareStatement.setString(5, carNo)
            prepareStatement.setString(6, i_member_id)
            prepareStatement.setLong(7, i_actual_balance)
            prepareStatement.setLong(8, i_actual_balance)
            prepareStatement.execute()
            prepareStatement.close()
          } //修改i_actual_balance或m_sum_power或i_power_balance
          else if (old_i_actual_b != null && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
            var actual_balance = i_actual_balance
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            val diff_actual_balance = i_actual_balance - actual_balance
            val month_member_sql = new StringBuffer("update station_show_month_team_balance_table set")
              .append(" month_team_actual_balance_count=month_team_actual_balance_count+?")
              .append(" where l_station_id=? and l_seller_id=? and d_month_time=? and l_member_id=? and v_vin=?")

            prepareStatement = connect.prepareStatement(month_member_sql.toString())
            prepareStatement.setLong(2, l_station_id)
            prepareStatement.setInt(3, l_seller_id)
            prepareStatement.setString(4, dayd_month)
            prepareStatement.setLong(5, i_member_id.toLong)
            prepareStatement.setString(6, v_vin)
            prepareStatement.setLong(1, diff_actual_balance)
            prepareStatement.execute()
            prepareStatement.close()
          }
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
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_team_balance_table**关闭prepareStatement失败****")
        }
      }

/******************************************************************************************/
      //更新station_show_month_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
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
            prepareStatement = connect.prepareStatement(month_total_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            prepareStatement.setString(3, dayd_month)
            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(4, m_sum_power)
            prepareStatement.setLong(36, m_sum_power)
            prepareStatement.setLong(5, i_actual_balance)
            prepareStatement.setLong(37, i_actual_balance)
            prepareStatement.setLong(6, 1)
            prepareStatement.setLong(38, 1)
            //是否参与活动
            if (i_aiscount_balance > 0 || b_feesingle.toInt > 0 || i_deductible_balance > 0 || b_fee_service.toInt > 0) {
              prepareStatement.setLong(10, 1)
              prepareStatement.setLong(41, 1)
            } else {
              prepareStatement.setLong(10, 0)
              prepareStatement.setLong(41, 0)
            }
            //是否个人会员
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(7, 1)
              prepareStatement.setLong(39, 1)
              prepareStatement.setLong(8, is_newCus_month_z_o)
              prepareStatement.setLong(40, is_newCus_month_z_o)
              prepareStatement.setLong(11, is_newgrouthC_month_z_o)
              prepareStatement.setLong(42, is_newgrouthC_month_z_o)
            } else {
              prepareStatement.setLong(7, 0)
              prepareStatement.setLong(39, 0)
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(40, 0)
              prepareStatement.setLong(11, 0)
              prepareStatement.setLong(42, 0)
            }
            //小时分类统计
            for (i <- 12 to 35) {
              if (i == 12 + dateHour) {
                prepareStatement.setLong(i, 1)
              } else {
                prepareStatement.setLong(i, 0)
              }
            }

            for (j <- 43 to 66) {
              if (j == 43 + dateHour) {
                prepareStatement.setLong(j, 1)
              } else {
                prepareStatement.setLong(j, 0)
              }
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            var dhour = dateHour
            var actual_balance = i_actual_balance
            var power_balance = i_power_balance
            var sum_power = m_sum_power
            var dayd_m = dayd_month
            var feesingle = b_feesingle
            var fee_service = b_fee_service
            var deductible_balance = i_deductible_balance
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_d_time_s != null) { dayd_m = old_dayd_month; dhour = old_dateHour }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (old_b_feesingle != null) { feesingle = old_b_feesingle }
            if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
            if (old_b_fee_s != null) { fee_service = old_b_fee_service }
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
            prepareStatement = connect.prepareStatement(month_total_sql.toString())
            prepareStatement.setLong(32, l_station_id)
            prepareStatement.setInt(33, l_seller_id)
            prepareStatement.setString(34, dayd_m)
            prepareStatement.setLong(1, sum_power)
            prepareStatement.setLong(2, actual_balance)
            prepareStatement.setLong(3, 1)
            //是否参与活动
            if (i_aiscount_b > 0 || feesingle.toInt > 0 || deductible_balance > 0
              || fee_service.toInt > 0) {
              prepareStatement.setLong(6, 1)
            } else {
              prepareStatement.setLong(6, 0)
            }
            //是否个人会员
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(4, 1)
              prepareStatement.setLong(5, is_oldCus_month_o_z)
              prepareStatement.setLong(7, is_oldgrouthC_month_o_z)
            } else {
              prepareStatement.setLong(4, 0)
              prepareStatement.setLong(5, 0)
              prepareStatement.setLong(7, 0)
            }
            //小时分类统计
            for (j <- 8 to 31) {
              if (j == 8 + dhour) {
                prepareStatement.setLong(j, 1)
              } else {
                prepareStatement.setLong(j, 0)
              }
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
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
            prepareStatement = connect.prepareStatement(month_total_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            prepareStatement.setString(3, dayd_month)
            prepareStatement.setLong(9, 0)
            prepareStatement.setLong(4, m_sum_power)
            prepareStatement.setLong(36, m_sum_power)
            prepareStatement.setLong(5, i_actual_balance)
            prepareStatement.setLong(37, i_actual_balance)
            prepareStatement.setLong(6, 1)
            prepareStatement.setLong(38, 1)
            //是否参与活动
            if (i_aiscount_balance > 0 || b_feesingle.toInt > 0 || i_deductible_balance > 0 || b_fee_service.toInt > 0) {
              prepareStatement.setLong(10, 1)
              prepareStatement.setLong(41, 1)
            } else {
              prepareStatement.setLong(10, 0)
              prepareStatement.setLong(41, 0)
            }
            //是否个人会员
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(7, 1)
              prepareStatement.setLong(39, 1)
              prepareStatement.setLong(8, is_newCus_month_z_o)
              prepareStatement.setLong(40, is_newCus_month_z_o)
              prepareStatement.setLong(11, is_newgrouthC_month_z_o)
              prepareStatement.setLong(42, is_newgrouthC_month_z_o)
            } else {
              prepareStatement.setLong(7, 0)
              prepareStatement.setLong(39, 0)
              prepareStatement.setLong(8, 0)
              prepareStatement.setLong(40, 0)
              prepareStatement.setLong(11, 0)
              prepareStatement.setLong(42, 0)
            }
            //小时分类统计
            for (i <- 12 to 35) {
              if (i == 12 + dateHour) {
                prepareStatement.setLong(i, 1)
              } else {
                prepareStatement.setLong(i, 0)
              }
            }

            for (j <- 43 to 66) {
              if (j == 43 + dateHour) {
                prepareStatement.setLong(j, 1)
              } else {
                prepareStatement.setLong(j, 0)
              }
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            var dhour = dateHour
            var actual_balance = i_actual_balance
            var power_balance = i_power_balance
            var sum_power = m_sum_power
            var dayd_m = dayd_month
            var feesingle = b_feesingle
            var fee_service = b_fee_service
            var deductible_balance = i_deductible_balance
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_d_time_s != null) { dayd_m = old_dayd_month; dhour = old_dateHour }
            if (old_i_power_b != null) { power_balance = old_i_power_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
            if (old_b_feesingle != null) { feesingle = old_b_feesingle }
            if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
            if (old_b_fee_s != null) { fee_service = old_b_fee_service }
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
            prepareStatement = connect.prepareStatement(month_total_sql.toString())
            prepareStatement.setLong(32, l_station_id)
            prepareStatement.setInt(33, l_seller_id)
            prepareStatement.setString(34, dayd_m)
            prepareStatement.setLong(1, sum_power)
            prepareStatement.setLong(2, actual_balance)
            prepareStatement.setLong(3, 1)
            //是否参与活动
            if (i_aiscount_b > 0 || feesingle.toInt > 0 || deductible_balance > 0
              || fee_service.toInt > 0) {
              prepareStatement.setLong(6, 1)
            } else {
              prepareStatement.setLong(6, 0)
            }
            //是否个人会员
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(4, 1)
              prepareStatement.setLong(5, is_oldCus_month_o_z)
              prepareStatement.setLong(7, is_oldgrouthC_month_o_z)
            } else {
              prepareStatement.setLong(4, 0)
              prepareStatement.setLong(5, 0)
              prepareStatement.setLong(7, 0)
            }
            //小时分类统计
            for (j <- 8 to 31) {
              if (j == 8 + dhour) {
                prepareStatement.setLong(j, 1)
              } else {
                prepareStatement.setLong(j, 0)
              }
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //d_time_s修改操作
        else if (old_d_time_s != null && !dayd_month.equals(old_dayd_month) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          //减少旧月份数据
          var actual_balance = i_actual_balance
          var power_balance = i_power_balance
          var sum_power = m_sum_power
          var feesingle = b_feesingle
          val timeDiff = TimeConUtil.timeDiff(old_d_time_s, d_time_s)
          var fee_service = b_fee_service
          var deductible_balance = i_deductible_balance
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_i_power_b != null) { power_balance = old_i_power_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          if (old_b_feesingle != null) { feesingle = old_b_feesingle }
          if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
          if (old_b_fee_s != null) { fee_service = old_b_fee_service }
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
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setLong(32, l_station_id)
          prepareStatement.setInt(33, l_seller_id)
          prepareStatement.setString(34, old_dayd_month)
          prepareStatement.setLong(1, sum_power)
          prepareStatement.setLong(2, actual_balance)
          prepareStatement.setLong(3, 1)
          //是否参与活动
          if (i_aiscount_b > 0 || feesingle.toInt > 0 || deductible_balance > 0
            || fee_service.toInt > 0) {
            prepareStatement.setLong(6, 1)
          } else {
            prepareStatement.setLong(6, 0)
          }
          //是否个人会员
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(4, 1)
            prepareStatement.setLong(5, is_oldCus_month_o_z)
            if ("1".equals(timeDiff)) {
              prepareStatement.setLong(7, is_oldgrouthC_month_z_o)
            } else {
              prepareStatement.setLong(7, is_oldgrouthC_month_o_z)
            }
          } else {
            prepareStatement.setLong(4, 0)
            prepareStatement.setLong(5, 0)
            prepareStatement.setLong(7, 0)
          }
          //小时分类统计
          for (j <- 8 to 31) {
            if (j == 8 + old_dateHour) {
              prepareStatement.setLong(j, 1)
            } else {
              prepareStatement.setLong(j, 0)
            }
          }
          prepareStatement.execute()
          prepareStatement.close()
          //增加新月份数据
          val month_total_sql1 = new StringBuffer("insert into station_show_month_table values(")
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
          prepareStatement = connect.prepareStatement(month_total_sql1.toString())
          prepareStatement.setLong(1, l_station_id)
          prepareStatement.setInt(2, l_seller_id)
          prepareStatement.setString(3, dayd_month)
          prepareStatement.setLong(9, 0)
          prepareStatement.setLong(4, m_sum_power)
          prepareStatement.setLong(36, m_sum_power)
          prepareStatement.setLong(5, i_actual_balance)
          prepareStatement.setLong(37, i_actual_balance)
          prepareStatement.setLong(6, 1)
          prepareStatement.setLong(38, 1)
          //是否参与活动
          if (i_aiscount_balance > 0 || b_feesingle.toInt > 0 || i_deductible_balance > 0 || b_fee_service.toInt > 0) {
            prepareStatement.setLong(10, 1)
            prepareStatement.setLong(41, 1)
          } else {
            prepareStatement.setLong(10, 0)
            prepareStatement.setLong(41, 0)
          }
          //是否个人会员
          if ("1".equals(s_member_flag)) {
            prepareStatement.setLong(7, 1)
            prepareStatement.setLong(39, 1)
            prepareStatement.setLong(8, is_newCus_month_z_o)
            prepareStatement.setLong(40, is_newCus_month_z_o)
            prepareStatement.setLong(11, is_newgrouthC_month_z_o)
            prepareStatement.setLong(42, is_newgrouthC_month_z_o)
          } else {
            prepareStatement.setLong(7, 0)
            prepareStatement.setLong(39, 0)
            prepareStatement.setLong(8, 0)
            prepareStatement.setLong(40, 0)
            prepareStatement.setLong(11, 0)
            prepareStatement.setLong(42, 0)
          }
          //小时分类统计
          for (i <- 12 to 35) {
            if (i == 12 + dateHour) {
              prepareStatement.setLong(i, 1)
            } else {
              prepareStatement.setLong(i, 0)
            }
          }

          for (j <- 43 to 66) {
            if (j == 43 + dateHour) {
              prepareStatement.setLong(j, 1)
            } else {
              prepareStatement.setLong(j, 0)
            }
          }
          prepareStatement.execute()
          prepareStatement.close()
        } //修改i_actual_balance或m_sum_power或i_aiscount_balance或i_deductible_balance或b_fee_service或b_feesingle
        else if ((old_m_sum_p != null || old_i_actual_b != null
          || old_i_aiscount_b != null || old_i_deductible_b != null ||
          old_b_fee_service != null || old_b_feesingle != null) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var actual_balance = i_actual_balance
          var sum_power = m_sum_power
          var feesingle = b_feesingle
          var fee_service = b_fee_service
          var deductible_balance = i_deductible_balance
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          if (old_b_feesingle != null) { feesingle = old_b_feesingle }
          if (old_i_deductible_b != null) { deductible_balance = old_i_deductible_balance }
          if (old_b_fee_s != null) { fee_service = old_b_fee_service }
          val diff_actual_balance = i_actual_balance - actual_balance
          val diff_sum_power = m_sum_power - sum_power
          val isvisit = deductible_balance + i_aiscount_b + feesingle.toInt + fee_service.toInt
          val month_total_sql = new StringBuffer("update station_show_month_table set")
            .append(" month_i_power_count=month_i_power_count+?,") //36
            .append(" month_i_power_balance_count=month_i_power_balance_count+?,")
            .append(" month_personal_visit_num=month_personal_visit_num+?")
            .append(" where l_station_id=? and l_seller_id=? and d_month_time=?")
          prepareStatement = connect.prepareStatement(month_total_sql.toString())
          prepareStatement.setLong(4, l_station_id)
          prepareStatement.setInt(5, l_seller_id)
          prepareStatement.setString(6, dayd_month)
          prepareStatement.setLong(1, diff_sum_power)
          prepareStatement.setLong(2, diff_actual_balance)
          prepareStatement.setLong(3, 0)
          if (old_i_aiscount_b != null || old_i_deductible_b != null || old_b_feesingle != null || old_b_fee_service != null) {
            if ("0".equals(b_feesingle) && "0".equals(b_fee_service) && i_aiscount_balance == 0 && i_deductible_balance == 0) {
              prepareStatement.setLong(3, -1)
            } else if (isvisit == 0) {
              prepareStatement.setLong(3, 1)
            }
          }
          prepareStatement.execute()
          prepareStatement.close()
        }
        connect.close()
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
          if (prepareStatement != null && !prepareStatement.isClosed()) {
            prepareStatement.close()
          }
        } catch {
          case ex: Exception => log.info("***station_show_month_table**关闭prepareStatement失败****")
        }
      }
/*********************************************************************************************/
      //更新station_show_total_table
      try {
        connect = DruidUtil.getConnection.get
        //修改b_delete_flag操作1到0
        if (old_b_delete_flag != null && "1".equals(old_b_delete_flag) && "0".equals(b_delete_flag)) {
          if ("1".equals(b_finish)) {
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
            prepareStatement = connect.prepareStatement(station_show_total_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            for (m <- 3 to 40) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(26, 1)
            prepareStatement.setLong(18, m_sum_power)
            prepareStatement.setLong(39, m_sum_power)
            prepareStatement.setLong(19, i_actual_balance)
            prepareStatement.setLong(40, i_actual_balance)

            //个人车队分类
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(6, 1)
              prepareStatement.setLong(27, 1)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(7, is_newCus_total_z_o)
              prepareStatement.setLong(28, is_newCus_total_z_o)
              prepareStatement.setLong(8, m_sum_power)
              prepareStatement.setLong(29, m_sum_power)
              prepareStatement.setLong(9, 1)
              prepareStatement.setLong(30, 1)
              prepareStatement.setLong(10, i_actual_balance)
              prepareStatement.setLong(31, i_actual_balance)
              prepareStatement.setLong(11, is_newCar_total_z_o)
              prepareStatement.setLong(32, is_newCar_total_z_o)
              prepareStatement.setLong(12, diffT)
              prepareStatement.setLong(33, diffT)
            }
            //会员等级统计
            if (is_newCar_total_z_o == 1 && "1".equals(level)) {
              prepareStatement.setLong(13, 1)
              prepareStatement.setLong(34, 1)
            } else if (is_newCar_total_z_o == 1 && "2".equals(level)) {
              prepareStatement.setLong(14, 1)
              prepareStatement.setLong(35, 1)
            } else if (is_newCar_total_z_o == 1 && "3".equals(level)) {
              prepareStatement.setLong(15, 1)
              prepareStatement.setLong(36, 1)
            } else if (is_newCar_total_z_o == 1 && "4".equals(level)) {
              prepareStatement.setLong(16, 1)
              prepareStatement.setLong(37, 1)
            } else if (is_newCar_total_z_o == 1 && "5".equals(level)) {
              prepareStatement.setLong(17, 1)
              prepareStatement.setLong(38, 1)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_delete_flag操作0到1
        else if (old_b_delete_flag != null && "0".equals(old_b_delete_flag) && "1".equals(b_delete_flag)) {
          if ("1".equals(b_finish) || ("0".equals(b_finish) && "1".equals(old_b_finish))) {
            var actual_balance = i_actual_balance
            var sum_power = m_sum_power
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
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
            prepareStatement = connect.prepareStatement(station_show_total_sql.toString())
            prepareStatement.setLong(16, l_station_id)
            prepareStatement.setInt(17, l_seller_id)
            for (m <- 1 to 15) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(14, sum_power)
            prepareStatement.setLong(15, actual_balance)

            //个人车队分类
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(2, 1)

            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(3, is_newCus_total_o_z)
              prepareStatement.setLong(4, sum_power)
              prepareStatement.setLong(5, 1)
              prepareStatement.setLong(6, actual_balance)
              prepareStatement.setLong(7, is_oldCar_total_o_z)
              prepareStatement.setLong(8, diffT)

            }
            //会员等级统计
            if (is_oldCar_total_o_z == 1 && "1".equals(level)) {
              prepareStatement.setLong(9, 1)
            } else if (is_oldCar_total_o_z == 1 && "2".equals(level)) {
              prepareStatement.setLong(10, 1)
            } else if (is_oldCar_total_o_z == 1 && "3".equals(level)) {
              prepareStatement.setLong(11, 1)
            } else if (is_oldCar_total_o_z == 1 && "4".equals(level)) {
              prepareStatement.setLong(12, 1)
            } else if (is_oldCar_total_o_z == 1 && "5".equals(level)) {
              prepareStatement.setLong(13, 1)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作0到1
        else if (old_b_finish != null && "0".equals(old_b_finish) && "1".equals(b_finish)) {
          if ("0".equals(b_delete_flag)) {
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
            prepareStatement = connect.prepareStatement(station_show_total_sql.toString())
            prepareStatement.setLong(1, l_station_id)
            prepareStatement.setInt(2, l_seller_id)
            for (m <- 3 to 40) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(5, 1)
            prepareStatement.setLong(26, 1)
            prepareStatement.setLong(18, m_sum_power)
            prepareStatement.setLong(39, m_sum_power)
            prepareStatement.setLong(19, i_actual_balance)
            prepareStatement.setLong(40, i_actual_balance)

            //个人车队分类
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(6, 1)
              prepareStatement.setLong(27, 1)
            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(7, is_newCus_total_z_o)
              prepareStatement.setLong(28, is_newCus_total_z_o)
              prepareStatement.setLong(8, m_sum_power)
              prepareStatement.setLong(29, m_sum_power)
              prepareStatement.setLong(9, 1)
              prepareStatement.setLong(30, 1)
              prepareStatement.setLong(10, i_actual_balance)
              prepareStatement.setLong(31, i_actual_balance)
              prepareStatement.setLong(11, is_newCar_total_z_o)
              prepareStatement.setLong(32, is_newCar_total_z_o)
              prepareStatement.setLong(12, diffT)
              prepareStatement.setLong(33, diffT)
            }
            //会员等级统计
            if (is_newCar_total_z_o == 1 && "1".equals(level)) {
              prepareStatement.setLong(13, 1)
              prepareStatement.setLong(34, 1)
            } else if (is_newCar_total_z_o == 1 && "2".equals(level)) {
              prepareStatement.setLong(14, 1)
              prepareStatement.setLong(35, 1)
            } else if (is_newCar_total_z_o == 1 && "3".equals(level)) {
              prepareStatement.setLong(15, 1)
              prepareStatement.setLong(36, 1)
            } else if (is_newCar_total_z_o == 1 && "4".equals(level)) {
              prepareStatement.setLong(16, 1)
              prepareStatement.setLong(37, 1)
            } else if (is_newCar_total_z_o == 1 && "5".equals(level)) {
              prepareStatement.setLong(17, 1)
              prepareStatement.setLong(38, 1)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改b_finish操作1到0
        else if (old_b_finish != null && "1".equals(old_b_finish) && "0".equals(b_finish)) {
          if ("0".equals(b_delete_flag) || ("1".equals(b_delete_flag) && "0".equals(old_b_delete_flag))) {
            var actual_balance = i_actual_balance
            var sum_power = m_sum_power
            if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
            if (old_m_sum_p != null) { sum_power = old_m_sum_power }
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
            prepareStatement = connect.prepareStatement(station_show_total_sql.toString())
            prepareStatement.setLong(16, l_station_id)
            prepareStatement.setInt(17, l_seller_id)
            for (m <- 1 to 15) {
              prepareStatement.setLong(m, 0L)
            }
            prepareStatement.setLong(1, 1)
            prepareStatement.setLong(14, sum_power)
            prepareStatement.setLong(15, actual_balance)

            //个人车队分类
            if ("1".equals(s_member_flag)) {
              prepareStatement.setLong(2, 1)

            } else if ("2".equals(s_member_flag) || StringUtil.isBlank(s_member_flag)) {
              prepareStatement.setLong(3, is_newCus_total_o_z)
              prepareStatement.setLong(4, sum_power)
              prepareStatement.setLong(5, 1)
              prepareStatement.setLong(6, actual_balance)
              prepareStatement.setLong(7, is_oldCar_total_o_z)
              prepareStatement.setLong(8, diffT)

            }
            //会员等级统计
            if (is_oldCar_total_o_z == 1 && "1".equals(level)) {
              prepareStatement.setLong(9, 1)
            } else if (is_oldCar_total_o_z == 1 && "2".equals(level)) {
              prepareStatement.setLong(10, 1)
            } else if (is_oldCar_total_o_z == 1 && "3".equals(level)) {
              prepareStatement.setLong(11, 1)
            } else if (is_oldCar_total_o_z == 1 && "4".equals(level)) {
              prepareStatement.setLong(12, 1)
            } else if (is_oldCar_total_o_z == 1 && "5".equals(level)) {
              prepareStatement.setLong(13, 1)
            }
            prepareStatement.execute()
            prepareStatement.close()
          }
        } //修改vin或i_actual_balance或m_sum_power
        else if ((old_v_vin != null || old_i_actual_b != null || old_m_sum_p != null || old_d_time_s != null
          || old_d_time_e != null) && "1".equals(b_finish) && "0".equals(b_delete_flag)) {
          var actual_balance = i_actual_balance
          var sum_power = m_sum_power
          if (old_i_actual_b != null) { actual_balance = old_i_actual_balance }
          if (old_m_sum_p != null) { sum_power = old_m_sum_power }
          val diff_actual_balance = i_actual_balance - actual_balance
          val diff_sum_power = m_sum_power - sum_power
          var diff_isnewCar = 0
          val diff_old_diffT = diffT - old_diffT
          if (old_v_vin != null) {
            diff_isnewCar = is_newCar_total_z_o - is_oldCar_total_o_z
          }
          val station_show_total_sql = new StringBuffer("update station_show_total_table set")
            .append(" total_station_team_power_count=total_station_team_power_count+?,")
            .append(" total_station_team_menoy_count=total_station_team_menoy_count+?,")
            .append(" total_station_service_team_cars=total_station_service_team_cars+?,")
            .append(" total_station_service_team_time=total_station_service_team_time+?,")
            .append(" total_station_power_count=total_station_power_count+?,")
            .append(" total_station_money_count=total_station_money_count+?") //40
            .append(" where l_station_id=? and l_seller_id=?")
          prepareStatement = connect.prepareStatement(station_show_total_sql.toString())
          prepareStatement.setLong(7, l_station_id)
          prepareStatement.setInt(8, l_seller_id)
          prepareStatement.setLong(1, diff_sum_power)
          prepareStatement.setLong(2, diff_actual_balance)
          prepareStatement.setLong(3, diff_isnewCar)
          prepareStatement.setLong(4, diff_old_diffT)
          prepareStatement.setLong(5, diff_sum_power)
          prepareStatement.setLong(6, diff_actual_balance)
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