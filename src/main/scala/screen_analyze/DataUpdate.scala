package screen_analyze

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.SQLException

import org.apache.log4j.Logger
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSON
import screen_analyze.service.source_bill.OperationPileUpdate
import screen_analyze.service.source_bill.OperationPileDelete
import screen_analyze.service.source_bill.OperationPileInsert
import screen_analyze.service.source_bill.OperationBillAnalyzeUpdate
import screen_analyze.service.source_bill.OperationBillAnalyzeDelete
import screen_analyze.service.source_bill.OperationBillAnalyzeInsert
import screen_analyze.service.source_bill.AppointmentChargeFormUpdate
import screen_analyze.service.source_bill.AppointmentChargeFormDelete
import screen_analyze.service.source_bill.AppointmentChargeFormInsert
import screen_analyze.service.source_bill.OperationGunUpdate
import screen_analyze.service.source_bill.OperationGunDelete
import screen_analyze.service.source_bill.OperationGunInsert
import screen_analyze.service.source_bill.PersionalMemberUpdate
import screen_analyze.service.source_bill.PersionalMemberDelete
import screen_analyze.service.source_bill.PersionalMemberInsert
import screen_analyze.service.source_bill.OperationStationUpdate
import screen_analyze.service.source_bill.OperationStationDelete
import screen_analyze.service.source_bill.OperationStationInsert
import screen_analyze.service.source_bill.OperationlVehicleUpdate
import screen_analyze.service.source_bill.OperationlVehicleDelete
import screen_analyze.service.source_bill.OperationlVehicleInsert
import screen_analyze.service.source_bill.OperationPileTypeUpdate
import screen_analyze.service.source_bill.OperationPileAlarmHisUpdate
import screen_analyze.service.source_bill.OperationPileAlarmHisDelete
import screen_analyze.service.source_bill.OperationPileAlarmHisInsert
import screen_analyze.service.source_bill.OperationPileBillBandUpdate
import screen_analyze.service.source_bill.OperationPileBillBandInsert
import screen_analyze.service.source_bill.OperationPileBillBandDelete
import screen_analyze.service.source_bill.TransFormerUpdate
import screen_analyze.service.source_bill.TransFormerInsert
import screen_analyze.service.source_bill.TransFormerDelete
import java.util.{Date, Properties}
import java.text.SimpleDateFormat

import org.apache.kafka.common.internals.Topic
import org.apache.spark.TaskContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import screen_analyze.service.analyze_bill.StationShowDayTableUpdate
import screen_analyze.service.analyze_bill.StationShowMbTableUpdate
import screen_analyze.util.RedisUtil
import screen_analyze.service.analyze_bill.StationShowMmTableUpdate
import screen_analyze.service.analyze_bill.StationShowMonthTableUpdate
import screen_analyze.service.analyze_bill.StationShowTotalTableUpdate
import screen_analyze.service.analyze_bill.AllDayMemberTableUpdate
import screen_analyze.service.analyze_bill.AllDayTableUpdate
import screen_analyze.service.analyze_bill.AllMemberTableUpdate
import screen_analyze.service.analyze_bill.AllMonthTableUpdate
import screen_analyze.service.analyze_bill.AllTableUpdate

class DataUpdate extends Serializable {
  var esTime = ""
  var valueTimeout: Int = 0
  var anyTime = ""
  var topic = ""

  //  var maxTotal: Int = 0
  //  var maxIdle: Int = 0
  //  var maxWaitMillis: Long = 0
  //  var sentinel1: String = ""
  //  var sentinel2: String = ""
  //  var sentinel3: String = ""
  def this(time: String, anyTime: String, valueTimeout: Int, topic: String) {
    this()
    this.esTime = time
    this.valueTimeout = valueTimeout
    this.anyTime = anyTime
    this.topic = topic
    //    this.maxIdle = maxIdle
    //    this.maxTotal = maxTotal
    //    this.maxWaitMillis = maxWaitMillis
    //    this.sentinel1 = sentinel1
    //    this.sentinel2 = sentinel2
    //    this.sentinel3 = sentinel3
  }

  @transient private lazy val log = Logger.getLogger(this.getClass)

  def updateToAnalyze(data: InputDStream[ConsumerRecord[String, String]]) {

    data.foreachRDD((rdd, batchTime) => {
      log.info("??????????????????")
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        log.info("Kafka---has----data")
        try {
          val rddr = rdd.take(rdd.count().toInt)
          rddr.foreach {
            f =>
              val start = new Date().getTime
              analyzeJson(f.value())
              log.info(f.value())
              val end = new Date().getTime
              log.info("???????????????"+(end-start)+"ms")
          }
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
      //????????????offset
      data.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

  }

  def analyzeJson(datajson: String) {
    val jsonObject = JSON.parseObject(datajson)
    val tableN = jsonObject.getString("table")
    log.info("=================================?????????"+tableN)
    val sqltype = jsonObject.getString("type")
    val startTime = jsonObject.getLongValue("es")
    val start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startTime))
    val proT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(esTime).getTime
    val anyT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(anyTime).getTime
    if (startTime - proT >= 0) {
      log.info("*********?????????:" + tableN + "????????????:" + sqltype+"sql?????????"+start)

      /** -------- */
      //?????????????????????
      if ("t_pile".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operation_pile = new OperationPileUpdate
          operation_pile.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operation_pile = new OperationPileDelete
          operation_pile.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          log.info("=================================??????insert??????")
          val operation_pile = new OperationPileInsert
          operation_pile.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //????????????????????????
      else if ("t_bill".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operation_bill = new OperationBillAnalyzeUpdate
          operation_bill.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operation_bill = new OperationBillAnalyzeDelete
          operation_bill.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val operation_bill = new OperationBillAnalyzeInsert
          operation_bill.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_appointment_charge_form
      else if ("t_appoint_bill".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val charge_form = new AppointmentChargeFormUpdate
          charge_form.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val charge_form = new AppointmentChargeFormDelete
          charge_form.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val charge_form = new AppointmentChargeFormInsert
          charge_form.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_business_base_operation_gun
      else if ("t_gun".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operation_gun = new OperationGunUpdate
          operation_gun.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operation_gun = new OperationGunDelete
          operation_gun.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val operation_gun = new OperationGunInsert
          operation_gun.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_business_base_operation_persional_member
      else if ("t_member".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val persional_member = new PersionalMemberUpdate
          persional_member.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val persional_member = new PersionalMemberDelete
          persional_member.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val persional_member = new PersionalMemberInsert
          persional_member.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_station
      else if ("t_station".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operation_station = new OperationStationUpdate
          operation_station.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operation_station = new OperationStationDelete
          operation_station.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val operation_station = new OperationStationInsert
          operation_station.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_business_base_operationl_vehicle
      else if ("t_vehicle".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operationl_vehicle = new OperationlVehicleUpdate
          operationl_vehicle.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operationl_vehicle = new OperationlVehicleDelete
          operationl_vehicle.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val operationl_vehicle = new OperationlVehicleInsert
          operationl_vehicle.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_business_config_operation_pile_type
      else if ("t_pile_type".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val pile_type = new OperationPileTypeUpdate
          pile_type.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_origin_history_operation_pile_alarm_his
      else if ("t_alarm".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operation_pile_alarm_his = new OperationPileAlarmHisUpdate
          operation_pile_alarm_his.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operation_pile_alarm_his = new OperationPileAlarmHisDelete
          operation_pile_alarm_his.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val operation_pile_alarm_his = new OperationPileAlarmHisInsert
          operation_pile_alarm_his.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_origin_history_operation_pile_bill_band
      else if ("t_bill_stage_detail".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val operation_pile_bill_band = new OperationPileBillBandUpdate
          operation_pile_bill_band.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val operation_pile_bill_band = new OperationPileBillBandDelete
          operation_pile_bill_band.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val operation_pile_bill_band = new OperationPileBillBandInsert
          operation_pile_bill_band.analyzeInsertTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_trans_former
      else if ("t_trans_former".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val trans_former = new TransFormerUpdate
          trans_former.analyzeUpdateTable(jsonObject)
        } else if ("DELETE".equals(sqltype)) {
          val trans_former = new TransFormerDelete
          trans_former.analyzeDeleteTable(jsonObject)
        } else if ("INSERT".equals(sqltype)) {
          val trans_former = new TransFormerInsert
          trans_former.analyzeInsertTable(jsonObject)
        }
      }
    }
    if (startTime - anyT >= 0) {
      /** -------- */
      //???????????????station_show_day_table
      if ("station_show_day_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val stationShowDayTableUpdate = new StationShowDayTableUpdate(jedisutil, valueTimeout)
          stationShowDayTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????station_show_member_balance_table
      else if ("station_show_member_balance_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val stationShowMbTableUpdate = new StationShowMbTableUpdate(jedisutil, valueTimeout)
          stationShowMbTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????station_show_month_member_table
      else if ("station_show_month_member_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val stationShowMmTableUpdate = new StationShowMmTableUpdate(jedisutil, valueTimeout)
          stationShowMmTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????station_show_month_table
      else if ("station_show_month_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val stationShowMonthTableUpdate = new StationShowMonthTableUpdate(jedisutil, valueTimeout)
          stationShowMonthTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????station_show_total_table
      else if ("station_show_total_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val stationShowTotalTableUpdate = new StationShowTotalTableUpdate(jedisutil, valueTimeout)
          stationShowTotalTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_all_day_member_table
      else if ("t_all_day_member_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val allDayMemberTableUpdate = new AllDayMemberTableUpdate(jedisutil, valueTimeout)
          allDayMemberTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_all_day_table
      else if ("t_all_day_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val allDayTableUpdate = new AllDayTableUpdate(jedisutil, valueTimeout)
          allDayTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_all_member_table
      else if ("t_all_member_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val allMemberTableUpdate = new AllMemberTableUpdate(jedisutil, valueTimeout)
          allMemberTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_all_month_table
      else if ("t_all_month_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val allMonthTableUpdate = new AllMonthTableUpdate(jedisutil, valueTimeout)
          allMonthTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }

      /** -------- */
      //???????????????t_all_table
      else if ("t_all_table".equals(tableN)) {
        if ("UPDATE".equals(sqltype)) {
          val jedisutil = RedisUtil.jedisPool.getResource
          val allTableUpdate = new AllTableUpdate(jedisutil, valueTimeout)
          allTableUpdate.analyzeUpdateTable(jsonObject)
        }
      }
    }

  }
}