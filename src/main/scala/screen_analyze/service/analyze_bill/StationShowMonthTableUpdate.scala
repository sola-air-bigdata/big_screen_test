package screen_analyze.service.analyze_bill
import com.alibaba.fastjson.JSONObject
import org.apache.log4j.Logger
import screen_analyze.util.RedisUtil
import redis.clients.jedis.Jedis

class StationShowMonthTableUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var jedisutil: Jedis = _
  var valueTimeout: Int = 0
  def this(jedisutil: Jedis, valueTimeout: Int) {
    this()
    this.jedisutil = jedisutil
    this.valueTimeout = valueTimeout
  }
  def analyzeUpdateTable(jsonSql: JSONObject) {
    val jedis = jedisutil
    try {
      //     log.info("********StationShowMonthTableUpdate处理json:" + jsonSql.toJSONString())
      val dataArr = jsonSql.getJSONArray("data")
      val oldArr = jsonSql.getJSONArray("old")
      var arrSize = dataArr.size()
      var old = new JSONObject
      var data = new JSONObject
      for (m <- 0 to arrSize - 1) {
        data = dataArr.getJSONObject(m)
        old = oldArr.getJSONObject(m)
        val l_station_id = data.getLongValue("l_station_id")
        val l_seller_id = data.getIntValue("l_seller_id")
        val d_month_time = data.getString("d_month_time")
        val keys = old.keySet()
        val itr = keys.iterator()
        while (itr.hasNext()) {
          var s = itr.next()
          var key = "SMOT" + "_" + l_station_id + "_" + l_seller_id + "_" + d_month_time + "_" + s
          var value = data.getLongValue(s)
          val isexist = jedis.exists(key)
          if (isexist) {
            jedis.setex(key, valueTimeout, value + "")
          }
        }
      }
      jedis.close()
    } catch {
      case ex: Exception => log.info("*****StationShowMonthTableUpdate**redis处理异常****")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
  }
}