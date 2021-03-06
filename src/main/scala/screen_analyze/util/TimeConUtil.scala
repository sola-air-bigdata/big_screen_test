package screen_analyze.util

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

/**日期转化类*/
object TimeConUtil {
  //取年月日
  def timeConversion(date:String,format:String):String={
    var s = ""
    if("%Y-%m".equals(format)) {
    		val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
    		val cal=Calendar.getInstance();
    		cal.setTime(d);
    		val year = cal.get(Calendar.YEAR)+"";
    		var month = cal.get(Calendar.MONTH)+1+"";
    		if(month.toInt<10){
    		  month = "0".concat(month)
    		}
    	s =	year+"-"+month;
    	}else if("%Y-%m-%d".equals(format)){
    		val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
    		val cal=Calendar.getInstance();
    		cal.setTime(d);
    		val year = cal.get(Calendar.YEAR)+"";
    		var month = cal.get(Calendar.MONTH)+1+"";
    		var day = cal.get(Calendar.DAY_OF_MONTH)+"";
    		if(month.toInt<10){
    		  month = "0".concat(month)
    		}
    		if(day.toInt<10){
    		  day = "0".concat(day)
    		}
    		s = year+"-"+month+"-"+day;
    	}
    s
  }
  //取时间差值
  def timeDIFF(s_time:String,e_time:String):Long={
    var l = 0L
    if(StringUtil.isBlank(s_time)||StringUtil.isBlank(e_time)) {
    		 l;
    	}
    	val s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s_time).getTime();
    	val e = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(e_time).getTime();
    	val diff = Math.abs(s-e);
    	l = (diff)/(1000);
    	 l
  }
  //取输入时间00:00:00和下天00:00:00
  def getTime(date:String,iscurrent:String):String={
    var s = ""
    if("1".equals(iscurrent)){
    val currentEndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
		val cal = Calendar.getInstance();
		cal.setTime(currentEndDate);
		cal.set(Calendar.AM_PM, 0);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		val nextDate = cal.getTime();
		s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(nextDate);
    }else{
    val currentEndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
		val cal = Calendar.getInstance();
		cal.setTime(currentEndDate);
		cal.add(Calendar.DATE, 1);
		cal.set(Calendar.AM_PM, 0);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		val nextDate = cal.getTime();
		s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(nextDate);
    }
    s
  }
  //取输入时间xxxx-xx-01 00:00:00和下月xxxx-xx-01 00:00:00
  def getMonth(date:String,iscurrent:String):String ={
    var s = ""
     if("1".equals(iscurrent)){
     val currentEndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
		val cal = Calendar.getInstance();
		cal.setTime(currentEndDate);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.AM_PM, 0);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		val nextDate = cal.getTime();
		s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(nextDate);
    }else{
    val currentEndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
		val cal = Calendar.getInstance();
		cal.setTime(currentEndDate);
		cal.add(Calendar.MONTH, 1);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.AM_PM, 0);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		val nextDate = cal.getTime();
		s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(nextDate);
    }
    s
  }
  //获取时间小时
  def getHours(date:String):Int={
    val s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date)
		  val cal=Calendar.getInstance()
			cal.setTime(s);
		cal.get(Calendar.HOUR_OF_DAY)
  }
  //比较两个时间大小
  def timeDiff(date_1:String,date_2:String):String={
    var flag = "" 
    val s1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date_1).getTime
    val s2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date_2).getTime
    if(s1-s2>0){
      flag = "1"
    }else if(s1-s2<0){
      flag = "0"
    }
    flag
  }
  
}