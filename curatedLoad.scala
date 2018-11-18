package ctl
//import ctl.rawConsumer
import ctl.rawConsumer._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.DataFrame

object curatedLoad {

  def hiveLoad (svoc_Curated : DataFrame) : Unit = {

    spark.sparkContext.setLogLevel("ERROR")
    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)
    //import spark.implicits._

    println("===============================================================")
    println("Inserting Curated DF into Hive SVOC_CTL table")
    println("===============================================================")

    svoc_Curated.selectExpr("RECEIVED_TS"
      ,"MKD_FLT_NB"
      ,"SCH_DPRT_ARPT_CD"
      ,"SCH_ARR_ARPT_CD"
      ,"SCHED_LEG_LCL_DPTR_TMS"
      ,"OPER_CRR_CD"
      ,"LEG_DPTR_DT"
      ,"SCH_ARR_LDDTM"
      ,"EST_LEG_LCL_DPTR_TMS"
      ,"EST_LEG_LCL_ARR_TMS"
      ,"EST_LEG_UTC_DPTR_TMS"
      ,"EST_LEG_UTC_ARR_TMS"
      ,"SCH_ARR_UTCDTTM"
      ,"SCHED_LEG_UTC_DPTR_TMS"
      ,"GATE_GROUPING_NAME"
      ,"GATE_ID"
      ,"PSR_CUST_SM_STT"
      ,"CUST_FST_NM"
      ,"CUST_LST_NM"
      ,"CUST_SEAT_NB"
      ,"CUST_HM_ARPRT_CD"
      ,"BRD_STATS_CDE"
      ,"PSR_PRI_NB"
      ,"RECOG_ID"
      ,"PSR_RCTN_TYP_CD"
      ,"PSR_RSN_CD"
      ,"PSR_RCTN_RSN_TXT"
      ,"AA_SENT_DATE_TIMESTAMP"
      ,"CTL_IND"
      ,"FEEDBACK_UTCTS"
      ,"RESOLUTION_STATUS_CD"
      ,"IMPACTED_CUST_CNT").write.mode("append").saveAsTable("svoc_ctl.svoc_ctl")
    //hdfs://EDL-HDP/dvl/edl/apps/hive/warehouse/svoc_ctl.db/svoc_ctl
    spark.sql("select * from svoc_ctl.svoc_ctl").show
  }
}
