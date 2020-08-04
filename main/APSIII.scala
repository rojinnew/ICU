
package edu.gatech.cse6250.main

import edu.gatech.cse6250.ioutils.CSVUtils.{registerNote2, registerTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, _}
import edu.gatech.cse6250.ioutils.CSVUtils.merge

object APSIII {
  def main(args: Array[String]) {
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

   // val sc = createContext


    val sparkContext = SparkSession.builder()
                                   .appName("Spark In Action")
                                   .master("local[*]")
                                   .getOrCreate()
   sparkContext.sql("set spark.sql.caseSensitive=false")
//sparkContext.sql("SET spark.sql.codegen=true")
    val path = "dataset/"
    // Remove """ in LABEVENTS.CSV
    //Preprocess.process()
    // READ the files from scv and create a corresponding table
    /* ------------------------------patients--------------------------------*/
    println("patients  starts")
    val patientsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),  
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("GENDER", StringType, true),  
      StructField("DOB", TimestampType, true), 
      StructField("DOD", TimestampType, true), 
      StructField("DOD_HOSP", TimestampType, true), 
      StructField("DOD_SSN", TimestampType, true), 
      StructField("EXPIRE_FLAG", IntegerType, true)
    ))
    registerTable(sparkContext,path, "PATIENTS.csv","patients",patientsSchema)
    /*
    val test = sparkContext.sql(
      """  SELECT * from patients where lower(gender) like "f" or
        |                 lower(gender)  like "m%" limit 10 """.stripMargin).show()
    */
    println("icu stay starts")
    /* --------------------------------icustays------------------------------*/
    val  icustaysSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("DBSOURCE", StringType,true),
      StructField("FIRST_CAREUNIT", StringType,true),
      StructField("LAST_CAREUNIT", StringType,true),
      StructField("FIRST_WARDID", IntegerType,true),
      StructField("LAST_WARDID", IntegerType,true),
      StructField("INTIME", TimestampType,true),
      StructField("OUTTIME", TimestampType,true),
      StructField("LOS", DoubleType,true)
    ))
    registerTable(sparkContext,path, "ICUSTAYS.csv","icustays",icustaysSchema)
   // val test = sparkContext.sql("""  SELECT * from icustays2   where subject_id = 214""").createTempView("icustays")
    //println(test.count())

    println("admissions starts")
    /* ---------------------------------admissions-----------------------------*/
    val  admissionsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ADMITTIME",TimestampType ,true),
      StructField("DISCHTIME",TimestampType ,true),
      StructField("DEATHTIME",TimestampType ,true),
      StructField("ADMISSION_TYPE",StringType ,true),
      StructField("ADMISSION_LOCATION",StringType ,true),
      StructField("DISCHARGE_LOCATION",StringType ,true),
      StructField("INSURANCE",StringType ,true),
      StructField("LANGUAGE",StringType ,true),
      StructField("RELIGION",StringType ,true),
      StructField("MARITAL_STATUS",StringType ,true),
      StructField("ETHNICITY",StringType ,true),
      StructField("EDREGTIME",TimestampType ,true),
      StructField("EDOUTTIME",TimestampType ,true),
      StructField("DIAGNOSIS",StringType ,true),
      StructField("HOSPITAL_EXPIRE_FLAG", IntegerType, true)
    ))
    registerTable(sparkContext,path, "ADMISSIONS.csv","admissions",admissionsSchema)
    //val test = sparkContext.sql("""  SELECT * from admissions""")
     // println(test.count())


    println("outputevents starts")
    /* ---------------------------------outputvents-----------------------------*/
    val  outputeventsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("CHARTTIME", TimestampType, true),
      StructField("ITEMID", IntegerType, true),
      StructField("VALUE",DoubleType, true),
      StructField("VALUEUOM",StringType, true),
      StructField("STORETIME", TimestampType, true),
      StructField("CGID", IntegerType, true),
      StructField("STOPPED", StringType, true),
      StructField("NEWBOTTLE", StringType, true),
      StructField("ISERROR", IntegerType, true)
    ))
    registerTable(sparkContext,path, "OUTPUTEVENTS.csv","outputevents",outputeventsSchema)
   // val test = sparkContext.sql("""  SELECT * from outputevents""")
    //println(test.count())

    println("chartevents starts")

    /* ---------------------------------chartevents-----------------------------*/
    val  charteventsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("ITEMID", IntegerType, true),
      StructField("CHARTTIME",TimestampType ,true),
      StructField("STORETIME",TimestampType ,true),
      StructField("CGID", IntegerType, true),
      StructField("VALUE",StringType ,true),//
      StructField("VALUENUM",DoubleType ,true),
      StructField("VALUEUOM",StringType ,true),
      StructField("WARNING",IntegerType ,true),
      StructField("ERROR",IntegerType ,true),
      StructField("RESULTSTATUS",StringType ,true),
      StructField("STOPPED",StringType ,true),
      StructField("CONSTRAINT",StringType, true)

    ))
    registerTable(sparkContext,path, "CHARTEVENTS.csv","chartevents",charteventsSchema)

   //FsparkContext.sql("""  SELECT * from chartevents """).toDF().select("error").distinct.show(10,false)



   //  println(test.count())


    println("diag starts")
        /* ---------------------------------diagnoses_icd-----------------------------*/
    val  diagnoses_icdSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("SEQ_NUM", IntegerType, true),
      StructField("ICD9_CODE",StringType, true)
    ))
    registerTable(sparkContext,path, "DIAGNOSES_ICD.csv","diagnoses_icd",diagnoses_icdSchema)
    //val test = sparkContext.sql("""  SELECT * from diagnoses_icd""")
    //println(test.count())

    println("services starts")
    /* ---------------------------------services-----------------------------*/
    val  servicesSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("TRANSFERTIME", TimestampType, true),
      StructField("PREV_SERVICE",StringType, true),
      StructField("CURR_SERVICE",StringType, true)
    ))
    registerTable(sparkContext,path, "SERVICES.csv","services",servicesSchema)

    //val test = sparkContext.sql("""  SELECT * from services""")
   // println(test.count())

    println("labevents starts")
    /* ---------------------------------labevents-----------------------------*/
    val  labeventsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ITEMID", IntegerType, true),
      StructField("CHARTTIME", TimestampType, true),
      StructField("VALUE",StringType, true),
      StructField("VALUENUM", DoubleType, true),
      StructField("VALUEUOM",StringType, true),
      StructField("FLAG",StringType, true)
    ))
    registerTable(sparkContext,path, "LABEVENTS.csv","labevents",labeventsSchema)

    //val test = sparkContext.sql("""  SELECT VALUENUM from labevents """).show()
    //println(test.count())



    println("procedureevents_mv starts")
    /* ---------------------------------labevents-----------------------------*/
    val  procedureevents_mvSchema = StructType(Array(
      StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("STARTTIME", TimestampType, true),
      StructField("ENDTIME", TimestampType, true),
      StructField("ITEMID", IntegerType, true),
      StructField("VALUE",DoubleType, true),
      StructField("VALUEUOM",StringType, true),
      StructField("LOCATION",StringType, true),
      StructField("LOCATIONCATEGORY",StringType, true),
      StructField("STORETIME", TimestampType, true),
      StructField("CGID", IntegerType, true),
      StructField("ORDERID", IntegerType, true),
      StructField("LINKORDERID", IntegerType, true),
      StructField("ORDERCATEGORYNAME",StringType, true),
      StructField("SECONDARYORDERCATEGORYNAME",StringType, true),
      StructField("ORDERCATEGORYDESCRIPTION",StringType, true),
      StructField("ISOPENBAG", IntegerType, true),
      StructField("CONTINUEINNEXTDEPT", IntegerType, true),
      StructField("CANCELREASON", IntegerType, true),
      StructField("STATUSDESCRIPTION", StringType, true),
      StructField("COMMENTS_EDITEDBY", StringType, true),
      StructField("COMMENTS_CANCELEDBY", StringType, true),
      StructField("COMMENTS_DATE", TimestampType, true)
    ))
    registerTable(sparkContext,path, "PROCEDUREEVENTS_MV.csv","procedureevents_mv",procedureevents_mvSchema)

    // val test = sparkContext.sql("""  SELECT row_id from procedureevents_mv """)
   // println(test.count())


/* --------------noteevents ---------*/
    println("notes  starts")
    val notesSchema = StructType(Array(
      StructField("ROW_ID", IntegerType, true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("CHARTDATE", DateType, true),
      StructField("CHARTTIME", TimestampType, true),
      StructField("STORETIME", TimestampType, true),
      StructField("CATEGORY", StringType, true),
      StructField("DESCRIPTION", StringType, true),
      StructField("CGID", IntegerType, true),
      StructField("ISERROR", StringType, true),
      StructField("TEXT", StringType, true)

    ))
   // registerTable(sparkContext, path, "NOTEEVENTS.csv", "noteevents", notesSchema)
    registerNote2(sparkContext, path, "new2.csv", "noteevents", notesSchema)


    /* --------------inputevents_cv ---------*/
    println("inputevents_cv  starts")
    val inputevents_cvSchema = StructType(Array(
      StructField("ROW_ID", IntegerType, true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("CHARTTIME", TimestampType, true),
      StructField("ITEMID", IntegerType, true),
      StructField("AMOUNT", DoubleType, true),
      StructField("AMOUNTUOM", StringType, true),
      StructField("RATE", DoubleType, true)

    ))
    registerTable(sparkContext, path, "INPUTEVENTS_CV.csv", "inputevents_cv", inputevents_cvSchema)

    /*---------inputevenets_mv ----------*/


    println("inputevents_mv  starts")
    val inputevents_mvSchema = StructType(Array(
      StructField("ROW_ID", IntegerType, true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("STARTTIME", TimestampType, true),
      StructField("ENDTIME", TimestampType, true),
      StructField("ITEMID", IntegerType, true),
      StructField("AMOUNT", DoubleType, true),
      StructField("AMOUNTUOM", StringType, true),
      StructField("RATE", DoubleType, true),
      StructField("RATEUOM", StringType, true),
      StructField("STORETIME", TimestampType, true),
      StructField("CGID", IntegerType, true),
      StructField("ORDERID", IntegerType, true),
      StructField("LINKORDERID", IntegerType, true),
      StructField("ORDERCATEGORYNAME", StringType, true),
      StructField("SECONDARYORDERCATEGORYNAME", StringType, true),
      StructField("ORDERCOMPONENTTYPEDESCRIPTION", StringType, true),
      StructField("ORDERCATEGORYDESCRIPTION", StringType, true),
      StructField("PATIENTWEIGHT", DoubleType, true),
      StructField("TOTALAMOUNT", DoubleType, true),
      StructField("TOTALAMOUNTUOM", StringType, true),
      StructField("ISOPENBAG", IntegerType, true),
      StructField("CONTINUEINNEXTDEPT", IntegerType, true),
      StructField("CANCELREASON", IntegerType, true),
      StructField("STATUSDESCRIPTION", StringType, true)
    ))
    registerTable(sparkContext, path, "INPUTEVENTS_MV.csv", "inputevents_mv", inputevents_mvSchema)

    //sparkContext.sql("""  SELECT STATUSDESCRIPTION from inputevents_mv """).show(2,false)





    /* --------------------------------- vitalsfirstday-----------------------------*/
    println("vitals first day starts")


// In needs tables: icustays, chartevents
    val vitalsfirstday = sparkContext.sql(
      """
-- This query pivots the vital signs for the first 24 hours of a patient's stay
-- Vital signs include heart rate, blood pressure, respiration rate, and temperature

SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
, avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean

FROM  (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose

    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum

  from icustays ie
  left join chartevents ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  and ce.charttime between ie.intime and ie.intime + interval 1 day
  -- exclude rows marked as error
  --and ce.error IS DISTINCT FROM 1
 and (ce.error = 0 or ce.error is null)
  where ce.itemid in
  (
  -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"

  -- Systolic/diastolic

  51, --	Arterial BP [Systolic]
  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  6701, --	Arterial BP #2 [Systolic]
  220179, --	Non Invasive Blood Pressure systolic
  220050, --	Arterial Blood Pressure systolic

  8368, --	Arterial BP [Diastolic]
  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  8555, --	Arterial BP #2 [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic
  220051, --	Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --	Arterial BP Mean #2
  443, --	Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"

  -- RESPIRATORY RATE
  618,--	Respiratory Rate
  615,--	Resp Rate (Total)
  220210,--	Respiratory Rate
  224690, --	Respiratory Rate (Total)


  -- SPO2, peripheral
  646, 220277,

  -- GLUCOSE, both lab and fingerstick
  807,--	Fingerstick Glucose
  811,--	Glucose (70-105)
  1529,--	Glucose
  3745,--	BloodGlucose
  3744,--	Blood Glucose
  225664,--	Glucose finger stick
  220621,--	Glucose (serum)
  226537,--	Glucose (whole blood)

  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,	-- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --	"Temperature F"

  )
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id """)


    vitalsfirstday.createTempView("vitalsfirstday")
   // vitalsfirstday.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("test/testfile.csv")


    //val test = sparkContext.sql("""  SELECT * from vitalsfirstday""").show()
    //println(test.count())
    /* --------------------------------- labsfirstday-----------------------------*/
    println("labs first day starts")
    //it needs icustays, labevents

    val labsfirstday = sparkContext.sql(
      """
-- This query pivots lab values taken in the first 24 hours of a patient's stay

-- Have already confirmed that the unit of measurement is always the same: null or the correct unit


SELECT
  pvt.subject_id, pvt.hadm_id, pvt.icustay_id

  , min(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_min
  , max(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_max
  , min(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_min
  , max(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_max
  , min(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_min
  , max(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_max
  , min(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_min
  , max(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_max
  , min(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_min
  , max(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_max
  , min(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_min
  , max(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_max
  , min(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_min
  , max(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_max
  , min(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_min
  , max(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_max
  , min(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_min
  , max(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_max
  , min(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_min
  , max(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_max
  , min(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_min
  , max(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_max
  , min(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_min
  , max(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_max
  , min(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_min
  , max(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_max
  , min(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_min
  , max(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_max
  , min(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_min
  , max(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_max
  , min(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_min
  , max(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_max
  , min(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END) as SODIUM_min
  , max(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end) as SODIUM_max
  , min(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_min
  , max(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_max
  , min(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_min
  , max(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_max


FROM
( -- begin query that extracts the data
  SELECT ie.subject_id, ie.hadm_id, ie.icustay_id
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
  , CASE
        WHEN itemid = 50868 THEN 'ANION GAP'
        WHEN itemid = 50862 THEN 'ALBUMIN'
        WHEN itemid = 51144 THEN 'BANDS'
        WHEN itemid = 50882 THEN 'BICARBONATE'
        WHEN itemid = 50885 THEN 'BILIRUBIN'
        WHEN itemid = 50912 THEN 'CREATININE'
        WHEN itemid = 50806 THEN 'CHLORIDE'
        WHEN itemid = 50902 THEN 'CHLORIDE'
        WHEN itemid = 50809 THEN 'GLUCOSE'
        WHEN itemid = 50931 THEN 'GLUCOSE'
        WHEN itemid = 50810 THEN 'HEMATOCRIT'
        WHEN itemid = 51221 THEN 'HEMATOCRIT'
        WHEN itemid = 50811 THEN 'HEMOGLOBIN'
        WHEN itemid = 51222 THEN 'HEMOGLOBIN'
        WHEN itemid = 50813 THEN 'LACTATE'
        WHEN itemid = 51265 THEN 'PLATELET'
        WHEN itemid = 50822 THEN 'POTASSIUM'
        WHEN itemid = 50971 THEN 'POTASSIUM'
        WHEN itemid = 51275 THEN 'PTT'
        WHEN itemid = 51237 THEN 'INR'
        WHEN itemid = 51274 THEN 'PT'
        WHEN itemid = 50824 THEN 'SODIUM'
        WHEN itemid = 50983 THEN 'SODIUM'
        WHEN itemid = 51006 THEN 'BUN'
        WHEN itemid = 51300 THEN 'WBC'
        WHEN itemid = 51301 THEN 'WBC'
      ELSE null
    END AS label
  , -- add in some sanity checks on the values
  -- the where clause below requires all valuenum to be > 0, so these are only upper limit checks
    CASE
      WHEN itemid = 50862 and valuenum >    10 THEN null -- g/dL 'ALBUMIN'
      WHEN itemid = 50868 and valuenum > 10000 THEN null -- mEq/L 'ANION GAP'
      WHEN itemid = 51144 and valuenum <     0 THEN null -- immature band forms, %
      WHEN itemid = 51144 and valuenum >   100 THEN null -- immature band forms, %
      WHEN itemid = 50882 and valuenum > 10000 THEN null -- mEq/L 'BICARBONATE'
      WHEN itemid = 50885 and valuenum >   150 THEN null -- mg/dL 'BILIRUBIN'
      WHEN itemid = 50806 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
      WHEN itemid = 50902 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
      WHEN itemid = 50912 and valuenum >   150 THEN null -- mg/dL 'CREATININE'
      WHEN itemid = 50809 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
      WHEN itemid = 50931 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
      WHEN itemid = 50810 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
      WHEN itemid = 51221 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
      WHEN itemid = 50811 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
      WHEN itemid = 51222 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
      WHEN itemid = 50813 and valuenum >    50 THEN null -- mmol/L 'LACTATE'
      WHEN itemid = 51265 and valuenum > 10000 THEN null -- K/uL 'PLATELET'
      WHEN itemid = 50822 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
      WHEN itemid = 50971 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
      WHEN itemid = 51275 and valuenum >   150 THEN null -- sec 'PTT'
      WHEN itemid = 51237 and valuenum >    50 THEN null -- 'INR'
      WHEN itemid = 51274 and valuenum >   150 THEN null -- sec 'PT'
      WHEN itemid = 50824 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
      WHEN itemid = 50983 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
      WHEN itemid = 51006 and valuenum >   300 THEN null -- 'BUN'
      WHEN itemid = 51300 and valuenum >  1000 THEN null -- 'WBC'
      WHEN itemid = 51301 and valuenum >  1000 THEN null -- 'WBC'
    ELSE le.valuenum
    END AS valuenum

  FROM icustays ie

  LEFT JOIN labevents le
    ON le.subject_id = ie.subject_id AND le.hadm_id = ie.hadm_id
    AND le.charttime BETWEEN (ie.intime - interval 6 hour) AND (ie.intime + interval 1 day)
    --AND le.ITEMID in
    AND le.itemid in
    (
      -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
      50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
      50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
      51144, -- BANDS - hematology
      50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
      50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
      50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
      50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
      50806, -- CHLORIDE, WHOLE BLOOD | BLOOD GAS | BLOOD | 48187
      50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
      50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
      51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
      50810, -- HEMATOCRIT, CALCULATED | BLOOD GAS | BLOOD | 89715
      51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
      50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
      50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
      51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
      50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
      50822, -- POTASSIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 192946
      51275, -- PTT | HEMATOLOGY | BLOOD | 474937
      51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
      51274, -- PT | HEMATOLOGY | BLOOD | 469090
      50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
      50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
      51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
      51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
      51300  -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
    )
    AND valuenum IS NOT null AND valuenum > 0 -- lab values cannot be 0 and cannot be negative
) pvt
GROUP BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
ORDER BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
      """)
    labsfirstday.createTempView("labsfirstday")


    // In needs tables: icustays, chartevents
    // val test = sparkContext.sql("""  SELECT * from labsfirstday""")
    //println(test.count())
    /* --------------------------------- gcsfirstday-----------------------------*/
    val gcsfirstday = sparkContext.sql(
      """
-- ITEMIDs used:

-- CAREVUE
--    723 as GCSVerbal
--    454 as GCSMotor
--    184 as GCSEyes

-- METAVISION
--    223900 GCS - Verbal Response
--    223901 GCS - Motor Response
--    220739 GCS - Eye Opening

-- The code combines the ITEMIDs into the carevue itemids, then pivots those
-- So 223900 is changed to 723, then the ITEMID 723 is pivoted to form GCSVerbal

-- Note:
--  The GCS for sedated patients is defaulted to 15 in this code.
--  This is in line with how the data is meant to be collected.
--  e.g., from the SAPS II publication:
--    For sedated patients, the Glasgow Coma Score before sedation was used.
--    This was ascertained either from interviewing the physician who ordered the sedation,
--    or by reviewing the patient's medical record.


with base as
(
  SELECT pvt.icustay_id
  , pvt.charttime

  -- Easier names - note we coalesced Metavision and CareVue IDs below
  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes

  -- If verbal was set to 0 in the below select, then this is an intubated patient
  , case
      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0
    then 1
    else 0
    end as EndoTrachFlag

  , ROW_NUMBER()
         OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn
  FROM  (
 -- select l.ICUSTAY_ID
 select l.icustay_id
  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
  , case
      when l.ITEMID in (723,223900) then 723
      when l.ITEMID in (454,223901) then 454
      when l.ITEMID in (184,220739) then 184
      else l.ITEMID end
    as ITEMID

  -- convert the data into a number, reserving a value of 0 for ET/Trach
  , case
      -- endotrach/vent is assigned a value of 0, later parsed specially
      when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
      when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision

      else VALUENUM
      end
    as VALUENUM
  , l.CHARTTIME
  from CHARTEVENTS l

  -- get intime for charttime subselection
  inner join icustays b
    on l.icustay_id = b.icustay_id

  -- Isolate the desired GCS variables
  where l.ITEMID in
  (
    -- 198 -- GCS
    -- GCS components, CareVue
    184, 454, 723
    -- GCS components, Metavision
    , 223900, 223901, 220739
  )
  -- Only get data for the first 24 hours
  and l.charttime between b.intime and b.intime + interval 1 day
  -- exclude rows marked as error
  --and l.error IS DISTINCT FROM 1
-- and l.error != 1
and (l.error = 0 or l.error is null)
  ) pvt
  group by pvt.ICUSTAY_ID, pvt.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS

  from base b
  -- join to itself within 6 hours to get previous value
  left join base b2
    on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 and b2.charttime > b.charttime - interval 6 hour
)
, gcs_final as (
  select gcs.*
  -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
  , ROW_NUMBER ()
          OVER (PARTITION BY gcs.ICUSTAY_ID
                ORDER BY gcs.GCS
               ) as IsMinGCS
  from gcs
)
select ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
-- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
, GCS as MinGCS
, coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
, coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
, coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
, EndoTrachFlag as EndoTrachFlag

-- subselect down to the cohort of eligible patients
from icustays ie
left join gcs_final gs
  on ie.ICUSTAY_ID = gs.ICUSTAY_ID and gs.IsMinGCS = 1
ORDER BY ie.ICUSTAY_ID
      """)
    gcsfirstday.createTempView("gcsfirstday")
    // In needs tables: icustays, outputevents 
    //val test = sparkContext.sql("""  SELECT * from gcsfirstday""").show()
   // println(test.count())
   println("uofirstday starts")

    /* --------------------------------- uofirstday-----------------------------*/
val uofirstday = sparkContext.sql(
  """
-- ------------------------------------------------------------------
-- Purpose: Create a view of the urine output for each ICUSTAY_ID over the first 24 hours.
-- ------------------------------------------------------------------
select
  -- patient identifiers
  ie.subject_id, ie.hadm_id, ie.icustay_id

  -- volumes associated with urine output ITEMIDs
  , sum(
      -- we consider input of GU irrigant as a negative volume
      case
        when oe.itemid = 227488 and oe.value > 0 then -1*oe.value
        else oe.value
    end) as UrineOutput
from icustays ie
-- Join to the outputevents table to get urine output
left join outputevents oe
-- join on all patient identifiers
on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and ie.icustay_id = oe.icustay_id
-- and ensure the data occurs during the first day
and oe.charttime between ie.intime and (ie.intime + interval '1' day) -- first ICU day
where itemid in
(
-- these are the most frequently occurring urine output observations in CareVue
40055, -- "Urine Out Foley"
43175, -- "Urine ."
40069, -- "Urine Out Void"
40094, -- "Urine Out Condom Cath"
40715, -- "Urine Out Suprapubic"
40473, -- "Urine Out IleoConduit"
40085, -- "Urine Out Incontinent"
40057, -- "Urine Out Rt Nephrostomy"
40056, -- "Urine Out Lt Nephrostomy"
40405, -- "Urine Out Other"
40428, -- "Urine Out Straight Cath"
40086,--	Urine Out Incontinent
40096, -- "Urine Out Ureteral Stent #1"
40651, -- "Urine Out Ureteral Stent #2"

-- these are the most frequently occurring urine output observations in MetaVision
226559, -- "Foley"
226560, -- "Void"
226561, -- "Condom Cath"
226584, -- "Ileoconduit"
226563, -- "Suprapubic"
226564, -- "R Nephrostomy"
226565, -- "L Nephrostomy"
226567, --	Straight Cath
226557, -- R Ureteral Stent
226558, -- L Ureteral Stent
227488, -- GU Irrigant Volume In
227489  -- GU Irrigant/Urine Volume Out
)
group by ie.subject_id, ie.hadm_id, ie.icustay_id
order by ie.subject_id, ie.hadm_id, ie.icustay_id
  """)

    uofirstday.createTempView("uofirstday")
   // val test = sparkContext.sql("""  SELECT * from uofirstday""").show()
    // println(test.count())

    println("ventilation settings starts")
// needs chartevents,procedureevents_mv

    val ventsettings = sparkContext.sql(
      """
-- This query extracts the duration of mechanical ventilation
-- The main goal of the query is to aggregate sequential ventilator settings
-- into single mechanical ventilation "events". The start and end time of these
-- events can then be used for various purposes: calculating the total duration
-- of mechanical ventilation, cross-checking values (e.g. PaO2:FiO2 on vent), etc

-- The query's logic is roughly:
--    1) The presence of a mechanical ventilation setting starts a new ventilation event
--    2) Any instance of a setting in the next 8 hours continues the event
--    3) Certain elements end the current ventilation event
--        a) documented extubation ends the current ventilation
--        b) initiation of non-invasive vent and/or oxygen ends the current vent
-- The ventilation events are numbered consecutively by the `num` column.

-- First, create a temporary table to store relevant data from CHARTEVENTS.
select
icustay_id, charttime
  -- case statement determining whether it is an instance of mech vent
  , max(
    case
      when itemid is null or value is null then 0 -- can't have null values
      when itemid = 720 and value != 'Other/Remarks' THEN 1  -- VentTypeRecorded
      when itemid = 223848 and value != 'Other' THEN 1
      when itemid = 223849 then 1 -- ventilator mode
      when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
      when itemid in
        (
        445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
        , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
        , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
        , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
        , 543 -- PlateauPressure
        , 5865,5866,224707,224709,224705,224706 -- APRV pressure
        , 60,437,505,506,686,220339,224700 -- PEEP
        , 3459 -- high pressure relief
        , 501,502,503,224702 -- PCV
        , 223,667,668,669,670,671,672 -- TCPCV
        , 224701 -- PSVlevel
        )
        THEN 1
      else 0
    end
    ) as MechVent
    , max(
      case
        -- initiation of oxygen therapy indicates the ventilation has ended
        when itemid = 226732 and value in
        (
          'Nasal cannula', -- 153714 observations
          'Face tent', -- 24601 observations
          'Aerosol-cool', -- 24560 observations
          'Trach mask ', -- 16435 observations
          'High flow neb', -- 10785 observations
          'Non-rebreather', -- 5182 observations
          'Venti mask ', -- 1947 observations
          'Medium conc mask ', -- 1888 observations
          'T-piece', -- 1135 observations
          'High flow nasal cannula', -- 925 observations
          'Ultrasonic neb', -- 9 observations
          'Vapomist' -- 3 observations
        ) then 1
        when itemid = 467 and value in
        (
          'Cannula', -- 278252 observations
          'Nasal Cannula', -- 248299 observations
          'None', -- 95498 observations
          'Face Tent', -- 35766 observations
          'Aerosol-Cool', -- 33919 observations
          'Trach Mask', -- 32655 observations
          'Hi Flow Neb', -- 14070 observations
          'Non-Rebreather', -- 10856 observations
          'Venti Mask', -- 4279 observations
          'Medium Conc Mask', -- 2114 observations
          'Vapotherm', -- 1655 observations
          'T-Piece', -- 779 observations
          'Hood', -- 670 observations
          'Hut', -- 150 observations
          'TranstrachealCat', -- 78 observations
          'Heated Neb', -- 37 observations
          'Ultrasonic Neb' -- 2 observations
        ) then 1
      else 0
      end
    ) as OxygenTherapy
    , max(
      case when itemid is null or value is null then 0
        -- extubated indicates ventilation event has ended
        when itemid = 640 and value = 'Extubated' then 1
        when itemid = 640 and value = 'Self Extubation' then 1
      else 0
      end
      )
      as Extubated
    , max(
      case when itemid is null or value is null then 0
        when itemid = 640 and value = 'Self Extubation' then 1
      else 0
      end
      )
      as SelfExtubated
from chartevents ce
where ce.value is not null
-- exclude rows marked as error
--and ce.error IS DISTINCT FROM 1
--and ce.error != 1
 and (ce.error = 0 or ce.error is null)
and itemid in
(
    -- the below are settings used to indicate ventilation
      720, 223849 -- vent mode
    , 223848 -- vent type
    , 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
    , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
    , 218,436,535,444,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean ("RespPressure")
    , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
    , 543 -- PlateauPressure
    , 5865,5866,224707,224709,224705,224706 -- APRV pressure
    , 60,437,505,506,686,220339,224700 -- PEEP
    , 3459 -- high pressure relief
    , 501,502,503,224702 -- PCV
    , 223,667,668,669,670,671,672 -- TCPCV
    , 224701 -- PSVlevel

    -- the below are settings used to indicate extubation
    , 640 -- extubated

    -- the below indicate oxygen/NIV, i.e. the end of a mechanical vent event
    , 468 -- O2 Delivery Device#2
    , 469 -- O2 Delivery Mode
    , 470 -- O2 Flow (lpm)
    , 471 -- O2 Flow (lpm) #2
    , 227287 -- O2 Flow (additional cannula)
    , 226732 -- O2 Delivery Device(s)
    , 223834 -- O2 Flow

    -- used in both oxygen + vent calculation
    , 467 -- O2 Delivery Device
)
group by icustay_id, charttime
UNION
-- add in the extubation flags from procedureevents_mv
-- note that we only need the start time for the extubation
-- (extubation is always charted as ending 1 minute after it started)
select
  icustay_id, starttime as charttime
  , 0 as MechVent
  , 0 as OxygenTherapy
  , 1 as Extubated
  , case when itemid = 225468 then 1 else 0 end as SelfExtubated
from procedureevents_mv
where itemid in
(
  227194 -- "Extubation"
, 225468 -- "Unplanned Extubation (patient-initiated)"
, 225477 -- "Unplanned Extubation (non-patient initiated)"
)
""")
    ventsettings.createTempView("ventsettings")
    //ventsettings.withColumn("charttime", $"charttime".cast(DateType))
     //val test = sparkContext.sql("""  SELECT (charttime) from ventsettings""").show()
    //println(test.count())


    println("ventilationdurations starts")

 val ventdurations = sparkContext.sql("""
--DROP MATERIALIZED VIEW IF EXISTS VENTDURATIONS CASCADE;
--create MATERIALIZED VIEW ventdurations as
with vd0 as
(
  select
    icustay_id
    -- this carries over the previous charttime which had a mechanical ventilation event
    , case
        when MechVent=1 then
          LAG(CHARTTIME, 1) OVER (partition by icustay_id, MechVent order by charttime)
        else null
      end as charttime_lag
    , charttime
    , MechVent
    , OxygenTherapy
    , Extubated
    , SelfExtubated
  from ventsettings
)
, vd1 as
(
  select
      icustay_id
      , charttime_lag
      , charttime
      , MechVent
      , OxygenTherapy
      , Extubated
      , SelfExtubated

      -- if this is a mechanical ventilation event, we calculate the time since the last event
      , case
          -- if the current observation indicates mechanical ventilation is present
          -- calculate the time since the last vent event
          when MechVent=1 then
            --CHARTTIME - charttime_lag
            unix_timestamp(charttime)-  (LAG(unix_timestamp(CHARTTIME), 1) OVER (partition by icustay_id, MechVent order by charttime) )
          else null
        end as ventduration

      , LAG(Extubated,1)
      OVER
      (
      partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
      order by charttime
      ) as ExtubatedLag

      -- now we determine if the current mech vent event is a "new", i.e. they've just been intubated
      , case
        -- if there is an extubation flag, we mark any subsequent ventilation as a new ventilation event
          --when Extubated = 1 then 0 -- extubation is *not* a new ventilation event, the *subsequent* row is
          when
            LAG(Extubated,1)
            OVER
            (
            partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
            order by charttime
            )
            = 1 then 1
          -- if patient has initiated oxygen therapy, and is not currently vented, start a newvent
          when MechVent = 0 and OxygenTherapy = 1 then 1
            -- if there is less than 8 hours between vent settings, we do not treat this as a new ventilation event
         -- when (CHARTTIME - charttime_lag) > interval '8' hour
          when ( unix_timestamp(charttime)-  (LAG(unix_timestamp(CHARTTIME), 1) OVER (partition by icustay_id, MechVent order by charttime))) > 28800
            then 1
        else 0
        end as newvent
  -- use the staging table with only vent settings from chart events
  FROM vd0 ventsettings
)
, vd2 as
(
  select vd1.*
  -- create a cumulative sum of the instances of new ventilation
  -- this results in a monotonic integer assigned to each instance of ventilation
  , case when MechVent=1 or Extubated = 1 then
      SUM( newvent )
      OVER ( partition by icustay_id order by charttime )
    else null end
    as ventnum
  --- now we convert CHARTTIME of ventilator settings into durations
  from vd1
)
-- create the durations for each mechanical ventilation instance
select icustay_id
  -- regenerate ventnum so it's sequential
  , ROW_NUMBER() over (partition by icustay_id order by ventnum) as ventnum
  , min(charttime) as starttime
  , max(charttime) as endtime
  --, extract(epoch from  max(charttime)-min(charttime))60/60 AS duration_hours
  ,( unix_timestamp(max(charttime))- unix_timestamp(min(charttime)))/3600 as duration_hours
from vd2
group by icustay_id, ventnum
having min(charttime) != max(charttime)
-- patient had to be mechanically ventilated at least once
-- i.e. max(mechvent) should be 1
-- this excludes a frequent situation of NIV/oxygen before intub
-- in these cases, ventnum=0 and max(mechvent)=0, so they are ignored
and max(mechvent) = 1
order by icustay_id, ventnum """)

   ventdurations.createTempView("ventdurations")
   //sparkContext.sql("""  SELECT * from ventdurations""").show()
    // println(test.count())

    // sparkContext.sql("""  SELECT * from gcsfirstday""").show()
    println("bloodgasfirstday starts")
// it need icustays, labevents
    val bloodgasfirstday = sparkContext.sql("""

-- The aim of this query is to pivot entries related to blood gases and
-- chemistry values which were found in LABEVENTS

-- things to check:
--  when a mixed venous/arterial blood sample are taken at the same time, is the store time different?

--create materialized view bloodgasfirstday as
with pvt as
( -- begin query that extracts the data
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
      , case
        when itemid = 50800 then 'SPECIMEN'
        when itemid = 50801 then 'AADO2'
        when itemid = 50802 then 'BASEEXCESS'
        when itemid = 50803 then 'BICARBONATE'
        when itemid = 50804 then 'TOTALCO2'
        when itemid = 50805 then 'CARBOXYHEMOGLOBIN'
        when itemid = 50806 then 'CHLORIDE'
        when itemid = 50808 then 'CALCIUM'
        when itemid = 50809 then 'GLUCOSE'
        when itemid = 50810 then 'HEMATOCRIT'
        when itemid = 50811 then 'HEMOGLOBIN'
        when itemid = 50812 then 'INTUBATED'
        when itemid = 50813 then 'LACTATE'
        when itemid = 50814 then 'METHEMOGLOBIN'
        when itemid = 50815 then 'O2FLOW'
        when itemid = 50816 then 'FIO2'
        when itemid = 50817 then 'SO2' -- OXYGENSATURATION
        when itemid = 50818 then 'PCO2'
        when itemid = 50819 then 'PEEP'
        when itemid = 50820 then 'PH'
        when itemid = 50821 then 'PO2'
        when itemid = 50822 then 'POTASSIUM'
        when itemid = 50823 then 'REQUIREDO2'
        when itemid = 50824 then 'SODIUM'
        when itemid = 50825 then 'TEMPERATURE'
        when itemid = 50826 then 'TIDALVOLUME'
        when itemid = 50827 then 'VENTILATIONRATE'
        when itemid = 50828 then 'VENTILATOR'
        else null
        end as label
        , charttime
        , value
        -- add in some sanity checks on the values
        , case
          when valuenum <= 0 then null
          when itemid = 50810 and valuenum > 100 then null -- hematocrit
          -- ensure FiO2 is a valid number between 21-100
          -- mistakes are rare (<100 obs out of ~100,000)
          -- there are 862 obs of valuenum == 20 - some people round down!
          -- rather than risk imputing garbage data for FiO2, we simply NULL invalid values
          when itemid = 50816 and valuenum < 20 then null
          when itemid = 50816 and valuenum > 100 then null
          when itemid = 50817 and valuenum > 100 then null -- O2 sat
          when itemid = 50815 and valuenum >  70 then null -- O2 flow
          when itemid = 50821 and valuenum > 800 then null -- PO2
           -- conservative upper limit
        else valuenum
        end as valuenum

    from icustays ie
    left join labevents le
      on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
      and le.charttime between (ie.intime - interval '6' hour) and (ie.intime + interval '1' day)
      and le.ITEMID in
      -- blood gases
      (
        50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809
        , 50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819
        , 50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828
        , 51545
      )
)
select pvt.SUBJECT_ID, pvt.HADM_ID, pvt.ICUSTAY_ID, pvt.CHARTTIME
, max(case when label = 'SPECIMEN' then value else null end) as SPECIMEN
, max(case when label = 'AADO2' then valuenum else null end) as AADO2
, max(case when label = 'BASEEXCESS' then valuenum else null end) as BASEEXCESS
, max(case when label = 'BICARBONATE' then valuenum else null end) as BICARBONATE
, max(case when label = 'TOTALCO2' then valuenum else null end) as TOTALCO2
, max(case when label = 'CARBOXYHEMOGLOBIN' then valuenum else null end) as CARBOXYHEMOGLOBIN
, max(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE
, max(case when label = 'CALCIUM' then valuenum else null end) as CALCIUM
, max(case when label = 'GLUCOSE' then valuenum else null end) as GLUCOSE
, max(case when label = 'HEMATOCRIT' then valuenum else null end) as HEMATOCRIT
, max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN
, max(case when label = 'INTUBATED' then valuenum else null end) as INTUBATED
, max(case when label = 'LACTATE' then valuenum else null end) as LACTATE
, max(case when label = 'METHEMOGLOBIN' then valuenum else null end) as METHEMOGLOBIN
, max(case when label = 'O2FLOW' then valuenum else null end) as O2FLOW
, max(case when label = 'FIO2' then valuenum else null end) as FIO2
, max(case when label = 'SO2' then valuenum else null end) as SO2 -- OXYGENSATURATION
, max(case when label = 'PCO2' then valuenum else null end) as PCO2
, max(case when label = 'PEEP' then valuenum else null end) as PEEP
, max(case when label = 'PH' then valuenum else null end) as PH
, max(case when label = 'PO2' then valuenum else null end) as PO2
, max(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM
, max(case when label = 'REQUIREDO2' then valuenum else null end) as REQUIREDO2
, max(case when label = 'SODIUM' then valuenum else null end) as SODIUM
, max(case when label = 'TEMPERATURE' then valuenum else null end) as TEMPERATURE
, max(case when label = 'TIDALVOLUME' then valuenum else null end) as TIDALVOLUME
, max(case when label = 'VENTILATIONRATE' then valuenum else null end) as VENTILATIONRATE
, max(case when label = 'VENTILATOR' then valuenum else null end) as VENTILATOR
from pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
      """)

    bloodgasfirstday.createTempView("bloodgasfirstday")
    //sparkContext.sql("""  SELECT * from bloodgasfirstday limit 10""").show()
    //bloodgasfirstday.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("test/bloodgasfirstday.csv")


    // needs bloodfirstday and chartevents
    println("bloodgasfirstdayarterial starts")
    // it need icustays, labevents
    val bloodgasfirstdayarterial = sparkContext.sql("""
--DROP MATERIALIZED VIEW IF EXISTS bloodgasfirstdayarterial CASCADE;
--CREATE MATERIALIZED VIEW bloodgasfirstdayarterial AS
with stg_spo2 as
(
  select SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
    -- max here is just used to group SpO2 by charttime
    , max(case when valuenum <= 0 or valuenum > 100 then null else valuenum end) as SpO2
  from CHARTEVENTS
  -- o2 sat
  where ITEMID in
  (
    646 -- SpO2
  , 220277 -- O2 saturation pulseoxymetry
  )
  group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
)
, stg_fio2 as
(
  select SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
    -- pre-process the FiO2s to ensure they are between 21-100%
    , max(
        case
          when itemid = 223835
            then case
              when valuenum > 0 and valuenum <= 1
                then valuenum * 100
              -- improperly input data - looks like O2 flow in litres
              when valuenum > 1 and valuenum < 21
                then null
              when valuenum >= 21 and valuenum <= 100
                then valuenum
              else null end -- unphysiological
        when itemid in (3420, 3422)
        -- all these values are well formatted
            then valuenum
        when itemid = 190 and valuenum > 0.20 and valuenum < 1
        -- well formatted but not in %
            then valuenum * 100
      else null end
    ) as fio2_chartevents
  from CHARTEVENTS
  where ITEMID in
  (
    3420 -- FiO2
  , 190 -- FiO2 set
  , 223835 -- Inspired O2 Fraction (FiO2)
  , 3422 -- FiO2 [measured]
  )
  -- exclude rows marked as error
  --and error IS DISTINCT FROM 1
 -- and error != 1
  and (error = 0 or error is null)

  group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
)
, stg2 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.icustay_id, bg.charttime order by s1.charttime DESC) as lastRowSpO2
  , s1.spo2
from bloodgasfirstday bg
left join stg_spo2 s1
  -- same patient
  on  bg.icustay_id = s1.icustay_id
  -- spo2 occurred at most 2 hours before this blood gas
  and s1.charttime between bg.charttime - interval '2' hour and bg.charttime
where bg.po2 is not null
)
, stg3 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.icustay_id, bg.charttime order by s2.charttime DESC) as lastRowFiO2
  , s2.fio2_chartevents

  -- create our specimen prediction
  ,  1/(1+exp(-(-0.02544
  +    0.04598 * po2
  + coalesce(-0.15356 * spo2             , -0.15356 *   97.49420 +    0.13429)
  + coalesce( 0.00621 * fio2_chartevents ,  0.00621 *   51.49550 +   -0.24958)
  + coalesce( 0.10559 * hemoglobin       ,  0.10559 *   10.32307 +    0.05954)
  + coalesce( 0.13251 * so2              ,  0.13251 *   93.66539 +   -0.23172)
  + coalesce(-0.01511 * pco2             , -0.01511 *   42.08866 +   -0.01630)
  + coalesce( 0.01480 * fio2             ,  0.01480 *   63.97836 +   -0.31142)
  + coalesce(-0.00200 * aado2            , -0.00200 *  442.21186 +   -0.01328)
  + coalesce(-0.03220 * bicarbonate      , -0.03220 *   22.96894 +   -0.06535)
  + coalesce( 0.05384 * totalco2         ,  0.05384 *   24.72632 +   -0.01405)
  + coalesce( 0.08202 * lactate          ,  0.08202 *    3.06436 +    0.06038)
  + coalesce( 0.10956 * ph               ,  0.10956 *    7.36233 +   -0.00617)
  + coalesce( 0.00848 * o2flow           ,  0.00848 *    7.59362 +   -0.35803)
  ))) as SPECIMEN_PROB
from stg2 bg
left join stg_fio2 s2
  -- same patient
  on  bg.icustay_id = s2.icustay_id
  -- fio2 occurred at most 4 hours before this blood gas
  and s2.charttime between bg.charttime - interval '4' hour and bg.charttime
where bg.lastRowSpO2 = 1 -- only the row with the most recent SpO2 (if no SpO2 found lastRowSpO2 = 1)
)

select subject_id, hadm_id,
icustay_id, charttime
, SPECIMEN -- raw data indicating sample type, only present 80% of the time

-- prediction of specimen for missing data
, case
      when SPECIMEN is not null then SPECIMEN
      when SPECIMEN_PROB > 0.75 then 'ART'
    else null end as SPECIMEN_PRED
, SPECIMEN_PROB

-- oxygen related parameters
, SO2, spo2 -- note spo2 is from chartevents
, PO2, PCO2
, fio2_chartevents, FIO2
, AADO2
-- also calculate AADO2
, case
    when  PO2 is not null
      and pco2 is not null
      and coalesce(FIO2, fio2_chartevents) is not null
     -- multiple by 100 because FiO2 is in a % but should be a fraction
      then (coalesce(FIO2, fio2_chartevents)/100) * (760 - 47) - (pco2/0.8) - po2
    else null
  end as AADO2_calc
, case
    when PO2 is not null and coalesce(FIO2, fio2_chartevents) is not null
     -- multiply by 100 because FiO2 is in a % but should be a fraction
      then 100*PO2/(coalesce(FIO2, fio2_chartevents))
    else null
  end as PaO2FiO2
-- acid-base parameters
, PH, BASEEXCESS
, BICARBONATE, TOTALCO2

-- blood count parameters
, HEMATOCRIT
, HEMOGLOBIN
, CARBOXYHEMOGLOBIN
, METHEMOGLOBIN

-- chemistry
, CHLORIDE, CALCIUM
, TEMPERATURE
, POTASSIUM, SODIUM
, LACTATE
, GLUCOSE

-- ventilation stuff that's sometimes input
, INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
, PEEP, O2Flow
, REQUIREDO2

from stg3
where lastRowFiO2 = 1 -- only the most recent FiO2
-- restrict it to *only* arterial samples
and (SPECIMEN = 'ART' or SPECIMEN_PROB > 0.75)
order by icustay_id, charttime
      """)
    bloodgasfirstdayarterial.createTempView("bloodgasfirstdayarterial")
   //    sparkContext.sql("""SELECT * from bloodgasfirstdayarterial""").show()
   // bloodgasfirstdayarterial.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("test/bloodgasfirstdayarterial.csv")


    println("ventfirstday starts")
    /*========== ventfirstday ======*/
val ventfirstday = sparkContext.sql("""
    -- Determines if a patient is ventilated on the first day of their ICU stay.
    -- Creates a table with the result.
    -- Requires the `ventdurations` table, generated by ../ventilation-durations.sql
   -- DROP MATERIALIZED VIEW IF EXISTS ventfirstday CASCADE;
  --  CREATE MATERIALIZED VIEW ventfirstday AS
    select
    ie.subject_id, ie.hadm_id, ie.icustay_id
    -- if vd.icustay_id is not null, then they have a valid ventilation event
    -- in this case, we say they are ventilated
    -- otherwise, they are not
    , max(case
    when vd.icustay_id is not null then 1
    else 0 end) as vent
      from icustays ie
    left join ventdurations vd
      on ie.icustay_id = vd.icustay_id
    and
    (
      -- ventilation duration overlaps with ICU admission -> vented on admission
      (vd.starttime <= ie.intime and vd.endtime >= ie.intime)
    -- ventilation started during the first day
    OR (vd.starttime >= ie.intime and vd.starttime <= ie.intime + interval '1' day)
    )
    group by ie.subject_id, ie.hadm_id, ie.icustay_id
    order by ie.subject_id, ie.hadm_id, ie.icustay_id
""")
    ventfirstday.createTempView("ventfirstday")

    val bg = sparkContext.sql("""
        -- join blood gas to ventilation durations to determine if patient was vent
    -- also join to cpap table for the same purpose
    select bg.icustay_id, bg.charttime
    , PO2 as PaO2
    , AADO2
    -- row number indicating the highest AaDO2
    , case
      when  coalesce(FIO2, fio2_chartevents) is not null
    and vd.icustay_id is not null -- patient is ventilated
    and coalesce(FIO2, fio2_chartevents) >= 50
    then ROW_NUMBER() over (partition by bg.ICUSTAY_ID ORDER BY AADO2 DESC)
    else null end
      as aado2_rn
    , case
      when  coalesce(FIO2, fio2_chartevents) >= 50
        then null
        when vd.icustay_id is not null
    then null
    else ROW_NUMBER() over (partition by bg.ICUSTAY_ID ORDER BY PO2 DESC)
    end as pao2_rn

    from bloodgasfirstdayarterial bg
    left join ventdurations vd
      on bg.icustay_id = vd.icustay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
    """)
    bg.createTempView("bg")


    val acidbase = sparkContext.sql("""
        select bg.icustay_id
    , ph, pco2 as PACO2
    , case
      when ph is null or pco2 is null then null
    when ph < 7.20 then
    case
    when pco2 < 50 then 12
    else 4
    end
    when ph < 7.30 then
    case
    when pco2 < 30 then 9
    when pco2 < 40 then 6
    when pco2 < 50 then 3
    else 2
    end
    when ph < 7.35 then
    case
    when pco2 < 30 then 9
    when pco2 < 45 then 0
    else 1
    end
    when ph < 7.45 then
    case
    when pco2 < 30 then 5
    when pco2 < 45 then 0
    else 1
    end
    when ph < 7.50 then
    case
    when pco2 < 30 then 5
    when pco2 < 35 then 0
    when pco2 < 45 then 2
    else 12
    end
    when ph < 7.60 then
    case
    when pco2 < 40 then 3
    else 12
    end
    else -- ph >= 7.60
    case
    when pco2 < 25 then 0
    when pco2 < 40 then 3
    else 12
    end
    end as acidbase_score
    from bloodgasfirstdayarterial bg
    where ph is not null and pco2 is not null
   """)
    acidbase.createTempView("acidbase")

    val acidbase_max = sparkContext.sql("""
        select icustay_id, acidbase_score, ph, paco2
        -- create integer which indexes maximum value of score with 1
        , case
      when ph is not null and paco2 is not null
    then ROW_NUMBER() over (partition by ICUSTAY_ID ORDER BY ACIDBASE_SCORE DESC)
    else null
    end as acidbase_rn
    from acidbase
    -- define acute renal failure (ARF) as:
      --  creatinine >=1.5 mg/dl
      --  and urine output <410 cc/day
      --  and no chronic dialysis""")
    acidbase_max.createTempView("acidbase_max")
    val arf =  sparkContext.sql("""
        select ie.icustay_id
    , case
      when labs.creatinine_max >= 1.5
    and  uo.urineoutput < 410
    -- acute renal failure is only coded if the patient is not on chronic dialysis
    -- we use ICD-9 coding of ESRD as a proxy for chronic dialysis
      and  icd.ckd = 0
    then 1
    else 0 end as arf
      from icustays ie
    left join uofirstday uo
      on ie.icustay_id = uo.icustay_id
    left join labsfirstday labs
      on ie.icustay_id = labs.icustay_id
    left join
      (
        select hadm_id
        , max(case
    -- severe kidney failure requiring use of dialysis
      when icd9_code in  ('5854','5855','5856') then 1
    -- we do not include 5859 as that is sometimes coded for acute-on-chronic ARF
    else 0 end)
    as ckd
      from diagnoses_icd
      group by hadm_id
    ) icd
    on ie.hadm_id = icd.hadm_id
    """)
    arf.createTempView("arf")
    val cohort = sparkContext.sql("""
        select ie.subject_id, ie.hadm_id, ie.icustay_id
    , ie.intime
    , ie.outtime

    , vital.heartrate_min
    , vital.heartrate_max
    , vital.meanbp_min
    , vital.meanbp_max
    , vital.tempc_min
    , vital.tempc_max
    , vital.resprate_min
    , vital.resprate_max

    , pa.PaO2
    , aa.AaDO2

    , ab.ph
    , ab.paco2
    , ab.acidbase_score

    , labs.hematocrit_min
    , labs.hematocrit_max
    , labs.wbc_min
    , labs.wbc_max
    , labs.creatinine_min
    , labs.creatinine_max
    , labs.bun_min
    , labs.bun_max
    , labs.sodium_min
    , labs.sodium_max
    , labs.albumin_min
    , labs.albumin_max
    , labs.bilirubin_min
    , labs.bilirubin_max

    , case
      when labs.glucose_max is null and vital.glucose_max is null
    then null
    when labs.glucose_max is null or vital.glucose_max > labs.glucose_max
    then vital.glucose_max
    when vital.glucose_max is null or labs.glucose_max > vital.glucose_max
    then labs.glucose_max
    else labs.glucose_max -- if equal, just pick labs
    end as glucose_max

    , case
      when labs.glucose_min is null and vital.glucose_min is null
    then null
    when labs.glucose_min is null or vital.glucose_min < labs.glucose_min
    then vital.glucose_min
    when vital.glucose_min is null or labs.glucose_min < vital.glucose_min
    then labs.glucose_min
    else labs.glucose_min -- if equal, just pick labs
    end as glucose_min

    -- , labs.bicarbonate_min
    -- , labs.bicarbonate_max
    , vent.vent
    , uo.urineoutput
    -- gcs and its components
    , gcs.mingcs
    , gcs.gcsmotor, gcs.gcsverbal,  gcs.gcseyes, gcs.endotrachflag
    -- acute renal failure
    , arf.arf as arf

    from icustays ie
    inner join admissions adm
      on ie.hadm_id = adm.hadm_id
    inner join patients pat
      on ie.subject_id = pat.subject_id

    -- join to above views - the row number filters to 1 row per ICUSTAY_ID
      left join bg pa
      on  ie.icustay_id = pa.icustay_id
    and pa.pao2_rn = 1
    left join bg aa
      on  ie.icustay_id = aa.icustay_id
    and aa.aado2_rn = 1
    left join acidbase_max ab
      on  ie.icustay_id = ab.icustay_id
    and ab.acidbase_rn = 1
    left join arf
    on ie.icustay_id = arf.icustay_id

    -- join to custom tables to get more data....
    left join ventfirstday vent
      on ie.icustay_id = vent.icustay_id
    left join gcsfirstday gcs
      on ie.icustay_id = gcs.icustay_id
    left join vitalsfirstday vital
      on ie.icustay_id = vital.icustay_id
    left join uofirstday uo
      on ie.icustay_id = uo.icustay_id
    left join labsfirstday labs
      on ie.icustay_id = labs.icustay_id
   """ )
    cohort.createTempView("cohort")

    //First, we calculate the score for the minimum values
    val score_min = sparkContext
        .sql(""" select cohort.subject_id, cohort.hadm_id, cohort.icustay_id
    , case
      when heartrate_min is null then null
    when heartrate_min <   40 then 8
    when heartrate_min <   50 then 5
    when heartrate_min <  100 then 0
    when heartrate_min <  110 then 1
    when heartrate_min <  120 then 5
    when heartrate_min <  140 then 7
    when heartrate_min <  155 then 13
    when heartrate_min >= 155 then 17
    end as hr_score

    , case
      when meanbp_min is null then null
    when meanbp_min <   40 then 23
    when meanbp_min <   60 then 15
    when meanbp_min <   70 then 7
    when meanbp_min <   80 then 6
    when meanbp_min <  100 then 0
    when meanbp_min <  120 then 4
    when meanbp_min <  130 then 7
    when meanbp_min <  140 then 9
    when meanbp_min >= 140 then 10
    end as meanbp_score

    -- TODO: add 1 degree to axillary measurements
    , case
      when tempc_min is null then null
    when tempc_min <  33.0 then 20
    when tempc_min <  33.5 then 16
    when tempc_min <  34.0 then 13
    when tempc_min <  35.0 then 8
    when tempc_min <  36.0 then 2
    when tempc_min <  40.0 then 0
    when tempc_min >= 40.0 then 4
    end as temp_score

    , case
      when resprate_min is null then null
    -- special case for ventilated patients
      when vent = 1 and resprate_min < 14 then 0
    when resprate_min <   6 then 17
    when resprate_min <  12 then 8
    when resprate_min <  14 then 7
    when resprate_min <  25 then 0
    when resprate_min <  35 then 6
    when resprate_min <  40 then 9
    when resprate_min <  50 then 11
    when resprate_min >= 50 then 18
    end as resprate_score

    , case
      when hematocrit_min is null then null
    when hematocrit_min <   41.0 then 3
    when hematocrit_min <   50.0 then 0
    when hematocrit_min >=  50.0 then 3
    end as hematocrit_score

    , case
      when wbc_min is null then null
    when wbc_min <   1.0 then 19
    when wbc_min <   3.0 then 5
    when wbc_min <  20.0 then 0
    when wbc_min <  25.0 then 1
    when wbc_min >= 25.0 then 5
    end as wbc_score

    , case
      when creatinine_min is null then null
    when arf = 1 and creatinine_min <  1.5 then 0
    when arf = 1 and creatinine_min >= 1.5 then 10
    when creatinine_min <   0.5 then 3
    when creatinine_min <   1.5 then 0
    when creatinine_min <  1.95 then 4
    when creatinine_min >= 1.95 then 7
    end as creatinine_score

    , case
      when bun_min is null then null
    when bun_min <  17.0 then 0
    when bun_min <  20.0 then 2
    when bun_min <  40.0 then 7
    when bun_min <  80.0 then 11
    when bun_min >= 80.0 then 12
    end as bun_score

    , case
      when sodium_min is null then null
    when sodium_min <  120 then 3
    when sodium_min <  135 then 2
    when sodium_min <  155 then 0
    when sodium_min >= 155 then 4
    end as sodium_score

    , case
      when albumin_min is null then null
    when albumin_min <  2.0 then 11
    when albumin_min <  2.5 then 6
    when albumin_min <  4.5 then 0
    when albumin_min >= 4.5 then 4
    end as albumin_score

    , case
      when bilirubin_min is null then null
    when bilirubin_min <  2.0 then 0
    when bilirubin_min <  3.0 then 5
    when bilirubin_min <  5.0 then 6
    when bilirubin_min <  8.0 then 8
    when bilirubin_min >= 8.0 then 16
    end as bilirubin_score

    , case
      when glucose_min is null then null
    when glucose_min <   40 then 8
    when glucose_min <   60 then 9
    when glucose_min <  200 then 0
    when glucose_min <  350 then 3
    when glucose_min >= 350 then 5
    end as glucose_score

    from cohort
    """)
    score_min.createTempView("score_min")

    val  score_max = sparkContext.sql("""
        select cohort.subject_id, cohort.hadm_id, cohort.icustay_id
    , case
      when heartrate_max is null then null
    when heartrate_max <   40 then 8
    when heartrate_max <   50 then 5
    when heartrate_max <  100 then 0
    when heartrate_max <  110 then 1
    when heartrate_max <  120 then 5
    when heartrate_max <  140 then 7
    when heartrate_max <  155 then 13
    when heartrate_max >= 155 then 17
    end as hr_score

    , case
      when meanbp_max is null then null
    when meanbp_max <   40 then 23
    when meanbp_max <   60 then 15
    when meanbp_max <   70 then 7
    when meanbp_max <   80 then 6
    when meanbp_max <  100 then 0
    when meanbp_max <  120 then 4
    when meanbp_max <  130 then 7
    when meanbp_max <  140 then 9
    when meanbp_max >= 140 then 10
    end as meanbp_score

    -- TODO: add 1 degree to axillary measurements
    , case
      when tempc_max is null then null
    when tempc_max <  33.0 then 20
    when tempc_max <  33.5 then 16
    when tempc_max <  34.0 then 13
    when tempc_max <  35.0 then 8
    when tempc_max <  36.0 then 2
    when tempc_max <  40.0 then 0
    when tempc_max >= 40.0 then 4
    end as temp_score

    , case
      when resprate_max is null then null
    -- special case for ventilated patients
      when vent = 1 and resprate_max < 14 then 0
    when resprate_max <   6 then 17
    when resprate_max <  12 then 8
    when resprate_max <  14 then 7
    when resprate_max <  25 then 0
    when resprate_max <  35 then 6
    when resprate_max <  40 then 9
    when resprate_max <  50 then 11
    when resprate_max >= 50 then 18
    end as resprate_score

    , case
      when hematocrit_max is null then null
    when hematocrit_max <   41.0 then 3
    when hematocrit_max <   50.0 then 0
    when hematocrit_max >=  50.0 then 3
    end as hematocrit_score

    , case
      when wbc_max is null then null
    when wbc_max <   1.0 then 19
    when wbc_max <   3.0 then 5
    when wbc_max <  20.0 then 0
    when wbc_max <  25.0 then 1
    when wbc_max >= 25.0 then 5
    end as wbc_score

    , case
      when creatinine_max is null then null
    when arf = 1 and creatinine_max <  1.5 then 0
    when arf = 1 and creatinine_max >= 1.5 then 10
    when creatinine_max <   0.5 then 3
    when creatinine_max <   1.5 then 0
    when creatinine_max <  1.95 then 4
    when creatinine_max >= 1.95 then 7
    end as creatinine_score

    , case
      when bun_max is null then null
    when bun_max <  17.0 then 0
    when bun_max <  20.0 then 2
    when bun_max <  40.0 then 7
    when bun_max <  80.0 then 11
    when bun_max >= 80.0 then 12
    end as bun_score

    , case
      when sodium_max is null then null
    when sodium_max <  120 then 3
    when sodium_max <  135 then 2
    when sodium_max <  155 then 0
    when sodium_max >= 155 then 4
    end as sodium_score

    , case
      when albumin_max is null then null
    when albumin_max <  2.0 then 11
    when albumin_max <  2.5 then 6
    when albumin_max <  4.5 then 0
    when albumin_max >= 4.5 then 4
    end as albumin_score

    , case
      when bilirubin_max is null then null
    when bilirubin_max <  2.0 then 0
    when bilirubin_max <  3.0 then 5
    when bilirubin_max <  5.0 then 6
    when bilirubin_max <  8.0 then 8
    when bilirubin_max >= 8.0 then 16
    end as bilirubin_score

    , case
      when glucose_max is null then null
    when glucose_max <   40 then 8
    when glucose_max <   60 then 9
    when glucose_max <  200 then 0
    when glucose_max <  350 then 3
    when glucose_max >= 350 then 5
    end as glucose_score
    from cohort
    """)
    score_max.createTempView("score_max")
   val  scorecomp = sparkContext.sql("""
    -- Combine together the scores for min/max, using the following rules:
      --  1) select the value furthest from a predefined normal value
    --  2) if both equidistant, choose the one which gives a worse score
      --  3) calculate score for acid-base abnormalities as it requires interactions
      -- sometimes the code is a bit redundant, i.e. we know the max would always be furthest from 0
        select co.*
    -- The rules for APS III require the definition of a "worst" value
      -- This value is defined as whatever value is furthest from a predefined normal
      -- e.g., for heart rate, worst is defined as furthest from 75
    , case
      when heartrate_max is null then null
    when abs(heartrate_max-75) > abs(heartrate_min-75)
    then smax.hr_score
    when abs(heartrate_max-75) < abs(heartrate_min-75)
    then smin.hr_score
    when abs(heartrate_max-75) = abs(heartrate_min-75)
    and  smax.hr_score >= smin.hr_score
    then smax.hr_score
    when abs(heartrate_max-75) = abs(heartrate_min-75)
    and  smax.hr_score < smin.hr_score
    then smin.hr_score
    end as hr_score

    , case
      when meanbp_max is null then null
    when abs(meanbp_max-90) > abs(meanbp_min-90)
    then smax.meanbp_score
    when abs(meanbp_max-90) < abs(meanbp_min-90)
    then smin.meanbp_score
    -- values are equidistant - pick the larger score
      when abs(meanbp_max-90) = abs(meanbp_min-90)
    and  smax.meanbp_score >= smin.meanbp_score
    then smax.meanbp_score
    when abs(meanbp_max-90) = abs(meanbp_min-90)
    and  smax.meanbp_score < smin.meanbp_score
    then smin.meanbp_score
    end as meanbp_score

    , case
      when tempc_max is null then null
    when abs(tempc_max-38) > abs(tempc_min-38)
    then smax.temp_score
    when abs(tempc_max-38) < abs(tempc_min-38)
    then smin.temp_score
    -- values are equidistant - pick the larger score
      when abs(tempc_max-38) = abs(tempc_min-38)
    and  smax.temp_score >= smin.temp_score
    then smax.temp_score
    when abs(tempc_max-38) = abs(tempc_min-38)
    and  smax.temp_score < smin.temp_score
    then smin.temp_score
    end as temp_score

    , case
      when resprate_max is null then null
    when abs(resprate_max-19) > abs(resprate_min-19)
    then smax.resprate_score
    when abs(resprate_max-19) < abs(resprate_min-19)
    then smin.resprate_score
    -- values are equidistant - pick the larger score
      when abs(resprate_max-19) = abs(resprate_max-19)
    and  smax.resprate_score >= smin.resprate_score
    then smax.resprate_score
    when abs(resprate_max-19) = abs(resprate_max-19)
    and  smax.resprate_score < smin.resprate_score
    then smin.resprate_score
    end as resprate_score

    , case
      when hematocrit_max is null then null
    when abs(hematocrit_max-45.5) > abs(hematocrit_min-45.5)
    then smax.hematocrit_score
    when abs(hematocrit_max-45.5) < abs(hematocrit_min-45.5)
    then smin.hematocrit_score
    -- values are equidistant - pick the larger score
      when abs(hematocrit_max-45.5) = abs(hematocrit_max-45.5)
    and  smax.hematocrit_score >= smin.hematocrit_score
    then smax.hematocrit_score
    when abs(hematocrit_max-45.5) = abs(hematocrit_max-45.5)
    and  smax.hematocrit_score < smin.hematocrit_score
    then smin.hematocrit_score
    end as hematocrit_score

    , case
      when wbc_max is null then null
    when abs(wbc_max-11.5) > abs(wbc_min-11.5)
    then smax.wbc_score
    when abs(wbc_max-11.5) < abs(wbc_min-11.5)
    then smin.wbc_score
    -- values are equidistant - pick the larger score
      when abs(wbc_max-11.5) = abs(wbc_max-11.5)
    and  smax.wbc_score >= smin.wbc_score
    then smax.wbc_score
    when abs(wbc_max-11.5) = abs(wbc_max-11.5)
    and  smax.wbc_score < smin.wbc_score
    then smin.wbc_score
    end as wbc_score


    -- For some labs, "furthest from normal" doesn't make sense
    -- e.g. creatinine w/ ARF, the minimum could be 0.3, and the max 1.6
    -- while the minimum of 0.3 is "further from 1", seems like the max should be scored

    , case
      when creatinine_max is null then null
    -- if they have arf then use the max to score
    when arf = 1 then smax.creatinine_score
    -- otherwise furthest from 1
    when abs(creatinine_max-1) > abs(creatinine_min-1)
    then smax.creatinine_score
    when abs(creatinine_max-1) < abs(creatinine_min-1)
    then smin.creatinine_score
    -- values are equidistant
      when smax.creatinine_score >= smin.creatinine_score
    then smax.creatinine_score
    when smax.creatinine_score < smin.creatinine_score
    then smin.creatinine_score
    end as creatinine_score

    -- the rule for BUN is the furthest from 0.. equivalent to the max value
    , case
      when bun_max is null then null
    else smax.bun_score
    end as bun_score

    , case
      when sodium_max is null then null
    when abs(sodium_max-145.5) > abs(sodium_min-145.5)
    then smax.sodium_score
    when abs(sodium_max-145.5) < abs(sodium_min-145.5)
    then smin.sodium_score
    -- values are equidistant - pick the larger score
      when abs(sodium_max-145.5) = abs(sodium_max-145.5)
    and  smax.sodium_score >= smin.sodium_score
    then smax.sodium_score
    when abs(sodium_max-145.5) = abs(sodium_max-145.5)
    and  smax.sodium_score < smin.sodium_score
    then smin.sodium_score
    end as sodium_score

    , case
      when albumin_max is null then null
    when abs(albumin_max-3.5) > abs(albumin_min-3.5)
    then smax.albumin_score
    when abs(albumin_max-3.5) < abs(albumin_min-3.5)
    then smin.albumin_score
    -- values are equidistant - pick the larger score
      when abs(albumin_max-3.5) = abs(albumin_max-3.5)
    and  smax.albumin_score >= smin.albumin_score
    then smax.albumin_score
    when abs(albumin_max-3.5) = abs(albumin_max-3.5)
    and  smax.albumin_score < smin.albumin_score
    then smin.albumin_score
    end as albumin_score

    , case
      when bilirubin_max is null then null
    else smax.bilirubin_score
    end as bilirubin_score

    , case
      when glucose_max is null then null
    when abs(glucose_max-130) > abs(glucose_min-130)
    then smax.glucose_score
    when abs(glucose_max-130) < abs(glucose_min-130)
    then smin.glucose_score
    -- values are equidistant - pick the larger score
      when abs(glucose_max-130) = abs(glucose_max-130)
    and  smax.glucose_score >= smin.glucose_score
    then smax.glucose_score
    when abs(glucose_max-130) = abs(glucose_max-130)
    and  smax.glucose_score < smin.glucose_score
    then smin.glucose_score
    end as glucose_score


    -- Below are interactions/special cases where only 1 value is important
    , case
      when urineoutput is null then null
    when urineoutput <   400 then 15
    when urineoutput <   600 then 8
    when urineoutput <   900 then 7
    when urineoutput <  1500 then 5
    when urineoutput <  2000 then 4
    when urineoutput <  4000 then 0
    when urineoutput >= 4000 then 1
    end as uo_score

    , case
      when endotrachflag = 1
    -- here they are intubated, so their verbal score is inappropriate
      -- normally you are supposed to use "clinical judgement"
    -- we don't have that, so we just assume normal (as was done in the original study)
    then 0
    when gcseyes = 1
    then case
    when gcsverbal = 1 and gcsmotor in (1,2)
    then 48
    when gcsverbal = 1 and gcsmotor in (3,4)
    then 33
    when gcsverbal = 1 and gcsmotor in (5,6)
    then 16
    when gcsverbal in (2,3) and gcsmotor in (1,2)
    then 29
    when gcsverbal in (2,3) and gcsmotor in (3,4)
    then 24
    when gcsverbal in (2,3) and gcsmotor >= 5
    -- highly unlikely clinical combination
    then null
    when gcsverbal >= 4
    then null
    end
    when gcseyes > 1
    then case
    when gcsverbal = 1 and gcsmotor in (1,2)
    then 29
    when gcsverbal = 1 and gcsmotor in (3,4)
    then 24
    when gcsverbal = 1 and gcsmotor in (5,6)
    then 15
    when gcsverbal in (2,3) and gcsmotor in (1,2)
    then 29
    when gcsverbal in (2,3) and gcsmotor in (3,4)
    then 24
    when gcsverbal in (2,3) and gcsmotor = 5
    then 13
    when gcsverbal in (2,3) and gcsmotor = 6
    then 10
    when gcsverbal = 4 and gcsmotor in (1,2,3,4)
    then 13
    when gcsverbal = 4 and gcsmotor = 5
    then 8
    when gcsverbal = 4 and gcsmotor = 6
    then 3
    when gcsverbal = 5 and gcsmotor in (1,2,3,4,5)
    then 3
    when gcsverbal = 5 and gcsmotor = 6
    then 0
    end
    else null
    end as gcs_score

    , case
      when PaO2 is null and AaDO2 is null
    then null
    when PaO2 is not null then
    case
    when PaO2 < 50 then 15
    when PaO2 < 70 then 5
    when PaO2 < 80 then 2
    else 0 end
      when AaDO2 is not null then
    case
    when AaDO2 <  100 then 0
    when AaDO2 <  250 then 7
    when AaDO2 <  350 then 9
    when AaDO2 <  500 then 11
    when AaDO2 >= 500 then 14
    else 0 end
      end as pao2_aado2_score

    from cohort co
    left join score_min smin
      on co.icustay_id = smin.icustay_id
    left join score_max smax
      on co.icustay_id = smax.icustay_id
    """)
    scorecomp.createTempView("scorecomp")

    val score = sparkContext.sql("""
        select s.*
    -- coalesce statements impute normal score of zero if data element is missing
    , coalesce(hr_score,0)
    + coalesce(meanbp_score,0)
    + coalesce(temp_score,0)
    + coalesce(resprate_score,0)
    + coalesce(pao2_aado2_score,0)
    + coalesce(hematocrit_score,0)
    + coalesce(wbc_score,0)
    + coalesce(creatinine_score,0)
    + coalesce(uo_score,0)
    + coalesce(bun_score,0)
    + coalesce(sodium_score,0)
    + coalesce(albumin_score,0)
    + coalesce(bilirubin_score,0)
    + coalesce(glucose_score,0)
    + coalesce(acidbase_score,0)
    + coalesce(gcs_score,0)
    as APSIII
      from scorecomp s
    """)
    score.createTempView("score")
    sparkContext.sql("""drop view bloodgasfirstday""")
    sparkContext.sql("""drop view bloodgasfirstdayarterial""")
    sparkContext.sql(""" drop view  labsfirstday""")
    sparkContext.sql(""" drop view  vitalsfirstday""")
    sparkContext.sql(""" drop view  gcsfirstday""")
    sparkContext.sql(""" drop view  ventsettings""")
    sparkContext.sql(""" drop view  uofirstday""")
    sparkContext.sql(""" drop view  ventdurations""")
    sparkContext.sql(""" drop view  ventfirstday""")
    sparkContext.sql(""" drop view  bg """)
    sparkContext.sql(""" drop view  acidbase """)
    sparkContext.sql(""" drop view  acidbase_max""")
    sparkContext.sql(""" drop view  arf""")
    sparkContext.sql(""" drop view  cohort""")
    sparkContext.sql(""" drop view  score_min""")
    sparkContext.sql(""" drop view  score_max""")
    println("apsiii starts")
    val apsiii = sparkContext.sql(
      """
-- ------------------------------------------------------------------
-- Title: Acute Physiology Score III (APS III)
-- This query extracts the acute physiology score III.
-- This score is a measure of patient severity of illness.
-- The score is calculated on the first day of each ICU patients' stay.
-- ------------------------------------------------------------------

-- Reference for APS III:
--    Knaus WA, Wagner DP, Draper EA, Zimmerman JE, Bergner M, Bastos PG, Sirio CA, Murphy DJ, Lotring T, Damiano A.
--    The APACHE III prognostic system. Risk prediction of hospital mortality for critically ill hospitalized adults.
--    Chest Journal. 1991 Dec 1;100(6):1619-36.

-- Reference for the equation for calibrating APS III to hospital mortality:
--    Johnson, A. E. W. (2015). Mortality prediction and acuity assessment in critical care.
--    University of Oxford, Oxford, UK.

-- Variables used in APS III:
--  GCS
--  VITALS: Heart rate, mean blood pressure, temperature, respiration rate
--  FLAGS: ventilation/cpap, chronic dialysis
--  IO: urine output
--  LABS: PaO2, A-aDO2, hematocrit, WBC, creatinine
--        , blood urea nitrogen, sodium, albumin, bilirubin, glucose, pH, pCO2

-- The following views are required to run this query:
--  1) uofirstday - generated by urine-output-first-day.sql
--  2) ventfirstday - generated by ventilated-first-day.sql
--  3) vitalsfirstday - generated by vitals-first-day.sql
--  4) gcsfirstday - generated by gcs-first-day.sql
--  5) labsfirstday - generated by labs-first-day.sql

-- Note:
--  The score is calculated for *all* ICU patients, with the assumption that the user will subselect appropriate ICUSTAY_IDs.
--  For example, the score is calculated for neonates, but it is likely inappropriate to actually use the score values for these patients.

-- List of TODO:
-- The site of temperature is not incorporated. Axillary measurements should be increased by 1 degree.
-- Unfortunately the data for metavision is not available at the moment.
--  674 | Temp. Site
--  224642 | Temperature Site

--DROP MATERIALIZED VIEW IF EXISTS APSIII CASCADE;
--CREATE MATERIALIZED VIEW APSIII as

-- because ph/pco2 rules are an interaction *within* a blood gas, we calculate them here
-- the worse score is then taken for the final calculation


-- tabulate the APS III using the scores from the worst values

select ie.subject_id, ie.hadm_id, ie.icustay_id
, APSIII
-- Calculate probability of hospital mortality using equation from Johnson 2014.
, 1 / (1 + exp(- (-4.4360 + 0.04726*(APSIII) ))) as APSIII_PROB
, hr_score
, meanbp_score
, temp_score
, resprate_score
, pao2_aado2_score
, hematocrit_score
, wbc_score
, creatinine_score
, uo_score
, bun_score
, sodium_score
, albumin_score
, bilirubin_score
, glucose_score
, acidbase_score
, gcs_score
from icustays ie
left join score s
  on ie.icustay_id = s.icustay_id
order by ie.icustay_id
      """)
apsiii.createTempView("apsiii")

apsiii.select("SUBJECT_ID","HADM_ID", "ICUSTAY_ID","APSIII")
      // .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("dataset/apsiii.csv")
 //   sparkContext.sql(SELECT * from apaciii""").show(2,false)




   merge("dataset/apsiii", "datset/mergedapsiii")




    sparkContext.stop

    //sc.stop
  }

/*
  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")
  def createContext: SparkContext = createContext("CSE 6250 ", "local")

*/






}
