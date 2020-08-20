
package edu.gatech.cse6250.main

import edu.gatech.cse6250.ioutils.CSVUtils.{registerNoteTable, registerTable}
import edu.gatech.cse6250.pattern.Match.extractDate
import edu.gatech.cse6250.testing.{TestMeasures, TestMeasuresSVM}
import edu.gatech.cse6250.training.{TrainMeasures, TrainMeasuresSVM}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.{LDA, LocalLDAModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, _}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}


object AllNote {
  def main(args: Array[String]) {
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val sparkSession = SparkSession.builder()
      .appName("Spark In Action")
      .master("local[*]")
      .config("spark.driver.maxResultSize", "2g")
       .config("spark.network.timeout", "100000000s")
      .getOrCreate()


    sparkSession.sql("set spark.sql.caseSensitive=false")
    //sparkSession.sparkContext.set("spark.driver.maxResultSize", "2g")//val path = "data/"
    val path = "dataset/"
    // Remove """ in LABEVENTS.CSV
    // READ the files from scv and create a corresponding table
    /* ------------------------------notes--------------------------------*/

    println("patients  starts")
    val patientsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType, true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("GENDER", StringType, true),
      StructField("DOB", TimestampType, true),
      StructField("DOD", TimestampType, true),
      StructField("DOD_HOSP", TimestampType, true),
      StructField("DOD_SSN", TimestampType, true),
      StructField("EXPIRE_FLAG", IntegerType, true)
    ))
    registerTable(sparkSession, path, "PATIENTS.csv", "patients", patientsSchema)


    val default_stopWord = StopWordsRemover
      .loadDefaultStopWords("english").toSet

    var stopWord_web = sparkSession.read.textFile("dataset/StopWordList.txt")
      .collect()
    val stopWord_web_set = stopWord_web.toSet
    //val broadcastStopWords = sparkContext.sparkContext.broadcast(stopWords_diff)
    val additional_words = Set("javascript", "webtag", "popup", "ml")
    val all_stop_words = stopWord_web_set.union(default_stopWord).union(additional_words).toArray
    sparkSession.udf.register("combine", (arrayofrecords: Seq[String]) => arrayofrecords.mkString(" ")
    )

    println("icu stay starts")
    /* --------------------------------icustays------------------------------*/
    val icustaysSchema = StructType(Array(
      StructField("ROW_ID", IntegerType, true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("DBSOURCE", StringType, true),
      StructField("FIRST_CAREUNIT", StringType, true),
      StructField("LAST_CAREUNIT", StringType, true),
      StructField("FIRST_WARDID", IntegerType, true),
      StructField("LAST_WARDID", IntegerType, true),
      StructField("INTIME", TimestampType, true),
      StructField("OUTTIME", TimestampType, true),
      StructField("LOS", DoubleType, true)
    ))
    registerTable(sparkSession, path, "ICUSTAYS.csv", "icustays", icustaysSchema)


    println("admissions starts")
    /* ---------------------------------admissions-----------------------------*/
    val admissionsSchema = StructType(Array(
      StructField("ROW_ID", IntegerType, true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ADMITTIME", TimestampType, true),
      StructField("DISCHTIME", TimestampType, true),
      StructField("DEATHTIME", TimestampType, true),
      StructField("ADMISSION_TYPE", StringType, true),
      StructField("ADMISSION_LOCATION", StringType, true),
      StructField("DISCHARGE_LOCATION", StringType, true),
      StructField("INSURANCE", StringType, true),
      StructField("LANGUAGE", StringType, true),
      StructField("RELIGION", StringType, true),
      StructField("MARITAL_STATUS", StringType, true),
      StructField("ETHNICITY", StringType, true),
      StructField("EDREGTIME", TimestampType, true),
      StructField("EDOUTTIME", TimestampType, true),
      StructField("DIAGNOSIS", StringType, true),
      StructField("HOSPITAL_EXPIRE_FLAG", IntegerType, true),
      StructField("HAS_CHARTEVENTS_DATA", IntegerType, true)

    ))
    registerTable(sparkSession, path, "ADMISSIONS.csv", "admissions", admissionsSchema)


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
    registerNoteTable(sparkSession, path, "new2.csv", "noteevents", notesSchema)


    println("sapssii starts")
    /* ---------------------------------sapsii-----------------------------*/
    val sapsiiSchema = StructType(Array(
      //StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("SAPSII", IntegerType, true)

    ))
    registerTable(sparkSession, path, "sapsii-folder/part-00000-606817c0-82eb-44b7-8d49-ebaef6a2254c-c000.csv", "sapsii", sapsiiSchema)

    sparkSession.sql("""  SELECT * from sapsii""").show(2, false)



    // subject_id,hadm_id,icustay_id,SOFA

    /* ---------------------------------sofa -----------------------------*/

    println("fetching sofa")
    val sofaSchema = StructType(Array(
      //StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("SOFA", IntegerType, true)
    ))
    registerTable(sparkSession, path, "sofa-folder/part-00000-3928c5e2-7de1-4c41-9642-b277978c89b8-c000.csv", "sofa", sofaSchema)

    sparkSession.sql("""  SELECT * from sofa""").show(2, false)

    /* --------------------------------- oasis-----------------------------*/

    println("fetching oasis")
    val oasisSchema = StructType(Array(
      //StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
      StructField("ICUSTAY_AGE_GROUP", StringType, true),
      StructField("HOSPITAL_EXPIRE_FLAG", IntegerType, true),
      StructField("ICUSTAY_EXPIRE_FLAG", IntegerType, true),
      StructField("OASIS", IntegerType, true)
    ))
    registerTable(sparkSession, path, "oasis.csv/part-00000-544eb43f-194c-4ea7-8b12-8591fdf523cc-c000.csv", "oasis", oasisSchema)
    sparkSession.sql("""  SELECT * from oasis""").show(2, false)

    /* --------------------------------- apsiii -----------------------------*/
    println("fetching apsiii")
    val  apsiiiSchema = StructType(Array(
      //StructField("ROW_ID", IntegerType,true),
      StructField("SUBJECT_ID", IntegerType, true),
      StructField("HADM_ID", IntegerType, true),
      StructField("ICUSTAY_ID", IntegerType, true),
   //   StructField("ICUSTAY_AGE_GROUP", StringType, true),
    //  StructField("HOSPITAL_EXPIRE_FLAG", IntegerType, true),
     // StructField("ICUSTAY_EXPIRE_FLAG", IntegerType, true),
      StructField("APSIII", IntegerType, true)
    ))
    registerTable(sparkSession,path, "mergedaspiii/outputapsiii.csv","apsiii",apsiiiSchema)

    sparkSession.sql("""  SELECT * from apsiii""")show(2,false)
    // println(test.count())

    println("icu details starts")
    val icustay_details = sparkSession.sql(
      """
    SELECT ie.subject_id, ie.hadm_id, ie.icustay_id
    -- patient level factors
      , pat.gender, pat.dod
    -- hospital level factors
    , adm.admittime, adm.dischtime
    , ((unix_timestamp(adm.dischtime) - unix_timestamp(adm.admittime))/(60*60*24)) AS los_hospital
    , (( unix_timestamp(adm.admittime) - unix_timestamp(pat.dob))/(60*60*24*365.242))  AS admission_age
    , adm.ethnicity, adm.admission_type
    , adm.hospital_expire_flag
    , DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq
    , CASE
    WHEN DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1 THEN 1
    ELSE 0 END AS first_hosp_stay
    -- icu level factors
    , ie.intime, ie.outtime
    , ((unix_timestamp( ie.outtime) - unix_timestamp(ie.intime))/(60*60*24)) AS los_icu
    , DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq
    -- first ICU stay *for the current hospitalization*
    , CASE
       WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN 1
       ELSE 0 END AS first_icu_stay
    FROM icustays ie
      INNER JOIN admissions adm
         ON ie.hadm_id = adm.hadm_id
      INNER JOIN patients pat
          ON ie.subject_id = pat.subject_id
      WHERE adm.has_chartevents_data = 1
      ORDER BY ie.subject_id, adm.admittime, ie.intime
    """).createTempView("icustaydetails") //show(2,false)
    /*
    sparkSession.sql(
      """SELECT * from  icustaydetails
                  WHERE subject_id <=1000""").show(2, false)

    */
    val patients_records = sparkSession.sql(
      """SELECT * from  patients
                 -- WHERE subject_id <=1000
      """)


    val Array(training, testing) = patients_records.randomSplit(Array(0.7, 0.3), seed = 6250L)
    training.createTempView("training")
    testing.createTempView("testing")
    //If the patient is in train test then train is set to 1 else is set to 0


    sparkSession.sql(
      """SELECT *, 1 as train FROM training
            UNION
            SELECT *, 0 as train FROM testing
         """).createTempView("allrecords")
    //.show(2,false)

    val ExtractDate = udf(extractDate _)
    sparkSession.udf.register("ExtractDate", ExtractDate)
    // retrive missing chart time
    val notes_records1 = sparkSession.sql("""
                                            SELECT
                                             noteevents.*,
                                             icustays.icustay_id,
                                             icustays.outtime
                                             FROM noteevents
                                             JOIN icustays ON
                                             noteevents.hadm_id = icustays.hadm_id AND
                                             noteevents.subject_id = icustays.subject_id  AND
                                             unix_timestamp(noteevents.chartdate) <= unix_timestamp(icustays.outtime)
                                             """).createTempView("noteevents_new")
    // sparkSession.sql("""SELECT  chartdate, outtime from  noteevents_new """).show(10,false)
    //println("here")
    val notes_records = sparkSession.sql(
      """
                                           WITH subset AS (
                                             SELECT
                                             noteevents_new.subject_id,
                                             noteevents_new.hadm_id,
                                             noteevents_new.icustay_id,
                                             LTRIM(RTRIM(combine(collect_list(lower(noteevents_new.text))) )) as notescollection
                                             FROM noteevents_new
                                             WHERE
                                             NOT (noteevents_new.storetime is null)
                                             AND
                                             -- if hamid is null it means the patient is not admitted to ICU
                                             NOT (noteevents_new.hadm_id is null)
                                             AND
                                             iserror is null OR iserror <> '1'
                                             AND
                                             lower(CATEGORY) <> "discharge summary"
                                            -- AND
                                            -- noteevents_new.subject_id <= 1000
                                             GROUP BY noteevents_new.subject_id ,noteevents_new.icustay_id, noteevents_new.hadm_id
                                             )
                                             SELECT subset.*, allrecords.train, allrecords.expire_flag
                                             FROM subset
                                             JOIN allrecords ON
                                             allrecords.subject_id = subset.subject_id
                                           """)
    notes_records.createTempView("noterecords")


    val training_notes_records = sparkSession.sql(
      """ SELECT noterecords.*
                                                           FROM noterecords
                                                           WHERE noterecords.train = 1""")

    //training_notes_records.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dataset/training_notes_records")

    val testing_notes_records = sparkSession.sql(
      """ SELECT noterecords.*
                                                            FROM noterecords
                                                             WHERE noterecords.train = 0""")
    //testing_notes_records.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dataset/testing_notes_records")


    val regexTokenizer = new RegexTokenizer()
      .setInputCol("notescollection")
      .setOutputCol("words")
      .setPattern("\\W")
    val tokenized_notes = regexTokenizer.transform(notes_records).cache()

    val stopWordsRemover = new StopWordsRemover().setStopWords(all_stop_words)
      .setInputCol("words")
      .setOutputCol("filtered")

    //stopWordsRemover.transform(tokenized_notes).show(10000, false)
    val filtered_text = stopWordsRemover.transform(tokenized_notes).cache()
    //var vectorsize = 10029
    val cvModel = new CountVectorizer().setInputCol("filtered")
      .setOutputCol("counter")
      // .setVocabSize(vectorsize)
      //.setMinDF(2)
      .setMinDF(10)
      .fit(filtered_text)


    val count_vector = cvModel.transform(filtered_text)


    val idf = new IDF().setInputCol("counter").setOutputCol("features")

    val idfModel = idf.fit(count_vector)
    val rescaledData = idfModel.transform(count_vector).cache()
    //rescaledData.select("subject_id", "features").take(3).foreach(println)
    val num_topic = 50
    val alpha = 50 / num_topic


    val vectorsize = rescaledData.select("features")
      .take(1).toList(0).get(0)
      .asInstanceOf[org.apache.spark.ml.linalg.SparseVector]
      .size //.asInstanceOf(org.apache.spark.ml.linalg.SparseVector)//.foreach(println)

    println("vectorsize", vectorsize)
    val beta = 200 / vectorsize
    //println("d", d)
    val lda = new LDA().setK(num_topic)
      .setMaxIter(200)
      .setSeed(6250L)
      .setFeaturesCol("features")
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setTopicDistributionCol("topics")


    val pipeline = new Pipeline().setStages(Array(regexTokenizer, stopWordsRemover, cvModel, idf, lda))
    val pipelineModel = pipeline.fit(training_notes_records)
    //pipelineModel.write.overwrite().save("LDAFit/pipe6")
    //val pipelineModel1 = PipelineModel.load("LDAFit/pipe6")
    val training_lda_transformed = pipelineModel.transform(training_notes_records)
    val lda_transformed = pipelineModel.transform(notes_records)
    lda_transformed.createTempView("lda_transformed")

    val ldaModel = pipelineModel.stages(4).asInstanceOf[LocalLDAModel]
    val vectorizerModel = pipelineModel.stages(2).asInstanceOf[CountVectorizerModel]
    val vocabList = vectorizerModel.vocabulary
    val termsIdx2Str = udf { (termIndices: Seq[Int]) => termIndices.map(idx => vocabList(idx)) }
    //sparkSession.udf.register("termsIdx2Str", (termIndices: Seq[Int]) => termIndices.map(idx => vocabList(idx)))

    val topics_show = ldaModel.describeTopics(maxTermsPerTopic = 20)
                              .withColumn("terms", termsIdx2Str(col("termIndices")))
    topics_show.select("topic", "terms", "termWeights").show(1000, false)



    val dataset_structured = sparkSession.sql(
      """
             SELECT icustaydetails.subject_id, icustaydetails.hadm_id,
                    icustaydetails.admission_age as age
                    ,icustaydetails.first_hosp_stay as first_hosp_stay
                    ,icustaydetails.first_icu_stay as first_icu_stay
                   ,allrecords.expire_flag as label, icustaydetails.icustay_id,
             CASE WHEN icustaydetails.gender = 'F' THEN 1 ELSE 0 END as sex,
             CASE WHEN lower(icustaydetails.admission_type )= 'urgent' THEN 0
                  WHEN  lower(icustaydetails.admission_type) = 'emergency' THEN 0
                  WHEN lower(icustaydetails.admission_type) = 'newborn' THEN 1
                  ELSE 2
             END as admission_type,
             COALESCE(sapsii.SAPSII, 0) as sapsii_score,
             COALESCE(sofa.SOFA, 0) as sofa_score,
             COALESCE(oasis.OASIS, 0) as oasis_score
             ,COALESCE(apsiii.APSIII, 0) as apsiii_score
            FROM icustaydetails
            LEFT JOIN sapsii ON icustaydetails.icustay_id = sapsii.icustay_id
                                  AND icustaydetails.hadm_id = sapsii.hadm_id
                                  AND icustaydetails.subject_id = sapsii.subject_id
            LEFT JOIN sofa ON icustaydetails.icustay_id = sofa.icustay_id
                        AND icustaydetails.hadm_id = sofa.hadm_id
                        AND icustaydetails.subject_id = sofa.subject_id

           LEFT JOIN oasis ON icustaydetails.icustay_id = oasis.icustay_id
                         AND icustaydetails.hadm_id = oasis.hadm_id
                        AND icustaydetails.subject_id = oasis.subject_id

           LEFT JOIN apsiii ON icustaydetails.icustay_id = apsiii.icustay_id
                     AND icustaydetails.hadm_id = apsiii.hadm_id
                     AND icustaydetails.subject_id = apsiii.subject_id
           JOIN allrecords ON icustaydetails.subject_id = allrecords.subject_id
           --  WHERE icustaydetails.icustay_seq=1
              """).createTempView("structureddata")


    val training_combined_data = sparkSession.sql(
      """ SELECT lda_transformed.topics, lda_transformed.train , structureddata.*
               FROM  lda_transformed
               JOIN  structureddata
               ON lda_transformed.subject_id = structureddata.subject_id AND
                    lda_transformed.hadm_id = structureddata.hadm_id AND
                    lda_transformed.icustay_id = structureddata.icustay_id
              WHERE lda_transformed.train = 1
           """).cache()
    training_combined_data.createTempView("trainingcombineddata")


    val testing_combined_data = sparkSession.sql(
      """
          SELECT lda_transformed.topics, lda_transformed.train , structureddata.*
               FROM  lda_transformed
               JOIN  structureddata
               ON lda_transformed.subject_id = structureddata.subject_id AND
                    lda_transformed.hadm_id = structureddata.hadm_id
                WHERE lda_transformed.train = 0
           """).cache()
    testing_combined_data.createTempView("testingcombineddata")

    /* =============================  Training and testng With LR  ========================*/

    def TrainAndTest(ClassifierType:String) : Unit = {
      val num_fold = 5
      /*------ Setting the classifier*/
      if(ClassifierType =="LR") {
        println("===========================LR Starts==============================")
        val lr = new LogisticRegression()
          .setMaxIter(100)
          .setFeaturesCol("allfeatures")
          .setLabelCol("label")
        println("Fit Intercept ", lr.getFitIntercept)
        val hashingTF = new HashingTF()

        val paramGrid = new ParamGridBuilder()
          .addGrid(hashingTF.numFeatures, Array(100))
          .build()
        val cv = new CrossValidator()
          .setEstimator(lr)
          .setSeed(6250L)
          .setEvaluator(new BinaryClassificationEvaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(num_fold)

        //================================ only topics  ============================
        /* ================= prepare data for only latent topic features ======================*/
        val topics_assembler = new VectorAssembler().setInputCols(Array("topics")).setOutputCol("allfeatures")
        val topics_train_labeled = topics_assembler.transform(training_combined_data).cache()
        //.withColumnRenamed("labels","label")
        /* ================= train ======================*/
        val cv_topics_lrModel = cv.fit(topics_train_labeled)
        /*========= result of tarining *================*/
        val topics_lrModel = cv_topics_lrModel.bestModel
        val topics_roc_training = topics_lrModel.asInstanceOf[LogisticRegressionModel].summary
          .asInstanceOf[BinaryLogisticRegressionSummary]
        println(s"AUC for Training with LR using Latent Topic Features , areaUnderROC: ${topics_roc_training.areaUnderROC}")
        val topics_train_predictions = cv_topics_lrModel.transform(topics_train_labeled).cache()
        TrainMeasures.measuer(topics_train_predictions.select("probability", "prediction", "label").rdd, " LR using Latent Topic Features")
        /* =========== obtain the result of test data =========================*/
        val topics_test_labeled = topics_assembler.transform(testing_combined_data).cache()
        val topics_predictions = cv_topics_lrModel.transform(topics_test_labeled).cache()
        val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
                                                            .setRawPredictionCol("rawPrediction")
                                                             .setMetricName("areaUnderROC")
        val topic_auc = evaluator.evaluate(topics_predictions)
        println("AUC for Testing with LR using Latent Topic Features" , topic_auc)
        TestMeasures.measuer(topics_predictions.select("probability", "prediction", "label").rdd, " LR using Latent Topic Features")


        //================================ structured  ============================
        /* ================= prepare data for only structured features ======================*/
        val base_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score"
          ,"apsiii_score" , "admission_type", "first_hosp_stay", "first_icu_stay"
        )).setOutputCol("allfeatures")
        val base_train_labeled = base_assembler.transform(training_combined_data).cache()
        val cv_base_lrModel = cv.fit(base_train_labeled)
        val base_lrModel = cv_base_lrModel.bestModel
        val base_roc_training = base_lrModel.asInstanceOf[LogisticRegressionModel].summary
          .asInstanceOf[BinaryLogisticRegressionSummary]
        println(s"AUC for Training with LR Using Structured Features, areaUnderROC: ${base_roc_training.areaUnderROC}")

        val base_train_predictions = cv_base_lrModel.transform(base_train_labeled).cache()
        TrainMeasures.measuer(base_train_predictions.select("probability", "prediction", "label").rdd, " LR using Structured Features")



        /* =========== obtain the result of test data =========================*/
        val base_test_labeled = base_assembler.transform(testing_combined_data).cache()
        val base_predictions = cv_base_lrModel.transform(base_test_labeled).cache()
        val h = base_predictions.getClass
        //println("classss", h)
        val base_auc = evaluator.evaluate(base_predictions)
        println("AUC for Testing with LR using Structured Features", base_auc)
        TestMeasures.measuer(base_predictions.select("probability", "prediction", "label").rdd, " LR using Structured Features")

        //================================ structured + sapssii +  note ============================

        /* ================= prepare data for structured + topic features ======================*/

        val all_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay", "topics"))
          .setOutputCol("allfeatures")
        val all_train_labeled = all_assembler.transform(training_combined_data)


        val cv_all_lrModel = cv.fit(all_train_labeled)
        val all_lrModel = cv_all_lrModel.bestModel

        val training_summary = all_lrModel.asInstanceOf[LogisticRegressionModel].summary
          .asInstanceOf[BinaryLogisticRegressionSummary]
        println(s"AUC for Training with LR Using Combined Features areaUnderROC: ${training_summary.areaUnderROC}")
        val all_train_predictions = cv_all_lrModel.transform(all_train_labeled).cache()
        TrainMeasures.measuer(all_train_predictions.select("probability", "prediction", "label").rdd, " LR using Combined Features")


        val test_labeled = all_assembler.transform(testing_combined_data)
        val all_predictions = cv_all_lrModel.transform(test_labeled).cache()
        val all_auc = evaluator.evaluate(all_predictions)
        println("AUC for Testing with LR Using Combined Features ", all_auc)
        TestMeasures.measuer(all_predictions.select("probability", "prediction", "label").rdd, " LR using Combined Features")
        println("===========================LR Ends==============================")
      }


      /* ===================================== Random Forest ===============================*/

      if(ClassifierType =="RF")
      {
        println("===========================RF Starts==============================")


        val rf = new RandomForestClassifier().setFeaturesCol("allfeatures")
                                              .setLabelCol("label")
                                               .setFeatureSubsetStrategy("auto")
                                                .setSeed(6250L)
         println("MinInfoGain", rf.getMinInfoGain)
        //println("Threshold", rf.getThresholds)
        val hashingTF = new HashingTF()
        val paramGrid = new ParamGridBuilder()
           .addGrid(rf.maxDepth, Array(5))
          .addGrid(rf.numTrees, Array(400))
           .addGrid(rf.impurity, Array("gini"))
          .build()


        val cv = new CrossValidator()
          .setEstimator(rf)
          .setSeed(6250L)
          .setEvaluator(new BinaryClassificationEvaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(num_fold)

        //================================ only topics  ============================
        /* ================= prepare data for only latent topic features ======================*/
        val topics_assembler = new VectorAssembler().setInputCols(Array("topics")).setOutputCol("allfeatures")
        val topics_train_labeled = topics_assembler.transform(training_combined_data).cache()
        //.withColumnRenamed("labels","label")
        /* ================= train ======================*/
        val cv_topics_rfModel = cv.fit(topics_train_labeled)
        /*========= result of tarining *================*/
        val topics_rfModel = cv_topics_rfModel.bestModel

        val topics_train_result = cv_topics_rfModel.transform(topics_train_labeled).cache()
        val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
                                                           .setRawPredictionCol("rawPrediction")
                                                           .setMetricName("areaUnderROC")
        val topic_train_auc = evaluator.evaluate(topics_train_result)
        println("AUC for Training with RF using Latent Topic Features" , topic_train_auc)
        TrainMeasures.measuer(topics_train_result.select("probability", "prediction", "label").rdd, " RF using Latent Topic Features")

        /* =========== obtain the result of test data =========================*/
        val topics_test_labeled = topics_assembler.transform(testing_combined_data).cache()
        val topics_predictions = cv_topics_rfModel.transform(topics_test_labeled).cache()

        val topic_test_auc = evaluator.evaluate(topics_predictions)
        println("AUC for Testing with RF using Latent Topic Features" , topic_test_auc)
        TestMeasures.measuer(topics_predictions.select("probability", "prediction", "label").rdd, " RF using Latent Topic Features")
       // println("test here")
        //topics_predictions.select("probability", "prediction", "label").rdd.foreach(println)



        //================================ structured  ============================
        /* ================= prepare data for only structured features ======================*/
        val base_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay"
        )).setOutputCol("allfeatures")
        val base_train_labeled = base_assembler.transform(training_combined_data).cache()
        val cv_base_rfModel = cv.fit(base_train_labeled)
        val base_rfModel = cv_base_rfModel.bestModel.params
        println("RF Class", base_rfModel.getClass())
        val base_train_result = cv_base_rfModel.transform(base_train_labeled).cache()
        val base_train_auc = evaluator.evaluate(base_train_result)
        println("AUC for Training with RF using Structured Features" , base_train_auc)
        TrainMeasures.measuer(base_train_result.select("probability", "prediction", "label").rdd, " RF using Structured Features")



        /* =========== obtain the result of test data =========================*/
        val base_test_labeled = base_assembler.transform(testing_combined_data).cache()
        val base_predictions = cv_base_rfModel.transform(base_test_labeled).cache()
        val h = base_predictions.getClass
        //println("classss", h)
        val base_auc = evaluator.evaluate(base_predictions)
        println("AUC for Testing with RF using Structured Features", base_auc)
        TestMeasures.measuer(base_predictions.select("probability", "prediction", "label").rdd, " RF using Structured Features")

        //================================ structured + sapssii +  note ============================

        /* ================= prepare data for structured + topic features ======================*/

        val all_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay", "topics"))
          .setOutputCol("allfeatures")
        val all_train_labeled = all_assembler.transform(training_combined_data)


        val cv_all_rfModel = cv.fit(all_train_labeled)
        val all_rfModel = cv_all_rfModel.bestModel
        val all_train_result = cv_all_rfModel.transform(all_train_labeled).cache()
        val all_train_auc = evaluator.evaluate(all_train_result)
        println("AUC for Training with RF using Combined Features" , all_train_auc)
        TrainMeasures.measuer(all_train_result.select("probability", "prediction", "label").rdd, " RF using Combined Features")
        val all_test_labeled = all_assembler.transform(testing_combined_data)
        val all_predictions = all_rfModel.transform(all_test_labeled).cache()
        val all_test_auc = evaluator.evaluate(all_predictions)
        println("AUC for Testing with RF Using All Features ", all_test_auc)
        TestMeasures.measuer(all_predictions.select("probability", "prediction", "label").rdd, " RF using Combined Features")
        println("===========================RF Ends==============================")
      }
/*  ======================= Gradient Boosting Tree ====================*/

      if(ClassifierType =="GBT")
      {
        println("===========================GBT Starts==============================")


        val gbt =  new GBTClassifier()
          .setMaxIter(50)
          .setFeaturesCol("allfeatures")
          .setLabelCol("label")
          .setSeed(6250L)


        println("Impurity ", gbt.getImpurity )
        println(" get loss type ", gbt.getLossType)
        println(" max depth", gbt.getMaxDepth)
        println("stepSize ", gbt.getStepSize)
        println("minInfoGain ", gbt.minInfoGain)


        val hashingTF = new HashingTF()
        val paramGrid = new ParamGridBuilder()
          .addGrid(gbt.maxDepth, Array(5))
          .addGrid(gbt.impurity, Array("gini"))
          .build()


        val cv = new CrossValidator()
          .setEstimator(gbt)
          .setSeed(6250L)
          .setEvaluator(new BinaryClassificationEvaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(num_fold)

        //================================ only topics  ============================
        /* ================= prepare data for only latent topic features ======================*/
        val topics_assembler = new VectorAssembler().setInputCols(Array("topics")).setOutputCol("allfeatures")
        val topics_train_labeled = topics_assembler.transform(training_combined_data).cache()
        //.withColumnRenamed("labels","label")
        /* ================= train ======================*/
        val cv_topics_gbtModel = cv.fit(topics_train_labeled)
        /*========= result of tarining *================*/
        val topics_gbtModel = cv_topics_gbtModel.bestModel

        val topics_train_result = cv_topics_gbtModel.transform(topics_train_labeled).cache()
        val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
          .setRawPredictionCol("rawPrediction")
          .setMetricName("areaUnderROC")
        val topic_train_auc = evaluator.evaluate(topics_train_result)
        println("AUC for Training with GBT using Latent Topic Features" , topic_train_auc)
        TrainMeasures.measuer(topics_train_result.select("probability", "prediction", "label").rdd, " GBT using Latent Topic Features")

        /* =========== obtain the result of test data =========================*/
        val topics_test_labeled = topics_assembler.transform(testing_combined_data).cache()
        val topics_predictions = cv_topics_gbtModel.transform(topics_test_labeled).cache()

        val topic_test_auc = evaluator.evaluate(topics_predictions)
        println("AUC for Testing with GBT using Latent Topic Features" , topic_test_auc)
        TestMeasures.measuer(topics_predictions.select("probability", "prediction", "label").rdd, " GBT using Latent Topic Features")
       // println("test here")


        //================================ structured  ============================
        /* ================= prepare data for only structured features ======================*/
        val base_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score" , "apsiii_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay"
        )).setOutputCol("allfeatures")
        val base_train_labeled = base_assembler.transform(training_combined_data).cache()
        val cv_base_gbtModel = cv.fit(base_train_labeled)
        val base_gbtModel = cv_base_gbtModel.bestModel.params
        println("GBT Class", base_gbtModel)
        val base_train_result = cv_base_gbtModel.transform(base_train_labeled).cache()
        val base_train_auc = evaluator.evaluate(base_train_result)
        println("AUC for Training with GBT using Structured Features" , base_train_auc)
        TrainMeasures.measuer(base_train_result.select("probability", "prediction", "label").rdd, " GBT using Structured Features")

        /* =========== obtain the result of test data =========================*/
        val base_test_labeled = base_assembler.transform(testing_combined_data).cache()
        val base_predictions = cv_base_gbtModel.transform(base_test_labeled).cache()
        val h = base_predictions.getClass
        //println("classss", h)
        val base_auc = evaluator.evaluate(base_predictions)
        println("AUC for Testing with GBT using Structured Features", base_auc)
        TestMeasures.measuer(base_predictions.select("probability", "prediction", "label").rdd, " GBT using Structured Features")

        //================================ structured + sapssii +  note ============================

        /* ================= prepare data for structured + topic features ======================*/

        val all_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score", "apsiii_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay", "topics"))
          .setOutputCol("allfeatures")
        val all_train_labeled = all_assembler.transform(training_combined_data)

        val cv_all_gbtModel = cv.fit(all_train_labeled)
        val all_gbtModel = cv_all_gbtModel.bestModel
        val all_train_result = cv_all_gbtModel.transform(all_train_labeled).cache()
        val all_train_auc = evaluator.evaluate(all_train_result)
        println("AUC for Training with GBT using Combined Features" , all_train_auc)
        TrainMeasures.measuer(all_train_result.select("probability", "prediction", "label").rdd, " GBT using Combined Features")
        val all_test_labeled = all_assembler.transform(testing_combined_data)
        val all_predictions = all_gbtModel.transform(all_test_labeled).cache()
        val all_test_auc = evaluator.evaluate(all_predictions)
        println("AUC for Testing with GBT Using All Features ", all_test_auc)
        TestMeasures.measuer(all_predictions.select("probability", "prediction", "label").rdd, " GBT using Combined Features")
        println("===========================GBT ENDs==============================")
      }

      if(ClassifierType =="SVM")
      {

        println("===========================SVM starts==============================")

        val svm =  new LinearSVC()
          .setMaxIter(100)
          //.setRegParam(0.3)
          //.setElasticNetParam(0.8)
          .setFeaturesCol("allfeatures")
          .setLabelCol("label")
          //.setSeed(6250L)
        println("svm.getFitIntercept",svm.getFitIntercept)
        println("svm.getTol",svm.getTol)
        val hashingTF = new HashingTF()
        val paramGrid = new ParamGridBuilder()
           .addGrid(hashingTF.numFeatures, Array(100))
          .build()


        val cv = new CrossValidator()
          .setEstimator(svm)
          .setSeed(6250L)
          .setEvaluator(new BinaryClassificationEvaluator)
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(num_fold)

        //================================ only topics  ============================
        /* ================= prepare data for only latent topic features ======================*/
        val topics_assembler = new VectorAssembler().setInputCols(Array("topics")).setOutputCol("allfeatures")
        val topics_train_labeled = topics_assembler.transform(training_combined_data).cache()
        /* ================= train ======================*/
        val cv_topics_svmModel = cv.fit(topics_train_labeled)
        /*========= result of tarining *================*/
        val topics_svmModel = cv_topics_svmModel.bestModel
        val topics_train_result = cv_topics_svmModel.transform(topics_train_labeled).cache()
        val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
                       .setRawPredictionCol("rawPrediction")
                       .setMetricName("areaUnderROC")
        val topic_train_auc = evaluator.evaluate(topics_train_result)
        println("AUC for Training with SVM using Latent Topic Features" , topic_train_auc)
        TrainMeasuresSVM.measuer(topics_train_result.select("rawPrediction", "prediction", "label").rdd, " SVM using Latent Topic Features")

        /* =========== obtain the result of test data =========================*/
        val topics_test_labeled = topics_assembler.transform(testing_combined_data).cache()
        val topics_predictions = cv_topics_svmModel.transform(topics_test_labeled).cache()

        val topic_test_auc = evaluator.evaluate(topics_predictions)
        println("AUC for Testing with SVM using Latent Topic Features" , topic_test_auc)
        TestMeasuresSVM.measuer(topics_predictions.select("rawPrediction", "prediction", "label").rdd, " SVM using Latent Topic Features")


        //================================ structured  ============================
        /* ================= prepare data for only structured features ======================*/
        val base_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay"
        )).setOutputCol("allfeatures")
        val base_train_labeled = base_assembler.transform(training_combined_data).cache()
        val cv_base_svmModel = cv.fit(base_train_labeled)
        val base_svmModel = cv_base_svmModel.bestModel.params
        println("svm Class", base_svmModel)
        val base_train_result = cv_base_svmModel.transform(base_train_labeled).cache()
        val base_train_auc = evaluator.evaluate(base_train_result)
        println("AUC for Training with SVM using Structured Features" , base_train_auc)
        TrainMeasuresSVM.measuer(base_train_result.select("rawPrediction", "prediction", "label").rdd, " SVM using Structured Features")

        /* =========== obtain the result of test data =========================*/
        val base_test_labeled = base_assembler.transform(testing_combined_data).cache()
        val base_predictions = cv_base_svmModel.transform(base_test_labeled).cache()
        val h = base_predictions.getClass
        val base_auc = evaluator.evaluate(base_predictions)
        println("AUC for Testing with SVM using Structured Features", base_auc)
        TestMeasuresSVM.measuer(base_predictions.select("rawPrediction", "prediction", "label").rdd, " SVM using Structured Features")

        //================================ structured + sapssii +  note ============================

        /* ================= prepare data for structured + topic features ======================*/

        val all_assembler = new VectorAssembler().setInputCols(Array("age", "sex", "sapsii_score", "sofa_score", "oasis_score"
          , "admission_type", "first_hosp_stay", "first_icu_stay", "topics"))
          .setOutputCol("allfeatures")
        val all_train_labeled = all_assembler.transform(training_combined_data)

        val cv_all_svmModel = cv.fit(all_train_labeled)
        val all_svmModel = cv_all_svmModel.bestModel
        val all_train_result = cv_all_svmModel.transform(all_train_labeled).cache()
        val all_train_auc = evaluator.evaluate(all_train_result)
        println("AUC for Training with SVM using Combined Features" , all_train_auc)
        TrainMeasuresSVM.measuer(all_train_result.select("rawPrediction", "prediction", "label").rdd, " SVM using Combined Features")
        val all_test_labeled = all_assembler.transform(testing_combined_data)
        val all_predictions = all_svmModel.transform(all_test_labeled).cache()
        val all_test_auc = evaluator.evaluate(all_predictions)
        println("AUC for Testing with SVM Using All Features ", all_test_auc)
        TestMeasuresSVM.measuer(all_predictions.select("rawPrediction", "prediction", "label").rdd, " SVM using Combined Features")
        println("===========================SVM ENDs==============================")
      }

  }
    TrainAndTest("LR")
    TrainAndTest("RF")
    TrainAndTest("SVM")
    TrainAndTest("GBT")
    sparkSession.stop()
    

}



}
