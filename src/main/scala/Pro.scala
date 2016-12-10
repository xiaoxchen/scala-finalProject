import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics




object Pro {


  def main(args: Array[String]) = {


    val path = "rawData.csv"

    val rawData = loadData(path)

    val parsedData = parseData(rawData)

    //val pd = parsedData.select("label", "Year", "Month", "Day",
    // "Team_index", "Opp_index", "Pos_index", "Name_index", "H",
    // "R", "RBI", "SB", "CS", "BB", "K", "ISO", "BABIP", "AVG", "OBP", "SLG")
    val pd = parsedData.select("label", "Year", "Month", "Day",
      "Team_index", "Opp_index", "Pos_index", "Name_index","K", "AVG", "SLG")


    val trainValidationSplit = Predict.prepareTrainValidation()


    val model = Predict.predict(pd, trainValidationSplit)

    val holdout = model.transform(pd).withColumnRenamed("label", "actual")
      .withColumnRenamed("Day", "pDay")
      .withColumnRenamed("Year", "pYear")
      .withColumnRenamed("Month", "pMonth")
      .withColumnRenamed("AVG", "pAVG")
      .withColumnRenamed("K", "pK")
        .withColumnRenamed("Name_index", "id")
      .select("id","pYear", "pMonth", "pDay","pK","pAVG","actual","prediction")

    holdout.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("wPredict.csv")
    val holdoutFiltered = holdout.filter("Year = 2000")
    holdoutFiltered.show(200)

    //Predict.savePredictions(holdout, pd, "wPredict.csv")

  }


  def loadData(path: String): DataFrame = {

    val conf = new SparkConf().setAppName("RegressionPrediction").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile(path)
    val header = file.first()
    val filtered = file.filter(_ != header)



    val inputRDD = filtered.map { line =>
      val d = line.split(",")
      //((d(0).substring(26, 30)).toDouble, (d(0).substring(31, 33)).toDouble,
      // (d(0).substring(34, 36)).toDouble, d(1), d(2), d(4), d(5), d(6).toDouble,
      // d(10).toDouble, d(11).toDouble, d(12).toDouble, d(13).toDouble, d(14).toDouble,
      // d(15).toDouble, d(16).toDouble, d(17).toDouble, d(18).toDouble, d(19).toDouble,
      // d(20).toDouble, d(22).toDouble, d(23).substring(0, d(23).length() - 5))
      ((d(0).substring(26, 30)).toDouble, (d(0).substring(31, 33)).toDouble,
        (d(0).substring(34, 36)).toDouble, d(1), d(2), d(4), d(5),
         d(15).toDouble, d(18).toDouble, d(20).toDouble,d(22).toDouble,
        d(23).substring(0, d(23).length() - 5))
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //val rawdf = inputRDD.toDF("Year", "Month", "Day", "Team", "Opp", "Pos",
    // "PA", "H", "R", "RBI", "SB", "CS", "BB", "K", "ISO", "BABIP", "AVG", "OBP", "SLG", "label", "Name")
    val rawdf = inputRDD.toDF("Year", "Month", "Day", "Team", "Opp", "Pos", "PA", "K", "AVG", "SLG","label", "Name")
    val df = rawdf.filter("PA > 0")
    df.show(20)
    df
  }

   def parseData(data: DataFrame): DataFrame = {
      val spark = SparkSession.builder
        .appName("RegressionPrediction")
        .getOrCreate()


      val stringColumns = Array("Team", "Opp", "Pos", "Name")
      val index_transformers: Array[org.apache.spark.ml.PipelineStage] = stringColumns.map(
        cname => new StringIndexer()
          .setInputCol(cname)
          .setOutputCol(s"${cname}_index")
      )

      val index_pipeline = new Pipeline().setStages(index_transformers)
      val index_model = index_pipeline.fit(data)
      val parsedData = index_model.transform(data)

      parsedData
   }




}