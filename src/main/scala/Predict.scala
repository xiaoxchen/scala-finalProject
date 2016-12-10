


import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics


object Predict {
  def prepareTrainValidation(): TrainValidationSplit = {

    val lr = new LinearRegression().setMaxIter(10)

    val assembler = new VectorAssembler()
      //.setInputCols(Array("Year", "Month", "Day", "Team_index", "Opp_index", "Pos_index", "Name_index", "H", "R", "RBI", "SB", "CS", "BB", "K", "ISO", "BABIP", "AVG", "OBP", "SLG"))
      .setInputCols(Array("Year", "Month", "Day",
      "Team_index", "Opp_index", "Pos_index",
      "Name_index","K", "AVG", "SLG"))
      .setOutputCol("features")


    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val pipeline = new Pipeline()
      .setStages(Array(assembler, lr))


    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 75% of the data will be used for training and the remaining 25% for validation.
      .setTrainRatio(0.75)
    trainValidationSplit
  }


  def predict(pd: DataFrame, trainValidationSplit: TrainValidationSplit)={

    val Array(training, test) = pd.randomSplit(Array(0.9, 0.1), seed = 777777)
    val model = trainValidationSplit.fit(training)

    val holdout =  model.transform(test).select("prediction", "label")

    val regressionMatrix = new RegressionMetrics(holdout.rdd.map(x =>
      (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

    println("Result Metrics")
    println("Result Explained Variance: " + regressionMatrix.explainedVariance)
    println("Result R^2 Coeffieciant: " +  regressionMatrix.r2)
    println("Result Mean Square Error : " + regressionMatrix.meanSquaredError)
    println("Result Root Mean Squared Error : " + regressionMatrix.rootMeanSquaredError)

    model

  }
  def savePredictions(holdout: DataFrame, test: DataFrame, filePath: String) = {

    val testdataOutput = test
      .select("Name_index")
      .distinct()
      .join(test,
        holdout("id") <=> test("Name_index")
          && holdout("pYear")<=> test("Year")
          && holdout("pMonth")<=> test("Year")
          && holdout("pDay") <=> test("Day")
          && holdout("pK") <=> test("K")
          && holdout("pAVG") <=> test("AVG"),
          //&& holdout("Label") <=> test("Label"),
        "outer")
      .select("Name_index", "Label")
      .na.fill(0: Double)

    // fill these with something
    testdataOutput
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(filePath)
  }

}
