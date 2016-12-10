/**
  * Created by Shuxin Chen on 2016/12/10.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext

class MySpec extends UnitSpec{

  val path = "rawData.csv"

  val rawData = Pro.loadData(path)

  "Raw data file" should "exist and consist of data" in {
    val count = rawData.count()
    count should be > 0L
  }


  "Linear regression pipeline" should "have transValidationSplit" in {
    val trainValidationSplit = Predict.prepareTrainValidation()
    trainValidationSplit.getTrainRatio shouldBe 0.75
  }


  "All string features" must "be converted to numeric" in {

    val parsedData = Pro.parseData(rawData)

    parsedData.select("Team_index", "Opp_index", "Pos_index", "Name_index").rdd.take(100).foreach(row => {
      row.getDouble(0).isInstanceOf[Double] shouldBe true
      row.getDouble(1).isInstanceOf[Double] shouldBe true
      row.getDouble(2).isInstanceOf[Double] shouldBe true
      row.getDouble(3).isInstanceOf[Double] shouldBe true
    })

  }


}
