package machineLearning

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.cloudera.sparkts.models.{ARIMA, ARIMAModel}
import org.apache.spark.sql.types._


// Hardcoded for GOOG data, change symbol passed as param as per the requirement
class MyArimaModel(sc: SparkContext) {
  val this.sc =sc
  val sqlCtx = new SQLContext(this.sc)
  import sqlCtx.implicits._

  def splitData(pricesData: DataFrame): Unit = {
    val trainingData = pricesData.filter(pricesData("date")<"2016-12-15")
    val testData = pricesData.filter(pricesData("date")>="2016-12-15")
    this.hyperParamSelection(trainingData, pricesData)
  }

  // Auto selects the best hyper-params values for ARIMA forecasting
  // Based on AIC values
  def hyperParamSelection(trainingData: DataFrame, priceData: DataFrame): Unit = {
    val close = trainingData.select("close").rdd.map(_.toString().replace("[","").replace("]",""))
    val ts = new DenseVector(close.map(_.toDouble).toArray)
    val clf = ARIMA.autoFit(ts,4,4,4)
    println("coefficients: " + clf.coefficients.mkString(","))
    this.makePredict(clf,ts, priceData)
  }

  // Forecast the test data points
  // Compare with actual values using RMSE accuracy measure in visualization
  def makePredict(clf: ARIMAModel, ts : DenseVector, priceData: DataFrame): Unit = {
    val predVals = clf.forecast(ts, 11).toString.replace("[","").replace("]","").split(",")
    predVals.foreach(println(_))
    val forecastedPts = sc.parallelize(predVals)
    val preds = forecastedPts.toDF("Predicted")
    preds.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save("C:\\Users\\Anik\\Desktop\\asdfr");
    priceData.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("C:\\Users\\Anik\\Desktop\\goog");

  }
}