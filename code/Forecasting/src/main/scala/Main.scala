import etl.ETL_price_split_YAHOO_Data
import machineLearning.MyArimaModel
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {
def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("BDAD_Proj").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val etl1= new ETL_price_split_YAHOO_Data(sc)
  val df1 = etl1.priceDataCleaning
  val googDF = etl1.googDF(df1)
  val ml =  new MyArimaModel(sc)
//    Passing google data for forecasting next 15 days prices
  ml.splitData(googDF)
  println("Completed!");
}
}