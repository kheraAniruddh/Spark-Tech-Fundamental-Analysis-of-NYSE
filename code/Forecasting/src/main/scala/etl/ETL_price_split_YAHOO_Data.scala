package etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/*
*******************************ABOUT***********************************************
This class does Extraction, Transformation & loading data as dataframe for analytics use
There are 3 datasets:
 a. prices-split-adjusted
 b. Securities
 c. Fundamentals
Each dataset has it's own function below for the cleaning/profiling/transformation into respective dataframes
*/

class ETL_price_split_YAHOO_Data(sc: SparkContext) {
  val this.sc = sc
  val sqlCtx = new SQLContext(this.sc)
  // Dataset1: prices-split-adjusted
  // Source: Yahoo! Finance
  def priceDataCleaning: DataFrame = {
    val sqlCtx = new SQLContext(this.sc)
    //Import file as RDD.
    // Format "date":String => "date":Date
    val priceData = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("C:\\Users\\Anik\\Desktop\\prices-split-adjusted.csv")
    return priceData
  }

    // Returning GOOGLE prices
    // Change the hardcoded value for other companies in symbol
   def googDF(pricesDF: DataFrame): DataFrame = {
    val goog = pricesDF.filter(pricesDF("symbol")==="GOOG")
    // Sort prices ascending order of dates
    val sorted_goog = goog.sort("date")
    return  sorted_goog
  }

}