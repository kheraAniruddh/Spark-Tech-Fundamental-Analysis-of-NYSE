package scala.nyu.edu

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, rank}
import org.apache.spark._
import java.util.Date
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{LongType, TimestampType}
import com.databricks.spark._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._


object Main {
        def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("BDAD_Proj").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val etl1= new ETL_price_split_YAHOO_Data(sc)
        val df1 = etl1.priceDataCleaning
        val roi = etl1.roiCalc(df1)

        val etl2 = new ETL_securities_NASDAQ_Data(sc)
        val df2 = etl2.securitiesCleaning
        val insight1 = new Insight1_Top20PerfCompanies(sc)
        insight1.top20(df2,roi)

        val etl3 = new ETL_fundamentals_NASDAQ_Data(sc)
        val df3 = etl3.fundamentalsDataForCR
        //val insight2 = new Insight2_PortfolioReturns_CR(sc)
        //insight2.portfolioCR(df3, roi)

        val df4 = etl3.fundamentalsDataCleaningROE
        //val insight3 = new Insight3_PortfolioReturns_ROE(sc)
        //insight3.portfolioROE(df4, roi)
        //println("Completed!");
        }

// ============================================ETL_fundamentals_NASDAQ_Data Starts ============================

        class ETL_fundamentals_NASDAQ_Data(sc: SparkContext) {
          //val sc = SparkContext(appName="BDAD")
          val sqlCtx = new SQLContext(this.sc)
                import sqlCtx.implicits._
          def fundamentalsDataForCR: DataFrame = {
            val fundamentalsDF = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/bdadproject/fundamentals.csv")
            val fund = fundamentalsDF.select(year($"Period Ending").as("year"),$"Ticker Symbol" , $"Cash Ratio")
            val finCR = fund.filter($"year">2010 && $"Cash Ratio".isNotNull && $"Cash Ratio" >0.0)

            finCR.registerTempTable("Fundamentals_CR")
            finCR.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/FundamentalsCR");
            return  finCR
          }

          def fundamentalsDataCleaningROE: DataFrame = {
            val fundamentalsDF = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/bdadproject/fundamentals.csv")
            val fund = fundamentalsDF.select(year($"Period Ending").as("year"),$"Ticker Symbol" , $"After Tax ROE")
            val finROE = fund.filter($"year">2010 && $"After Tax ROE".isNotNull && $"After Tax ROE" >0.0)
            finROE.registerTempTable("Fundamentals_ROE")
            finROE.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/FundamentalsROE");
            return  finROE
          }
        }

// ============================================ETL_fundamentals_NASDAQ_Data Ends ============================


//=============================================ETL_price_split_YAHOO_Data Starts =============================


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

  val sqlCtx = new SQLContext(this.sc)
  import sqlCtx.implicits._

  // Dataset1: prices-split-adjusted

  def priceDataCleaning: DataFrame = {
    val sqlCtx = new SQLContext(this.sc)
    //Import file as RDD.
    // Format "date":String => "date":Date
    val priceData = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/bdadproject/prices-split-adjusted.csv")
    return priceData
  }


  def roiCalc(priceData: DataFrame): DataFrame = {
    // Output of this function is saved in a file. UNcomment the line below and comment the rest to avoid expensive joins
    //   val priceData = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("C:\\Users\\Anik\\Desktop\\PriceData\\part-00000")
      val grpdata = priceData.groupBy(year($"date") as "year", $"symbol").agg(max("date") as
        "maxDate")
      val findata = grpdata.as('a).join(priceData.as('b), $"a.symbol" === $"b.symbol" && $"b.date"
        === $"a.maxDate").select($"a.*",$"b.close").orderBy($"symbol",$"year")
      val finnextyr = findata.withColumn("nextyr", $"year" +1)
      val finjoin = finnextyr.as('a).join(finnextyr.as('b), $"a.symbol" === $"b.symbol" && $"a.nextyr" ===
        $"b.year").select($"a.*",$"b.close" as "nextyrval").orderBy($"symbol",$"year")
      val roi = finjoin.withColumn("ROI" , (($"nextyrval" / $"close") - 1))
      roi.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/PriceData");
      roi.registerTempTable("PriceData");
      return roi
  }

}

//=============================================ETL_price_split_YAHOO_Data Ends =============================

//=============================================ETL_securities_NASDAQ_Data Starts =============================

class ETL_securities_NASDAQ_Data(sc: SparkContext) {
 // val this.sc = sc
  val sqlCtx = new SQLContext(this.sc)
  def securitiesCleaning(): DataFrame = {
    val securitiesData = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/bdadproject/securities.csv")
  // securitiesData.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/securitiesData");
	 return securitiesData
  }

}

//=============================================ETL_securities_NASDAQ_Data Ends =============================




class Insight1_Top20PerfCompanies(sc: SparkContext) {
  val this.sc = sc
  val sqlCtx = new SQLContext(this.sc)
  import sqlCtx.implicits._
  // Generates insight1
   def top20(securitiesDF: DataFrame, pricesDF: DataFrame): Unit = {
     val joinedDF = pricesDF.as('a).join(securitiesDF.as('b), $"a.symbol" === $"b.Ticker symbol").select($"a.*",
       $"b.GICS Sector")
	joinedDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/securitiesData");
}
}
}// Object Main

