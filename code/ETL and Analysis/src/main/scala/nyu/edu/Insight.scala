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


object Insight {
        def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("BDAD_Insights").setMaster("local[2]")
        val sc = new SparkContext(conf)
	val sqlCtx = new SQLContext(sc)        
	val df1 = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/PriceData/*")	
val df2 = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/rankSecData")
val df3 = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/rankCRData")
val df4 = sqlCtx.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("dateFormat", "MM/dd/yyyy").load("/user/ppv221/rankROEData")

	//val insight1 = new Insight1_Top20PerfCompanies(sc)
        //insight1.top20(df2,df1)

      //  val insight2 = new Insight2_PortfolioReturns_CR(sc)
       // insight2.portfolioCR(df3, df1)

        val insight3 = new Insight3_PortfolioReturns_ROE(sc)
        insight3.portfolioROE(df4, df1)
        //println("Completed!");
        }

class Insight1_Top20PerfCompanies(sc: SparkContext) {
  val this.sc = sc
  val sqlCtx = new SQLContext(this.sc)
  import sqlCtx.implicits._
  // Generates insight1
   def top20(securitiesDF: DataFrame, pricesDF: DataFrame): Unit = {
	val grp = securitiesDF.groupBy($"year", $"GICS Sector").agg(count("GICS Sector") as "max")
     grp.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/top20")
	}
	}

class Insight2_PortfolioReturns_CR(sc: SparkContext) {
  val this.sc = sc
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  // Generates insight2
  def portfolioCR(fundamentalsDF: DataFrame, pricesDF: DataFrame): Unit = {
    val findata = pricesDF.as('a).join(fundamentalsDF.as('b), $"a.symbol" === $"b.Ticker Symbol" && $"a.year" === $"b.year").select($"b.*",$"a.ROI")
    // Code updated beyond this stage to find 5-9 top X companies.

    val dsc5 = findata.where($"rank_desc" <=5).groupBy($"year").agg(sum($"ROI"))
    val asc5 = findata.where($"rank_asc" <=5).groupBy($"year").agg(sum($"ROI"))
    val df5 = asc5.as('a).join(dsc5.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('maxROI),$"b.sum(ROI)".as('minROI))
    val dsc6 = findata.where($"rank_desc" <=6).groupBy($"year").agg(sum($"ROI"))
    val asc6 = findata.where($"rank_asc" <=6).groupBy($"year").agg(sum($"ROI"))
    val df6 = asc6.as('a).join(dsc6.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('maxROI),$"b.sum(ROI)".as('minROI))
    val dsc7 = findata.where($"rank_desc" <=7).groupBy($"year").agg(sum($"ROI"))
    val asc7 = findata.where($"rank_asc" <=7).groupBy($"year").agg(sum($"ROI"))
    val df7 = asc7.as('a).join(dsc7.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('maxROI),$"b.sum(ROI)".as('minROI))
    val dsc8 = findata.where($"rank_desc" <=8).groupBy($"year").agg(sum($"ROI"))
    val asc8 = findata.where($"rank_asc" <=8).groupBy($"year").agg(sum($"ROI"))
    val df8 = asc8.as('a).join(dsc8.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('maxROI),$"b.sum(ROI)".as('minROI))
    val dsc9 = findata.where($"rank_desc" <=9).groupBy($"year").agg(sum($"ROI"))
    val asc9 = findata.where($"rank_asc" <=9).groupBy($"year").agg(sum($"ROI"))
    val df9 = asc9.as('a).join(dsc9.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('maxROI),$"b.sum(ROI)".as('minROI))

    val df5Final = df5.withColumn("Rank",lit(5)).withColumn("Value", ($"minROI" - $"maxROI"))
    val df6Final = df6.withColumn("Rank",lit(6)).withColumn("Value", ($"minROI" - $"maxROI"))
    val df7Final = df7.withColumn("Rank",lit(7)).withColumn("Value", ($"minROI" - $"maxROI"))
    val df8Final = df8.withColumn("Rank",lit(8)).withColumn("Value", ($"minROI" - $"maxROI"))
    val df9Final = df9.withColumn("Rank",lit(9)).withColumn("Value", ($"minROI" - $"maxROI"))

    val dfs = Seq(df5Final,df6Final,df7Final,df8Final,df9Final)
    val finalData = dfs.reduce(_ unionAll  _)
    val final_Data  = finalData.groupBy($"Rank").agg(sum($"Value"))
    final_Data.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/CR")
  }
}


class Insight3_PortfolioReturns_ROE(sc: SparkContext) {
  val sqlCtx = new SQLContext(this.sc)
  import sqlCtx.implicits._
  // Generates insight1
  def portfolioROE(fundamentalsDF: DataFrame, pricesDF: DataFrame): Unit = {
    val findata = pricesDF.as('a).join(fundamentalsDF.as('b),$"a.symbol"===$"b.Ticker Symbol" && $"a.year" === $"b.year").select($"b.*",$"a.ROI")
    // Code updated beyond this stage to find 5-9 top X companies.
    val dsc5 = findata.where($"rank_desc" <=5).groupBy($"year").agg(sum($"ROI"))
    val asc5 = findata.where($"rank_asc" <=5).groupBy($"year").agg(sum($"ROI"))
    val df5 = asc5.as('a).join(dsc5.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('minROI),$"b.sum(ROI)".as('maxROI))
    val dsc6 = findata.where($"rank_desc" <=6).groupBy($"year").agg(sum($"ROI"))
    val asc6 = findata.where($"rank_asc" <=6).groupBy($"year").agg(sum($"ROI"))
    val df6 = asc6.as('a).join(dsc6.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('minROI),$"b.sum(ROI)".as('maxROI))
    val dsc7 = findata.where($"rank_desc" <=7).groupBy($"year").agg(sum($"ROI"))
    val asc7 = findata.where($"rank_asc" <=7).groupBy($"year").agg(sum($"ROI"))
    val df7 = asc7.as('a).join(dsc7.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('minROI),$"b.sum(ROI)".as('maxROI))
    val dsc8 = findata.where($"rank_desc" <=8).groupBy($"year").agg(sum($"ROI"))
    val asc8 = findata.where($"rank_asc" <=8).groupBy($"year").agg(sum($"ROI"))
    val df8 = asc8.as('a).join(dsc8.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('minROI),$"b.sum(ROI)".as('maxROI))
    val dsc9 = findata.where($"rank_desc" <=9).groupBy($"year").agg(sum($"ROI"))
    val asc9 = findata.where($"rank_asc" <=9).groupBy($"year").agg(sum($"ROI"))
    val df9 = asc9.as('a).join(dsc9.as('b), $"a.year" === $"b.year").select($"a.year".as('year),$"a.sum(ROI)".as('minROI),$"b.sum(ROI)".as('maxROI))

    val df5Final = df5.withColumn("Rank",lit(5)).withColumn("Value", ($"maxROI" - $"minROI"))
    val df6Final = df6.withColumn("Rank",lit(6)).withColumn("Value", ($"maxROI" - $"minROI"))
    val df7Final = df7.withColumn("Rank",lit(7)).withColumn("Value", ($"maxROI" - $"minROI"))
    val df8Final = df8.withColumn("Rank",lit(8)).withColumn("Value", ($"maxROI" - $"minROI"))
    val df9Final = df9.withColumn("Rank",lit(9)).withColumn("Value", ($"maxROI" - $"minROI"))
    val dfs = Seq(df5Final,df6Final,df7Final,df8Final,df9Final)
    val finalData = dfs.reduce(_ unionAll _)
    val final_Data  = finalData.groupBy($"Rank").agg(sum($"Value"))
    final_Data.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/ppv221/ROE");
  }

}



	
}// Object Main

