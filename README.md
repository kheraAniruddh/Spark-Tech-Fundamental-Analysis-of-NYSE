# Spark-Tech-Fundamental-Analysis-of-NYSE
Technical & Fundamental analysis of S&P 500 companies on NYSE

	The input files can be downloaded from: https://www.kaggle.com/dgawlik/nyse/data
1) Used maven to create the jar file


 


Steps to execute project:

2)	Once the jar file is ready:
File 1: Main Scala 
•	Execute Main.scala first
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 --class scala.nyu.edu.Main --master yarn edu-0.0.1-SNAPSHOT.jar
•	This file reads all the 3 csv files: Fundamentals, Price and Securities and creates 3 individual data frames. 
•	Price: This table gives us the closing price for all the symbols. The return on investment is calculated with help of closing price. 
This data is stored in ‘PriceData’ on HDFS.
•	Fundamentals: This file gives us the parameters for choosing various labels and see who have maximum impact on ROI. For the purpose of this project we focused on Cash Ratio and Return on Equity.
The calculations for both are stored as ‘FundamentalsROE’ and ‘FundamentalsCR’ respectively on HDFS.
•	Securities: This data gives the sector of a company. This will be used to check if companies in same sector show any similar pattern in growth.After reading the file it is joined with the Price Data to find the ROI for each company.This file is placed as ‘securitiesData’ on HDFS.

These 3 files stored will be used as Input to the below files. All files are used to compute the row_number of the rows by different parameters as mentioned below.


3)	Execute Table Scripts:

•	sec.scala : This script is executed on the ‘securitiesData’ file created above. It associates row_number to each row by ordering the ROI value (It is calculated as part of ‘PriceData’ file- Step1).Top X companies( X chosen as 20 for project purpose) are chosen to see their growth for years 2010-2016. Data is saved on hdfs as ‘rankSecData’.

•	fundamental.scala: This script is executed on the ‘FundamentalsCR’ file created above. It associates row_number to each row by ordering the Cash Ratio values from the file. Companies with Top 5-10 cash ratio and companies with bottom 5-10 cash ratio are filtered.Data is saved on hdfs as ‘rankCRData’

•	fundamentalROE.scala: This script is executed on the ‘FundamentalsROE’ file created above. It associates row_number to each row by ordering the ‘Return on Equity’ values from the file. Companies with Top 5-10 ROE and companies with bottom 5-10 ROE are filtered.Data is saved on hdfs as ‘rankROEData’

Commands:

spark-shell --packages com.databricks:spark-csv_2.10:1.1.0 -i sec.scala
spark-shell --packages com.databricks:spark-csv_2.10:1.1.0 -i fundamental.scala
spark-shell --packages com.databricks:spark-csv_2.10:1.1.0 -i fundamentalROE.scala


Post the above 2 steps we have tables created which would be used for drawing insights

4)	Run Insights
•	Execute Insight.scala
•	spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 --class scala.nyu.edu.Insight --master yarn edu-0.0.1-SNAPSHOT.jar
•	This file will read ‘PriceData’, ‘rankSecData’, ‘rankCRData’,‘rankROEData’ and create new dataframes.
•	Final Output is provided in ‘Top20’,’CR’,’ROE’


Output Top 20:

	
 


Output CR:

 

Output ROE:
 


Insight 1:This program will be used to check if various companies belonging to same sector show any particular trend in their ROI as years progress. Top companies with rank <=20 are chosen. The graph below (Fig.1) showsthe number of Technological companies in the top 20 performers on S&P500 index (based on calculated value Return of Investment) is increasing almost monotonically  across years.
Code: The class Insight1_Top20PerfCompanies() contains the script.
Tables Used: Securities Table & Price Table

![Alt text](/snaps/inisght1.png)
 
					Fig. 1 (Insight 1)

Insight 2:
We use Cash Ratio (C.R.) and ROI as a predictor of the stock performance. We choose the top N and bottom N companies from the S&P 500 based on C.R. to take long and short positions in and rebalanced at the end of every year. 
Code: The class Insight2_PortfolioReturns_CRcontains the script.
Tables Used: Fundamentals Table & Price Table
This gave us 2 insights: 
1.   Cash Ratio is an inverse predictor of next year’s returns, which follows the Mean Reversion financial theory.
2.   The effects of diversification tend to fade after top 5 companies and returns in fact diminishes if we increase the top N   companies.





 
Fig: 2 (Insight 2)


Insight 3:
We use Return on Equity and ROI as a predictor of the stock performance. We choose the top N and bottom N companies using ROE as a parameter from the S&P 500 based to take long and short positions in and rebalanced at the end of every year. This shows that if we use ROE as a parameter it gives better portfolio returns as we increase count of companies.
Code: The class Insight3_PortfolioReturns_ROE contains the script.
Tables Used: Fundamentals Table & Price Table

 
					Fig:3 (Insight 3)

Insight 4:
Forecasting using ARIMA(Machine Learning)
We were able to forecast next/last15 days stock closing value of S&P companies, with a decent yet could be improved accuracy.
Table Used: Price Data
Code: MyArimaModel  This project has been built separately.
 

To run the code, install sbt on local/cloudera with scala 2.11.8. Run Main.scala under BDAD_Forecasting. Or alternatively run  MyArima.scala file on REPL dumbo with all the dependencies under folder dependencies
Output: 
 
 


 
