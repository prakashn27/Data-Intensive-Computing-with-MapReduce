# Volatility-Analysis
In this assignment, you will use Mapreduce on a Hadoop environment at CCR to compute the monthly volatility of stocks. You are given daily data of 2970 stocks on NASDAQ market for 3 years from 01/01/2012 to 12/31/2014 (except holidays, otherwise called trading days). Imagine that you are a data analyst working for an investment company, your daily job is to analyse stock price data, and find out which stocks in a certain period have higher earnings potential, etc. One characteristic that is widely used by traders is the volatility index. You can get more details at http://stockcharts.com/school/doku.php? id=chart_school:technical_indicators:standard_deviation _volatility
Your data is 2970 CSV format files (Comma Separated). Each file contains the data for one stock using its symbol as the file name. A stock list file is also provided.


###Objective:   
*  Find the top 10 stocks with Lowest (min) volatility.
*  Find the top 10 stocks with the Highest (max) volatility.

###Algorithm      
I used mapreduce paradigm of Hadoop to serialise the calculation of volatility for each month and computed the top 10 and bottom values.        
Number of Mapper Implementation : 3 Number of Reducer Implementation : 3        
####Roles of each Mapper and Reducer:####         
**Mapper1**         
• splits the input data and options the date and close adjacent value.
• key - stock_name + month + year
• value - date + adjacent close value       
**Reducer1**        
• Since after the map step the values which have same key are grouped
together and passed to the reducer as iterable, values that correspond to specific month and year of the particular stock are grouped together.
• Beginning adjacent close value and end adjacent close value are obtained by integrating through the iterable and the value of xi for the corresponding month is computed.
• key - Company Name
• Value - Computed Xi.          
**Mapper2**     
• Now we have to consolidate all the values obtained from the reducer with respect to company name.
• Key - Company Name
• Value - Xi    
**Reducer2**     
• All the xi corresponding to the the respective companies are grouped
together.
• Volatility for the particular company is obtained from these values.
• Key - Company Name
• Value - Volatility     
**Mapper3:**    
• All the companies are grouped together with a common key.
• Key - Common
• Value = Company Name + Volatility
**Reducer3:**    
• Obtained iterable contains all the company name with values and they are
sorted by a custom comparator.
• top 10 and bottom 10 values are obtained from the List

