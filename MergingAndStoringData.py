# Conrad Ibanez
# Merging and Storing Data 


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
#from bs4 import BeautifulSoup


def createDatabaseTables(fundamentalsDf,pricesSplitAdjustedDf,securitiesDf,webDataDf,apiDataDf):
    
    '''
    I will merge the following attributes from the three data sources:
    apiDataDf
    Use as basis and take all columns for main database table
    
    securitiesDf
    Ticker Symbol
    Company Name
    Address of Headquarters
    
    webDataDf
    High_52
    Low_52
    
    '''
    securitiesSubsetDf = securitiesDf[['Ticker Symbol','Company Name','Address of Headquarters']]
    webDataSubsetDf = webDataDf[['Ticker Symbol','High_52','Low_52' ]]
    # merge api data with flat file data
    mainCompanyTableDf = pd.merge(apiDataDf, securitiesSubsetDf,on='Ticker Symbol', how='inner')
    # merge main table data with web data
    mainCompanyTableDf = pd.merge(mainCompanyTableDf, webDataSubsetDf ,on='Ticker Symbol', how='inner')
    print(mainCompanyTableDf.head())
    print(mainCompanyTableDf.shape)
    
    # query to create the table
    #query = """CREATE TABLE Main_Company_Table (Ticker_Symbol VARCHAR(10), Price FLOAT(2), Beta Float(2), Volume_Avg INTEGER, Market_Cap BIGINT, Last_Dividend FLOAT(2), Range VARCHAR(25), Changes FLOAT(2), Changes_Percentage VARCHAR(20), Exchange VARCHAR(20), Industry VARCHAR(20), Description VARCHAR(30), CEO VARCHAR(20), Sector VARCHAR(20), Image VARCHAR(40), Company_Name VARCHAR(30), Address VARCHAR(50), High_52 Float(2), Low_52 FLOAT(2));"""
    #dropTableQuery = """DROP table contact_Conrad;"""
    
    conn = sqlite3.connect('DSC540Project_ConradIbanez.sqlite')
    # using dataframe to_sql() function to create table 
    # reference https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    mainCompanyTableDf.to_sql('Main_Company_Table', con = conn, if_exists = 'replace')
    
    # creating tables for the other dataframes
    fundamentalsDf.to_sql('Fundamentals', con = conn, if_exists = 'replace')
    pricesSplitAdjustedDf.to_sql('Prices_Split_Adjusted', con = conn, if_exists = 'replace')
    securitiesDf.to_sql('Securities', con = conn, if_exists = 'replace')
    webDataDf.to_sql('Web_Data', con = conn, if_exists = 'replace')
    

    cursor = conn.cursor()
    # enable foreign key features
    cursor.execute("PRAGMA foreign_keys = 1")
    
    
    '''
    # Sample query
    print("************\n\n");
    rows = cursor.execute("SELECT * FROM Main_Company_Table WHERE `Ticker Symbol` = 'MMM' OR `Ticker Symbol` = 'AAPL'")
    for row in rows:
        print(row)
    '''
     
    
    # to get table information
    cursor.execute("PRAGMA table_info(Prices_Split_Adjusted)")
    print(cursor.fetchall())
    
    # to get table information
    cursor.execute("PRAGMA table_info(Main_Company_Table)")
    print(cursor.fetchall())
    
    
    conn.commit()
    # close the connection
    conn.close()
    # drop the table if it exists so can rerun this over again
    #con.execute(dropTableQuery)
    # execute query to create table
    #con.execute(query)
    #con.commit()
    
def createVisualizations():
    conn = sqlite3.connect('DSC540Project_ConradIbanez.sqlite')
    cursor = conn.cursor()
    # enable foreign key features
    cursor.execute("PRAGMA foreign_keys = 1")
    # using pandas read_sql_query function to read query results directly into dataframe
    mainCompanyDf = pd.read_sql_query("SELECT * FROM Main_Company_Table", conn)
        # create a scatter plot of crime rate versus price
    mainCompanyDf.plot.scatter('price', 'mktCap', s=150, c='blue', edgecolor='k')
    plt.grid(True)
    # set the title and labels
    plt.title("Price vs. Market Cap Scatter Plot", fontsize=18)
    plt.xlabel("Price", fontsize=15)
    plt.ylabel("Market Cap", fontsize=15)
    plt.show()
    
    sectorDf = pd.read_sql_query("SELECT count(*) as companies, sector FROM Main_Company_Table GROUP BY sector ORDER BY companies DESC", conn)
    
    print(sectorDf)
    # create horizontal bar chart https://python-graph-gallery.com/2-horizontal-barplot/
    height = sectorDf.companies
    bars = sectorDf.sector
    y_pos = np.arange(len(bars))
     
    # Create horizontal bars
    plt.barh(y_pos, height)
     
    # Create names on the y-axis
    plt.yticks(y_pos, bars)
     
    plt.title("Number of Companies in Sectors", fontsize=20)
    # Show graphic
    plt.show()

    stockExchangeDf = pd.read_sql_query("SELECT count(*) as companies, exchange FROM Main_Company_Table GROUP BY exchange", conn)
    
    plt.figure(figsize=(8,14))
    plt.pie(stockExchangeDf.companies, labels= stockExchangeDf.exchange, autopct='%1.1f%%', shadow=True, startangle=140)
    plt.title("Stock Exchange Pie Chart", fontsize=20)
    
    pieLabels= stockExchangeDf.exchange
    plt.legend(pieLabels, loc="best")
    #plt.savefig('IndustryPieChart.pdf')
    plt.show()
    # close the connection
    
    
    # Using query to get information from two different sources
    # using pandas read_sql_query function to read query results directly into dataframe
    priceRevenueShareDf = pd.read_sql_query("SELECT Ticker Symbol, Main_Company_Table.price, Web_Data.Revenue_Per_Share FROM Main_Company_Table,Web_Data WHERE Ticker Symbol = `Web_Data.Ticker Symbol`", conn)
    #priceRevenueShareDf = priceRevenueShareDf.dropna()
    # create a scatter plot of crime rate versus price
    priceRevenueShareDf["price"] = pd.to_numeric(priceRevenueShareDf["price"], downcast="float")
    priceRevenueShareDf["Revenue_Per_Share"] = pd.to_numeric(priceRevenueShareDf["Revenue_Per_Share"], downcast="float")
    print(priceRevenueShareDf.head())
    priceRevenueShareDf.plot.scatter('price', 'Revenue_Per_Share', s=150, c='blue', edgecolor='k')
    plt.grid(True)
    # set the title and labels
    plt.title("Price vs. Revenue Per Share Scatter Plot", fontsize=18)
    plt.xlabel("Price", fontsize=15)
    plt.ylabel("Revenue Per Share", fontsize=15)
    plt.show()
    
    conn.close()
    
    
def main():
    
    '''
    Milestone 4 (Weeks 9 & 10)
    Connecting to an API/Pulling in the Data and Cleaning/Formatting
    Perform at least 5 data transformation and/or cleansing steps to your API data. For example:

        Replace Headers
        Format data into a more readable format
        Identify outliers and bad data
        Find duplicates
        Fix casing or inconsistent values
        Conduct Fuzzy Matching
    '''
    
    ############### Start of flat file data
    fundamentalDf = pd.read_csv("StockMarket/nyse/fundamentals.csv")
    fundamentalDf["Ticker_PeriodEnding"] = fundamentalDf["Ticker Symbol"] + "_" + fundamentalDf["Period Ending"]
    #print(fundamentalDf.head())
    #print("\nShape of fundamentals.csv\n", '-'*40, sep='')
    #print(fundamentalDf.shape)
    #outputs (1781, 79)
    
    
    # Replace headers
    # specify better headers for prices-split-adjusted.csv file to also match headings in fundamentals.csv
    pricesHeaders = ["Period Ending","Ticker Symbol","Open Price","Close Price","Low Price","High Price","Volume Exchanged"]
    pricesSplitAdjustedDf = pd.read_csv("StockMarket/nyse/prices-split-adjusted.csv",header=0,names=pricesHeaders)
    # interested in the price difference between Open Price and Close Price and difference between High Price and Low Price for volatility
    
    # round the columns with prices to two decimal places for better readability since most prices are to the nearest cents
    pricesSplitAdjustedDf = pricesSplitAdjustedDf.round({"Open Price":2,"Close Price":2,"Low Price":2,"High Price":2}) 

    
    # create new column to hold the difference between Open Price and Close Price
    pricesSplitAdjustedDf["Diff_Open_Close_Price"] = pricesSplitAdjustedDf["Open Price"] - pricesSplitAdjustedDf["Close Price"]
    # create new column to hold the difference between High Price and Low Price
    pricesSplitAdjustedDf["Diff_High_Low_Price"] = pricesSplitAdjustedDf["High Price"] - pricesSplitAdjustedDf["Low Price"]

    
    # create a new dataframe to hold outliers for Open and Close Price
    # box plot shows any value over 60 and less than -40 are outliers
    diffOpenClosePriceOutliers = pricesSplitAdjustedDf[(pricesSplitAdjustedDf["Diff_Open_Close_Price"] > 60) | (pricesSplitAdjustedDf["Diff_Open_Close_Price"] < -40)]
    #print(diffOpenClosePriceOutliers.head())
    #print("\nShape dataframe containing outliers for Open and Close Price: ", diffOpenClosePriceOutliers.shape)
    
    # print the box plot to show any outliers between Low and High Price calculations that may be of interest
    #print("\nBox Plot of Differences between High and Low Price\n", '-'*40, sep='')
    plt.boxplot(pricesSplitAdjustedDf["Diff_High_Low_Price"])
    #plt.show()
    
    # create a new dataframe to hold outliers for High and Low Price
    # box plot shows any value over 60 are outliers
    diffHighLowPriceOutliers = pricesSplitAdjustedDf[(pricesSplitAdjustedDf["Diff_High_Low_Price"] > 60)]
    #print(diffHighLowPriceOutliers.head())
    #print("\nShape dataframe containing outliers for High and Low Price: ", diffHighLowPriceOutliers.shape)
    
   
    securitiesDf = pd.read_csv("StockMarket/nyse/securities.csv")
    # need to rename "Ticker symbol" column heading to "Ticker Symbol" as used for data in other files
    securitiesDf.rename(columns={'Ticker symbol': 'Ticker Symbol', 'Security': 'Company Name'}, inplace=True)
    #print(securitiesDf.head())
    #print("\nShape of securities.csv\n", '-'*40, sep='')
    #print(securitiesDf.shape)
    
    # check for NaN values or other issues such as if Ticker Symbol needs to be changed to all caps
    #print("\nChecking securities.csv for issues:")
    # using isnull() method of the dataframe to check for NaN
    #print("Ticker Symbol has NaN- {}".format(any(securitiesDf["Ticker Symbol"].isnull())))
    #print("Period Ending has NaN- {}".format(any(securitiesDf["Company Name"].isnull())))
        # using str.islower() to check if Ticker Symbol has any lower case
    #print("Ticker Symbol has lowercase - {}".format(any(securitiesDf["Ticker Symbol"].str.islower())))
    # using str.isupper() to check if Ticker Symbol has only Upper case
    #print("Ticker Symbol has uppercase - {}".format(any(securitiesDf["Ticker Symbol"].str.isupper())))
    ############### End of flat file data
    
    
    ############### Start of web data
     
    # create the dataframe from the csv file
    websiteDf = pd.read_csv("webData_05022020.csv")
    #print(websiteDf.head())
    #print("\nShape of websiteDf created from webData_05022020.csv\n", '-'*40, sep='')
    #print(websiteDf.shape)
    #print(websiteDf.dtypes)
    #outputs (1781, 79)
    
    # noticed when viewing file in Excel and filtering that there were some rows that were completely null
    # need to remove 
    websiteDf = websiteDf.dropna(subset=["Fiscal_Year_End","Recent_Qtr","Profit_Margin","Operation_Margin","Return_On_Assets","Return_On_Equity","Revenue","Revenue_Per_Share","Gross_Profit","EBITDA","Total_Cash","High_52","Low_52"], how='all')
    #print("\nShape of websiteDf after removing row with all N/A values except the symbol\n", '-'*40, sep='')
    #print(websiteDf.shape)
    # assigning to another data frame as it might not be good to remove entries with just a few missing data
    websiteRemoveAllMissingDf = websiteDf.dropna()
    #print("\nShape of websiteDf if removing rows with any missing data\n", '-'*40, sep='')
    #print(websiteRemoveAllMissingDf.shape)
    
    #print("\nRenamed the column for the symbol to be consistent with the columns for the flat files\n", '-'*40, sep='')
    websiteDf.rename(columns={'Symbol': 'Ticker Symbol'}, inplace=True)
    #print(websiteDf.head())
    
    #print("\nConverting Fiscal_Year_End and Recent_Qtr to datetime format for better processing\n", '-'*40, sep='')
    websiteDf['Fiscal_Year_End'] = websiteDf['Fiscal_Year_End'].str.replace('[, ]', '', regex=True)
    websiteDf['Recent_Qtr'] = websiteDf['Recent_Qtr'].str.replace('[, ]', '', regex=True)
    #print(websiteDf.head())
    
    # need to remove unnecessary characters (,) from amount
    websiteDf['Revenue_Per_Share'] = websiteDf['Revenue_Per_Share'].str.replace('[, ]', '', regex=True)
    websiteDf = websiteDf.dropna()
    websiteDf['Fiscal_Year_End'] = pd.to_datetime(websiteDf['Fiscal_Year_End'], format='%b%d%Y')
    websiteDf['Recent_Qtr'] = pd.to_datetime(websiteDf['Recent_Qtr'], format='%b%d%Y')
    #print(websiteDf.head())
    
    #print("\nAnalyzing 52 Week High and Low Price Difference for outliers, bad data,\n", '-'*40, sep='')
    # Need to remove some quotes on the High and Low 52 week price
    # new dataframe for this part
    website52Df = websiteDf.dropna(subset=["High_52","Low_52"], how='all')
    website52Df['High_52'] = website52Df['High_52'].str.replace('["\']', '', regex=True)
    website52Df['Low_52'] = website52Df['Low_52'].str.replace('["\']', '', regex=True)
    website52Df['High_52'] = pd.to_numeric(website52Df['High_52'],errors='coerce')
    website52Df['Low_52'] = pd.to_numeric(website52Df['Low_52'],errors='coerce')
    
    
    # create new column to hold the difference between High 52 Price and Low 52 Price
    website52Df["Diff_High_Low_Price_52"] = website52Df["High_52"] - website52Df["Low_52"]
    website52Df = website52Df.dropna(subset=["Diff_High_Low_Price_52"])
    (website52Df["Diff_High_Low_Price_52"])
    # print the box plot to show any outliers between High and Low 52 Week Price calculations that may be of interest
    #print("\nBox Plot of Differences between High and Low 52 Week Price\n", '-'*40, sep='')
    plt.boxplot(website52Df["Diff_High_Low_Price_52"])
    #plt.show()
    #print("\nBox Plot shows outliers that are close to 500 or more difference between the High and Low 52 Week Price\n", '-'*40, sep='')
 
    # need to remove unnecessary characters (,) from amount
    websiteDf['Revenue_Per_Share'] = websiteDf['Revenue_Per_Share'].str.replace('[,]', '', regex=True)
    # need to remove null
    websiteDf = websiteDf.dropna()
    ############### End of web data
    
    ############### Start of api data
    
    # create the dataframe from the csv file for the API Data
    apiDf = pd.read_csv("apiData_05172020.csv")
    #print(apiDf.head())
    #print("\nShape of apiDf created from apiData_05172020.csv\n", '-'*40, sep='')
    #print(apiDf.shape)
    #print(apiDf.dtypes)
    #outputs (1781, 79)
    
    # noticed when viewing file in Excel and filtering that there were some rows that appeared to have issues 
    # those appeared to have price as 0, so we will just remove them 
    apiDf = apiDf[apiDf.price != 0]
    #print("\nShape of apiDf after removing row with all N/A values except the symbol\n", '-'*40, sep='')
    #print(apiDf.shape)

    
    #print("\nRenamed the column for the symbol to be consistent with the columns for the flat files\n", '-'*40, sep='')
    apiDf.rename(columns={'symbol': 'Ticker Symbol'}, inplace=True)
    #print(apiDf.head())
    
    #print("\nRound price, lastDiv, and changes to consistently display as amounts with two decimals\n", '-'*40, sep='')
    apiDf.round({'price': 2, 'lastDiv': 2, 'changes':2})
    #print(apiDf[['Ticker Symbol','price', 'lastDiv', 'changes']].head(10))
    
    #print("\nSplitting the range column into two float columns for low range and high range\n", '-'*40, sep='')
    rangeDf= apiDf["range"].str.split("-", n = 1, expand = True) 
    apiDf["lowRange"] = rangeDf[0].astype(float)
    apiDf["highRange"] = rangeDf[1].astype(float)
    #print(apiDf[['Ticker Symbol','range', 'lowRange', 'highRange']].head(10))
    
    ############### End of api data
    
    createDatabaseTables(fundamentalDf,pricesSplitAdjustedDf,securitiesDf,websiteDf,apiDf)
    
    createVisualizations()

    
if __name__ == '__main__':
    main()