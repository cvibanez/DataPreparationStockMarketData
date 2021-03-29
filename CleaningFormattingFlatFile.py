# Conrad Ibanez
# Cleaning/Formatting Flat File Source


import pandas as pd
import matplotlib.pyplot as plt


def main():
    
    '''
    Cleaning/Formatting Flat File Source
    
    Perform at least 5 data transformation and/or cleansing steps to your flat file data.
    
    I have completed the following using the three flat file sources in my data set
        Replaced Headers
        Formatted data into a more readable format by rounding stock prices to nearest cent
        Identified outliers using box plots for changes in stock prices
        Found duplicate values for certain columns in tables which were non-issues due to the nature of the data
          For example, Ticker Symbol shows many times in fundamentals.csv because there is company data for different years.
        Checked casing especially for Ticker Symbol as that relates together the flat file in the data.
        Labeled the header for "Ticker Symbol" to be consistent across the dataframes for the three file sources.
    
    '''
        
    fundamentalDf = pd.read_csv("StockMarket/nyse/fundamentals.csv")
    fundamentalDf["Ticker_PeriodEnding"] = fundamentalDf["Ticker Symbol"] + "_" + fundamentalDf["Period Ending"]
    print(fundamentalDf.head())
    print("\nShape of fundamentals.csv\n", '-'*40, sep='')
    print(fundamentalDf.shape)
    #outputs (1781, 79)
    
    
    # using the duplicated() method of the dataframe to check for duplicate values and using any to return boolean indicator if any duplicates
    print("\nChecking fundamentals.csv duplicates: ")
    print("Ticker Symbol duplicates - {}".format(any(fundamentalDf["Ticker Symbol"].duplicated())))
    print("Period Ending duplicates - {}".format(any(fundamentalDf["Period Ending"].duplicated())))
    print("Ticker_PeriodEnding duplicates - {}".format(any(fundamentalDf["Ticker_PeriodEnding"].duplicated())))

    # table contains duplicates of ticker symbols and Period Ending as there is data for same company for multiple years
    # some companies share the same Period Ending date
    # the duplicate to check for that makes sense is the combination of Ticker Symbol and Period Ending
    # create a new column that is the concatenation of the Ticker Symbol and Period Ending
    # adding a new column to the original dataframe to hold the string combinattion of Ticker Symbol and Period ending
    

    print("\nChecking fundamentals.csv for issues: ")
    # using isnull() method of the dataframe to check for NaN
    print("Ticker Symbol has NaN- {}".format(any(fundamentalDf["Ticker Symbol"].isnull())))
    print("Period Ending has NaN- {}".format(any(fundamentalDf["Period Ending"].isnull())))
    
    # using str.islower() to check if Ticker Symbol has any lower case
    print("Ticker Symbol has lowercase - {}".format(any(fundamentalDf["Ticker Symbol"].str.islower())))
    # using str.isupper() to check if Ticker Symbol has only Upper case
    print("Ticker Symbol has uppercase - {}".format(any(fundamentalDf["Ticker Symbol"].str.isupper())))
    
    # iterating the columns
    #print("\nColumns in fundamentals.csv\n", '-'*40, sep='')
    #for c in fundamentalDf.columns: 
    #    print(c) 
    
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
    print(pricesSplitAdjustedDf.head())
    print("\nShape of prices-split-adjusted.csv\n", '-'*40, sep='')
    print(pricesSplitAdjustedDf.shape)
    
    # check for NaN values or other issues such as if Ticker Symbol needs to be changed to all caps
    print("\nChecking prices-split-adjusted.csv for issues:")
    # using isnull() method of the dataframe to check for NaN
    print("Period Ending has NaN- {}".format(any(pricesSplitAdjustedDf["Period Ending"].isnull())))
    print("Ticker Symbol has NaN- {}".format(any(pricesSplitAdjustedDf["Ticker Symbol"].isnull())))
    print("Open Price has NaN- {}".format(any(pricesSplitAdjustedDf["Open Price"].isnull())))
    print("Close Price has NaN- {}".format(any(pricesSplitAdjustedDf["Close Price"].isnull())))
    print("Low Price has NaN- {}".format(any(pricesSplitAdjustedDf["Low Price"].isnull())))
    print("High Price has NaN- {}".format(any(pricesSplitAdjustedDf["High Price"].isnull())))
    print("Volume Exchanged has NaN- {}".format(any(pricesSplitAdjustedDf["Volume Exchanged"].isnull())))
    # using str.islower() to check if Ticker Symbol has any lower case
    print("Ticker Symbol has lowercase - {}".format(any(pricesSplitAdjustedDf["Ticker Symbol"].str.islower())))
    # using str.isupper() to check if Ticker Symbol has only Upper case
    print("Ticker Symbol has uppercase - {}".format(any(pricesSplitAdjustedDf["Ticker Symbol"].str.isupper())))
    
     
    
    # print the box plot to show any outliers between Open and Close Price calculations that may be of interest
    print("\nBox Plot of Differences between Open and Close Price\n", '-'*40, sep='')
    plt.boxplot(pricesSplitAdjustedDf["Diff_Open_Close_Price"])
    plt.show()
    
    # create a new dataframe to hold outliers for Open and Close Price
    # box plot shows any value over 60 and less than -40 are outliers
    diffOpenClosePriceOutliers = pricesSplitAdjustedDf[(pricesSplitAdjustedDf["Diff_Open_Close_Price"] > 60) | (pricesSplitAdjustedDf["Diff_Open_Close_Price"] < -40)]
    print(diffOpenClosePriceOutliers.head())
    print("\nShape dataframe containing outliers for Open and Close Price: ", diffOpenClosePriceOutliers.shape)
    
    # print the box plot to show any outliers between Low and High Price calculations that may be of interest
    print("\nBox Plot of Differences between High and Low Price\n", '-'*40, sep='')
    plt.boxplot(pricesSplitAdjustedDf["Diff_High_Low_Price"])
    plt.show()
    
    # create a new dataframe to hold outliers for High and Low Price
    # box plot shows any value over 60 are outliers
    diffHighLowPriceOutliers = pricesSplitAdjustedDf[(pricesSplitAdjustedDf["Diff_High_Low_Price"] > 60)]
    print(diffHighLowPriceOutliers.head())
    print("\nShape dataframe containing outliers for High and Low Price: ", diffHighLowPriceOutliers.shape)
    
    # save the new data frames
    print("\nSaving outliers data to PriceDiffOutliers.xlsx \n")
    writer = pd.ExcelWriter("PriceDiffOutliers.xlsx")
    diffOpenClosePriceOutliers.to_excel(writer, "diffOpenClosePriceOutliers")
    diffHighLowPriceOutliers.to_excel(writer, "diffHighLowPriceOutliers")
    writer.save()
    
    #print("\nColumns in prices-split-adjusted.csv\n", '-'*40, sep='')
    #for c in pricesSplitAdjustedDf.columns: 
    #    print(c)
        
   
    securitiesDf = pd.read_csv("StockMarket/nyse/securities.csv")
    # need to rename "Ticker symbol" column heading to "Ticker Symbol" as used for data in other files
    securitiesDf.rename(columns={'Ticker symbol': 'Ticker Symbol', 'Security': 'Company Name'}, inplace=True)
    print(securitiesDf.head())
    print("\nShape of securities.csv\n", '-'*40, sep='')
    print(securitiesDf.shape)
    
    # check for NaN values or other issues such as if Ticker Symbol needs to be changed to all caps
    print("\nChecking securities.csv for issues:")
    # using isnull() method of the dataframe to check for NaN
    print("Ticker Symbol has NaN- {}".format(any(securitiesDf["Ticker Symbol"].isnull())))
    print("Period Ending has NaN- {}".format(any(securitiesDf["Company Name"].isnull())))
        # using str.islower() to check if Ticker Symbol has any lower case
    print("Ticker Symbol has lowercase - {}".format(any(securitiesDf["Ticker Symbol"].str.islower())))
    # using str.isupper() to check if Ticker Symbol has only Upper case
    print("Ticker Symbol has uppercase - {}".format(any(securitiesDf["Ticker Symbol"].str.isupper())))
    
    #print("\nColumns in securities.csv\n", '-'*40, sep='')
    #for c in securitiesDf.columns: 
    #    print(c)
        
    # Web Service
    # https://financialmodelingprep.com/api/v3/company/profile/AAPL,FB
    # https://finnhub.io/docs/api#company-executive
    
    # Website - Yahoo finance
    # https://finance.yahoo.com/quote/AAPL/profile?p=AAPL
    
    
    
if __name__ == '__main__':
    main()