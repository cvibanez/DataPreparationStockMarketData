# Conrad Ibanez
# Processing JSON from API 


import pandas as pd
import matplotlib.pyplot as plt
#from bs4 import BeautifulSoup
import requests
import json


# this method is to read in json file from the API and save the file to disk using the company's symbol in the filename    
# this method will also create the dataframe from the API data and save the dataframe to a csv file to be processed
# this will make the program run faster since we do not need to process API data every time.        
def readSaveJson(companyList):
    
    #companyList=["AAPL","WFM","YHOO","MSFT"]
    columnsList=['symbol','price','beta','volAvg','mktCap','lastDiv','range','changes','changesPercentage','exchange','industry','description','ceo','sector','image']
    jsonDataList = []
  
    for c in companyList:
        jsonData = requests.get("https://financialmodelingprep.com/api/v3/company/profile/" + c).json()
        if 'error' not in jsonData and "symbol" in jsonData:
            jsonDataList.append([jsonData['symbol'],jsonData['profile']['price'],jsonData['profile']['beta'],jsonData['profile']['volAvg'],jsonData['profile']['mktCap'],jsonData['profile']['lastDiv'],jsonData['profile']['range'],jsonData['profile']['changes'],jsonData['profile']['changesPercentage'],jsonData['profile']['exchange'],jsonData['profile']['industry'],jsonData['profile']['description'],jsonData['profile']['ceo'],jsonData['profile']['sector'],jsonData['profile']['image']])
            #print(jsonData)
            with open("../apiJson/" + c + "-api.json", "w", encoding="utf-8") as jsonFile:
                json.dump(jsonData, jsonFile)
            
    apiDf = pd.DataFrame(jsonDataList, columns=columnsList)
    print(apiDf.head())
    print(apiDf.shape)
    # Saving dataframe to csv file as the processing of the data is taking some time
    apiDf.to_csv('apiData_05172020.csv', index=False)
    

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
    
    securitiesDf = pd.read_csv("StockMarket/nyse/securities.csv")
    # need to rename "Ticker symbol" column heading to "Ticker Symbol" as used for data in other files
    securitiesDf.rename(columns={'Ticker symbol': 'Ticker Symbol', 'Security': 'Company Name'}, inplace=True)
    print(securitiesDf.head())
    print("\nShape of securities.csv\n", '-'*40, sep='')
    print(securitiesDf.shape)
    
    #get all the ticker symbols for companies
    companyList = securitiesDf["Ticker Symbol"].tolist()
    print("\ncompanyList contains this many companies: ", len(companyList))
    
    print("\nreadSaveJson() was called in previous program run to save json files to ../apiJson/ directory\n", '-'*40, sep='')
    print("\nreadSaveJson() also saved the dataframe data into apiData_05172020.csv so this file could be read into this program\n", '-'*40, sep='')
    # the following method was used to get the json and save the json files as well as the dataframe csv to disk
    #readSaveJson(companyList)
 
    # create the dataframe from the csv file
    apiDf = pd.read_csv("apiData_05172020.csv")
    print(apiDf.head())
    print("\nShape of apiDf created from apiData_05172020.csv\n", '-'*40, sep='')
    print(apiDf.shape)
    print(apiDf.dtypes)
    #outputs (1781, 79)
    
    # noticed when viewing file in Excel and filtering that there were some rows that appeared to have issues 
    # those appeared to have price as 0, so we will just remove them 
    apiDf = apiDf[apiDf.price != 0]
    print("\nShape of apiDf after removing row with all N/A values except the symbol\n", '-'*40, sep='')
    print(apiDf.shape)

    
    print("\nRenamed the column for the symbol to be consistent with the columns for the flat files\n", '-'*40, sep='')
    apiDf.rename(columns={'symbol': 'Ticker Symbol'}, inplace=True)
    print(apiDf.head())
    
    print("\nRound price, lastDiv, and changes to consistently display as amounts with two decimals\n", '-'*40, sep='')
    apiDf.round({'price': 2, 'lastDiv': 2, 'changes':2})
    print(apiDf[['Ticker Symbol','price', 'lastDiv', 'changes']].head(10))
    
    print("\nSplitting the range column into two float columns for low range and high range\n", '-'*40, sep='')
    rangeDf= apiDf["range"].str.split("-", n = 1, expand = True) 
    apiDf["lowRange"] = rangeDf[0].astype(float)
    apiDf["highRange"] = rangeDf[1].astype(float)
    print(apiDf[['Ticker Symbol','range', 'lowRange', 'highRange']].head(10))
    
    # print the box plot to show any outliers for price that may be of interest
    print("\nBox Plot of Price\n", '-'*40, sep='')
    plt.boxplot(apiDf["price"])
    plt.show()
    print("\nBox Plot shows outliers for price that are over 1000\n", '-'*40, sep='')
    
    # print the box plot to show any outliers for market cap that may be of interest
    print("\nBox Plot of Market Cap\n", '-'*40, sep='')
    plt.boxplot(apiDf["mktCap"])
    plt.show()
    print("\nBox Plot shows outliers for market cap that are over 500 billion\n", '-'*40, sep='')

    print("\nDisplaying the number of companies for each sector", '-'*40, sep='')
    print(apiDf.groupby('sector').size())
    sectorGroups = apiDf.groupby("sector")
    plt.figure(figsize=(8,14))
    plt.pie(sectorGroups.size(), labels= sectorGroups.groups, autopct='%1.1f%%', shadow=True, startangle=140)
    plt.title("Sector Groups Pie Chart", fontsize=20)
    plt.show()
    
if __name__ == '__main__':
    main()