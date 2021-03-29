# Conrad Ibanez
# Webscraping data from online source


import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests

# this method is from the textbook
def decode_content(r, encoding):
    return (r.content.decode(encoding))

# this method is from the textbook
def encoding_check(r):
    return (r.encoding)

# this method is to read in the html file and save the file to disk using the company's symbol in the filename
def readSaveHtml(companyList):
    # referenced this webpage for the code to get html and save- https://stackoverflow.com/questions/40529848/how-to-write-the-output-to-html-file-with-python-beautifulsoup
    # also read suggestions in course slack channel
    #soup = BeautifulSoup(open('1.html'),"html.parser")
    #html =soup.contents
    for c in companyList:
        #url = "https://finance.yahoo.com/quote/" + c
        url = "https://finance.yahoo.com/quote/" + c + "/key-statistics"
        response = requests.get(url)
        contents = decode_content(response,encoding_check(response))
        with open("../companyStats/" + c + "-stats.html", "w", encoding="utf-8") as file:
            file.write(contents)

# this method is to create the dataframe from processing the html files that were saved to disk; the dataframe is saved as csv file to disk
def createDataFrame(companyList):
    #companyList = companyList[0:400]
    #companyList = ['AAPL']
    
    # the below company symbols needed to be removed from the list as processing the data would not complete due to errors
    # many of these companies may be obsolete or have no data
    companyList.remove("AET") #Issue with Data
    companyList.remove("APC") #Not Valid
    companyList.remove("BHI") #Issue with Data
    companyList.remove("BCR") #Issue with Data
    companyList.remove("BBT") #Not Valid
    companyList.remove("BF.B") #Not Valid
    companyList.remove("CA") #Issue with Data
    companyList.remove("CBG") #Not Valid
    companyList.remove("CELG") 
    companyList.remove("COH")
    companyList.remove("CSRA")
    companyList.remove("DPS")
    companyList.remove("DNB")
    companyList.remove("EVHC")
    companyList.remove("ESRX")
    companyList.remove("GGP")
    companyList.remove("HAR")
    companyList.remove("HRS")
    companyList.remove("HCP")
    companyList.remove("LLL")
    companyList.remove("LUK")
    companyList.remove("LVLT")
    companyList.remove("LLTC")
    companyList.remove("MJN")
    companyList.remove("KORS")
    companyList.remove("MON")
    companyList.remove("NFX")
    companyList.remove("PX")
    companyList.remove("PCLN")
    companyList.remove("RHT")
    companyList.remove("RAI")
    companyList.remove("COL")
    companyList.remove("SCG")
    companyList.remove("SNI")
    companyList.remove("SPLS")
    companyList.remove("SYMC")
    companyList.remove("TDC")
    companyList.remove("TSO")
    companyList.remove("TWX")
    companyList.remove("TMK")
    companyList.remove("TSS")
    companyList.remove("VIAB")
    companyList.remove("HCN")
    companyList.remove("WFM") #Whole foods bought by Amazon
    companyList.remove("WYN")
    companyList.remove("XL")
    companyList.remove("YHOO") #Bought by Verizon
    
    columnNames = ["Symbol","Fiscal_Year_End","Recent_Qtr","Profit_Margin","Operation_Margin","Return_On_Assets","Return_On_Equity","Revenue","Revenue_Per_Share","Gross_Profit","EBITDA","Total_Cash","High_52","Low_52"]
    
    webDf= pd.DataFrame(columns = columnNames)
    
    '''
    print(companyList)
    for c in companyList:
        with open("../companyStats/" + c + "-stats.html", "rb") as fd:
            soup = BeautifulSoup(fd)
            tables = soup.findAll('table')
            print("\nThe total number of tables is: ", len(tables))
            fiscalYrEnd = soup.select("td[class*='Fw(500) Ta(end) Pstart(10px) Miw(60px)']")[0].text
            #recentQtr = soup.select("td[data-reactid*='310']")[0].text
            print(fiscalYrEnd)
            #print(recentQtr)
    '''
    for c in companyList:
        with open("../companyStats/" + c + "-stats.html", "rb") as fd:
            soup = BeautifulSoup(fd)
            tables = soup.findAll('table')
            #print("\nThe total number of tables is: ", len(tables))

            #print(recentQtr)
        
        
            
            # the following code was used to display the table contents
            # using the output and also the view-source and inspect element of the web browser to understand the table structures
            '''
            for i, table in enumerate(tables, start=1):
                print("\nTables ", i, "\n", '-'*40, sep='')
                print(table)
            '''
              
            #print(c,"++++++++++++++++++++++++++++++")
            # get the sources from tables[3]
            rItems = tables[3].findAll('tr')
            tItems0 = rItems[0].findAll('td')
            tItems1 = rItems[1].findAll('td')
            fiscalYrEnd = tItems0[1].getText().strip()
            recentQtr = tItems1[1].getText().strip()
            
            rItems = tables[4].findAll('tr')
            tItems0 = rItems[0].findAll('td')
            tItems1 = rItems[1].findAll('td')
            profitMargin = tItems0[1].getText().strip()
            opMargin = tItems1[1].getText().strip()
             
            rItems = tables[5].findAll('tr')
            tItems0 = rItems[0].findAll('td')
            tItems1 = rItems[1].findAll('td')
            returnAssets = tItems0[1].getText().strip()
            returnEquity = tItems1[1].getText().strip()
            
            rItems = tables[6].findAll('tr')
            tItems0 = rItems[0].findAll('td')
            tItems1 = rItems[1].findAll('td')
            tItems3 = rItems[3].findAll('td')
            tItems4 = rItems[4].findAll('td')
            revenue = tItems0[1].getText().strip()
            revenuePerShare= tItems1[1].getText().strip()
            grossProfit  = tItems3[1].getText().strip()
            ebitda  = tItems4[1].getText().strip()
            
            rItems = tables[7].findAll('tr')
            tItems0 = rItems[0].findAll('td')
            totalCash  = tItems0[1].getText().strip()
            
            rItems = tables[0].findAll('tr')
            tItems3 = rItems[3].findAll('td')
            tItems4 = rItems[4].findAll('td')
            high52 = tItems3[1].getText().strip()
            low52 = tItems4[1].getText().strip()
            
            '''
            print(fiscalYrEnd)
            print(recentQtr)
            print(profitMargin)
            print(opMargin)
            print(returnAssets)
            print(returnEquity)
            print(revenue)
            print(revenuePerShare)
            print(grossProfit)
            print(ebitda)
            print(totalCash)
            print(high52)
            print(low52)
            print("++++++++++++++++++++++++++++++")
            '''
            dictRow = {"Symbol":c,"Fiscal_Year_End":fiscalYrEnd,"Recent_Qtr":recentQtr,"Profit_Margin":profitMargin,"Operation_Margin":opMargin,"Return_On_Assets":returnAssets,"Return_On_Equity":returnEquity,"Revenue":revenue,"Revenue_Per_Share":revenuePerShare,"Gross_Profit":grossProfit,"EBITDA":ebitda,"Total_Cash":totalCash,"High_52":high52,"Low_52":low52}
            webDf = webDf.append(dictRow,ignore_index=True)
            
            #sourceNames =[t.findAll('a')[0].getText().strip() for t in tItems]
            #print("length of sourceNames: ", len(sourceNames))
            #for n in tItems0:
            #    (print(n))
    
    print(webDf.head())
    print(webDf.shape)
    # Saving dataframe to csv file as the processing of the data is taking some time
    webDf.to_csv('webData_05022020.csv', index=False)

def main():
    
    '''
    Milestone 3 (Weeks 7 & 8)
    Cleaning/Formatting Website Data
    
    Perform at least 5 data transformation and/or cleansing steps to your website data. For example:
    
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
    
    print("\nreadSaveHtml() was called in previous program run to save html files to ../companyStats/ directory\n", '-'*40, sep='')
    # the following method was used to get the html and save the file to disk
    #readSaveHtml(companyList)
    
    print("\ncreateDataFrame() was called in previous program run to process html files and create and save webData_05022020.csv to disk \n", '-'*40, sep='')
    # the following method was used to create the dataframe using the html files that was saved to disk
    # the dataframe is saved to disk in csv format to save time of processing the data every time program is run
    #createDataFrame(companyList)
     
    # create the dataframe from the csv file
    websiteDf = pd.read_csv("webData_05022020.csv")
    print(websiteDf.head())
    print("\nShape of websiteDf created from webData_05022020.csv\n", '-'*40, sep='')
    print(websiteDf.shape)
    print(websiteDf.dtypes)
    #outputs (1781, 79)
    
    # noticed when viewing file in Excel and filtering that there were some rows that were completely null
    # need to remove 
    websiteDf = websiteDf.dropna(subset=["Fiscal_Year_End","Recent_Qtr","Profit_Margin","Operation_Margin","Return_On_Assets","Return_On_Equity","Revenue","Revenue_Per_Share","Gross_Profit","EBITDA","Total_Cash","High_52","Low_52"], how='all')
    print("\nShape of websiteDf after removing row with all N/A values except the symbol\n", '-'*40, sep='')
    print(websiteDf.shape)
    # assigning to another data frame as it might not be good to remove entries with just a few missing data
    websiteRemoveAllMissingDf = websiteDf.dropna()
    print("\nShape of websiteDf if removing rows with any missing data\n", '-'*40, sep='')
    print(websiteRemoveAllMissingDf.shape)
    
    print("\nRenamed the column for the symbol to be consistent with the columns for the flat files\n", '-'*40, sep='')
    websiteDf.rename(columns={'Symbol': 'Ticker Symbol'}, inplace=True)
    print(websiteDf.head())
    
    print("\nConverting Fiscal_Year_End and Recent_Qtr to datetime format for better processing\n", '-'*40, sep='')
    websiteDf['Fiscal_Year_End'] = websiteDf['Fiscal_Year_End'].str.replace('[, ]', '', regex=True)
    websiteDf['Recent_Qtr'] = websiteDf['Recent_Qtr'].str.replace('[, ]', '', regex=True)
    print(websiteDf.head())
    
    websiteDf['Fiscal_Year_End'] = pd.to_datetime(websiteDf['Fiscal_Year_End'], format='%b%d%Y')
    websiteDf['Recent_Qtr'] = pd.to_datetime(websiteDf['Recent_Qtr'], format='%b%d%Y')
    print(websiteDf.head())
    
    print("\nAnalyzing 52 Week High and Low Price Difference for outliers, bad data,\n", '-'*40, sep='')
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
    print("\nBox Plot of Differences between High and Low 52 Week Price\n", '-'*40, sep='')
    plt.boxplot(website52Df["Diff_High_Low_Price_52"])
    plt.show()
    print("\nBox Plot shows outliers that are close to 500 or more difference between the High and Low 52 Week Price\n", '-'*40, sep='')
    
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