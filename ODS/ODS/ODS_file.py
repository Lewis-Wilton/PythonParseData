#Library
import pyodbc
import pandas as pd
import time
import urllib
from datetime import datetime
import json
#Classes
from sqlalchemy.engine import create_engine
from ODS import StoreODS

#Reads all data into dataframe

class Main:
    print("main")
    def __init__(self):
        #Connection to SQL Server
        connectionString = "DRIVER={SQL Server};SERVER=sql2016.fse.network;Database=db_1814736_1814736;UID=user_db_1814736_1814736;PWD=EasyPWD"
        self.conn = pyodbc.connect(connectionString)
        self.cursor = self.conn.cursor()

        #Read from SQL into Python Dataframes
        StoreODS.Product = pd.read_sql_query('SELECT * FROM dbo.Product',self.conn)
        StoreODS.Category = pd.read_sql_query('SELECT * FROM dbo.Category',self.conn)
        StoreODS.SaleItem = pd.read_sql_query('SELECT * FROM dbo.SaleItem',self.conn)
        StoreODS.StoreSale = pd.read_sql_query('SELECT * FROM dbo.StoreSale',self.conn)
        StoreODS.Store = pd.read_sql_query('SELECT * FROM dbo.Store',self.conn)
        StoreODS.Stock = pd.read_sql_query('SELECT * FROM dbo.Stock',self.conn)
        StoreODS.Supplier = pd.read_sql_query('SELECT * FROM dbo.Supplier',self.conn)
        StoreODS.Employee = pd.read_sql_query('SELECT * FROM dbo.Employee',self.conn)
        StoreODS.EmployeeStatus = pd.read_sql_query('SELECT * FROM dbo.EmployeeStatus',self.conn)
        StoreODS.Job = pd.read_sql_query('SELECT * FROM dbo.Job',self.conn)
        StoreODS.PayFrequency = pd.read_sql_query('SELECT * FROM dbo.PayFrequency',self.conn)
        StoreODS.InternetSaleItem = pd.read_sql_query('SELECT * FROM dbo.InternetSaleItem',self.conn)
        StoreODS.InternetSales = pd.read_sql_query('SELECT * FROM dbo.InternetSale',self.conn)
        StoreODS.Customer = pd.read_sql_query('SELECT * FROM dbo.Customer',self.conn)

        #InternetSales convert date
        StoreODS.InternetSales['DateOfSale'] = pd.to_datetime(StoreODS.InternetSales['DateOfSale'])
        pd.read_csv(dtype={''})
        #Read CSV into Python Dataframes
        StoreODS.CsvData = pd.read_csv("SalesCSV.csv",encoding="ISO-8859-1")
        StoreODS.CsvData.rename(columns ={'sale':'SaleID','employee':'FirstName','date':'DateOfSale','item':'ProductID','quantity':'Quantity','total':'SaleTotal'},inplace = True)
        StoreODS.CsvData['DateOfSale'] = pd.to_datetime(StoreODS.CsvData['DateOfSale'])
        StoreODS.CsvData[['FirstName','LastName']] = StoreODS.CsvData['FirstName'].loc[StoreODS.CsvData['FirstName'].str.split().str.len() == 2].str.split(expand=True)
        StoreODS.CsvData = StoreODS.CsvData [['SaleID','FirstName','LastName','DateOfSale','ProductID','Quantity','SaleTotal']]
        print(StoreODS.Csv.to_string())

        #ReadJSON into Python Dataframes
        jsonFile = "SalesJson.json" 
        with open (jsonFile) as f:
            StoreODS.JsonData = json.load(f, encoding="ISO-8859-1")
            StoreODS.JsonData =  pd.json_normalize(StoreODS.JsonData['Sale'])
            StoreODS.JsonData = StoreODS.JsonData.explode('Sales')
            StoreODS.JsonData = pd.concat(
                [
                    StoreODS.JsonData.drop(['Sales'],axis=1), 
                 StoreODS.JsonData['Sales'].apply(pd.Series)],axis=1)

            #Change data format
            StoreODS.JsonData['DateOfSale'] = pd.to_datetime(StoreODS.JsonData['DateOfSale'])

            #rename columns to fit to internetSales
            StoreODS.JsonData.rename(columns={'SaleID':'SaleID1','Customer':'FirstName','Delivery':'City','SubTotal':'SaleAmount','SaleTax':'SalesTax','Product':'ProductID'},inplace=True)
            StoreODS.JsonData[['FirstName','SecondName']] = StoreODS.JsonData['FirstName'].loc[StoreODS.JsonData['FirstName'].str.split().str.len() == 2].str.split(expand=True)
            StoreODS.JsonData[['City','StateProvince','Country','PostalCode']] = StoreODS.JsonData['City'].str.split(',',expand=True)
            StoreODS.JsonData.drop(columns=['Subtotal','TaxRate'],inplace=True)
            StoreODS.JsonData = StoreODS.JsonData[['SaleID1','ProductID','DateOfSale','SaleAmount','SalesTax','SaleTotal','FirstName','SecondName','City','StateProvince','Country','PostalCode']]
            #print(StoreODS.JsonData.to_string())
            print("Print json")

        self.Surrogate()
        self.Merge()

        #print(StoreODS.Product.to_string())
        #print(StoreODS.EmployeeStatus.to_string())
        #print(StoreODS.CsvData.to_string())
        

        #Merge Store and StoreSale into one
    def Merge(self):
            
        #Combining Fact_SaleItem & StoreSale & Store
            StoreODS.StoreSale = pd.merge(StoreODS.SaleItem,StoreODS.StoreSale,
                                  left_on='SaleID',
                                  right_on='SaleID', how='right').merge(StoreODS.Store)
            StoreODS.StoreSale = pd.concat([StoreODS.CsvData,StoreODS.StoreSale])
            StoreODS.StoreSale = StoreODS.StoreSale.replace('',regex=True)
            StoreSale = StoreODS.StoreSale.groupby('SaleID',as_index=False,sort=False).last()
            StoreODS.StoreSale['DateOfSale'] = pd.to_datetime(StoreODS.StoreSale['DateOfSale'])
            StoreODS.StoreSale.drop_duplicates()
            #print(StoreODS.StoreSale.to_string())
            print("Merge first ")

            
            #Combining Product & Category & Supplier
            StoreODS.Product = pd.merge(StoreODS.Supplier,StoreODS.Product,
                                         left_on='SupplierID',
                                         right_on='SupplierID',how='inner').merge(StoreODS.Category)
            
            print("Merge 2nd ")
            #print(StoreODS.Product.to_string())
                                         
            #Combining InternetSales & InterentSales & Customer
            StoreODS.InternetSales = pd.merge(StoreODS.InternetSaleItem,StoreODS.InternetSales,
                                             left_on='SaleID',
                                             right_on='SaleID',how='right').merge(StoreODS.Customer)
            StoreODS.InternetSales = pd.concat([StoreODS.InternetSales,StoreODS.JsonData],ignore_index=True)
            StoreODS.InternetSales.drop_duplicates()
            #print(StoreODS.InternetSales.to_string())
            print("Merge 3rd ")
            #Combining EmployeeStatus & Employee & Job & PayFrequency
            StoreODS.EmployeeStatus = pd.merge(StoreODS.Employee,StoreODS.EmployeeStatus,
                                               left_on='EmployeeID',
                                               right_on='StatusID',how='left').merge(StoreODS.Job).merge(StoreODS.PayFrequency)
            StoreODS.EmployeeStatus['StatusID'] = StoreODS.EmployeeStatus['Status'].where(StoreODS.EmployeeStatus['Status'].str.contains('Current')).ffill()
            #StoreODS.EmployeeStatus = EmployeeDataframe
            print("Merge 4th ")
            #print(StoreODS.EmployeeStatus.to_string())

    def Surrogate(self):
            #CSV
            StoreODS.CsvData['BookingID1'] = "EMP-" + StoreODS.CsvData['FirstName'].str[0] + StoreODS.CsvData['LastName'].str[0]
            StoreODS.CsvData['BookingID2'] = range(0, len(StoreODS.CsvData.index))
            StoreODS.CsvData['StaffID'] = StoreODS.CsvData['BookingID1'] + StoreODS.CsvData['BookingID2'].astype(str)
            StoreODS.CsvData = StoreODS.CsvData.drop(columns=['BookingID1','BookingID2','FirstName','LastName'])
            print("Surrogate key 1")
            #JSON #CustomerEmail
            StoreODS.JsonData['Email1'] = StoreODS.JsonData['FirstName'] +'.'+ StoreODS.JsonData['SecondName'] + "@shopping.com"
            StoreODS.JsonData['CustomerEmail'] = StoreODS.JsonData['Email1'].astype(str)
            StoreODS.JsonData = StoreODS.JsonData.drop(columns=['Email1'])
            print("Surrogate key 2")
            #SaleID = DateOfSale + Province
            StoreODS.JsonData['SaleID3'] = "US-" + StoreODS.JsonData['DateOfSale'].dt.strftime("%Y") + '-'
            StoreODS.JsonData['SaleID2'] = range(0, len(StoreODS.JsonData.index))
            StoreODS.JsonData['SaleID'] = StoreODS.JsonData['SaleID3'] + StoreODS.JsonData['SaleID2'].astype(str)
            StoreODS.JsonData = StoreODS.JsonData.drop(columns=['SaleID2','SaleID1','SaleID3'])
            #Export sql string
            print("Surrogate key 3")
                #export back to sql 

    def BuildTables(export):
        buildQuery = '''
            DROP TABLE IF EXISTS StoreSale
            CREATE TABLE StoreSale(
                SaleID int(1500) NOT NULL,DateOfSale date,
                ProductID nvarchar(70),Quantity int,
                SaleTotal money,StaffID nvarchar,
                SaleAmount money,SalesTax money,
                StoreID nvarchar(40),StoreAddress char(100),
                StoreCity char(80),StoreStateProvince char(50),
                StoreCountry char(3),StorePostCode int,
                StorePhone bigint,
                CONSTRAINT PK_StoreSale PRIMARY KEY (SaleID)
            )
        '''
        buildQuery = '''
           DROP TABLE IF EXISTS InternetSales
           CREATE TABLE InternetSales(
               SaleID nvarchar(15) NOT NULL,ProductID nvarchar(20),
               Quantity decimal(10,0),DateShipped date,
               ShippingType varchar(30),CustomerID nvarchar(20),
               DateOfSale date,SaleAmount money,
               SalesTax money,SaleTotal money,
               FirstName char(50),SecondName char(50)
               CustomerType char(30),City char(20),
               StateProvince char(10),Country char(40),
               PostalCode int,
               CONSTRAINT PK_InternetSales PRIMARY KEY (SaleID)
            )
        '''
        buildQuery = '''
          DROP TABLE IF EXISTS ProductSupplies
          CREATE TABLE ProductSupplies(
              SupplierID varchar(15) NOT NULL,SupplierAddress varchar(40),
              SupplierCity char(20),SupplierStateProvince char(30),
              SupplierCountry char(4),SupplierPostCode int,
              SupplierPhone bigint,ProductID nvarchar(30),
              ProductDescription nvarchar(100),CategoryID char(30),
              SupplierPrice money,productPrice money,
              SafetyStockLevel int,ReOrderPoint int,
              CategoryDescription char(10),ParentCategory char(40),
              CONSTRAINT PK_ProductSupplies PRIMARY KEY (SupplierID)
           )
       '''
        buildQuery = '''
         DROP TABLE IF EXISTS EmployeeInfomation
         CREATE TABLE EmployeeInfomation(
             EmployeeID nvarchar(20) NOT NULL,FirstName char(40),
             LastName char(20),BirthDate date,
             EndDate char(4),EmailAddress nvarchar(30),
             Phone bigint,EmergencyContactPhone bigint,
             JobTitleID char(100),Status char(30),
             StoreID nvarchar(20),StatusID varchar(20),
             StatusDescription nvarchar(40),JobTitle char(30),
             JobDescription char(20),Salaried char(15),
             PayRate bigint,PayFrequencyID int,
             VactionHours int,SickLeaveAllowance int,
             SalesPersonFlag char(5),FrequencyName char(15),
             CONSTRAINT PK_EmployeeInfomation PRIMARY KEY (EmployeeID)
           )
       '''
    def ExportODS(export):
        connectionString2 = "DRIVER={SQL Server};SERVER=sql2016.fse.network;Database=db_1814736_assignment1co5222;UID=user_db_1814736_assignment1co5222;PWD=EasyPWD1"
        export.connectionString2 = pyodbc.connect(connectionString2)
        export.cursor = export.connectionString2.cursor()
        params = urllib.parse.quote_plus(connectionString2)
        export.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        start_time = time.time()
        StoreODS.StoreSale.to_sql("StoreSale", method='multi',chunksize=int(2100/len(StoreODS.StoreSale.columns))-1,index=False, con = export.engine,
                                  if_exists='append')
        print(f"Completed in{(time.time() - start_time)} seconds")
        StoreODS.InternetSales.to_sql("InternetSales", method='multi',chunksize=int(2100/len(StoreODS.InternetSales.columns))-1, index=False, con = export.engine,
                                  if_exists='append')
        print(f"Completed in{(time.time() - start_time)} seconds")
        StoreODS.Product.to_sql("ProductSupplies", method='multi',chunksize=int(2100/len(StoreODS.Product.columns))-1, index=False, con = export.engine,
                                  if_exists='append')
        print(f"Completed in{(time.time() - start_time)} seconds")
        StoreODS.EmployeeStatus.to_sql("EmployeeInfomation", method='multi',chunksize=int(2100/len(StoreODS.EmployeeStatus.columns))-1, index=False, con = export.engine,
                                  if_exists='append')
        print(f"Completed in{(time.time() - start_time)} seconds")

        export.cursor.execute(buildQuery)
        export.connectionString2.commit() 
        
main = Main()
main.BuildTables()
main.ExportODS()
    