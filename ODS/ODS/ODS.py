import pandas as pd
#Loads all data into dataFrame
class StoreODS:
 
   Product = pd.DataFrame (columns = ['ProductID','ProductDesciption','CategoryID','SupplierPrice','ProductPrice','SafetyStockLevels','ReorderPoint','SupplierID'])
   Category = pd.DataFrame (columns = ['CategoryID','CategoryDescription','ParentCategory']),
   SaleItem = pd.DataFrame (columns = ['SaleID','ProductID','Quantity']),
   StoreSale = pd.DataFrame (columns =['SaleID','StaffID','DateOfSale','SaleAmount','SalesTax','SaleTotal','StoreID']),
   Store = pd.DataFrame (columns =['StoreID','StoreAddress','StoreCity','StoreStateProvince','StoreCountry','StorePostCode','StorePhone']),
   Stock = pd.DataFrame (columns =['ProductID','StoreID','StockDate','Units']),
   Customer = pd.DataFrame (columns =['CustomerID','CustomerEmail','FirstName','SecondName','CustomerType','City','StateProvince','Country','PostalCode']),
   Employee = pd.DataFrame (columns =['EmployeeID','FirstName','LastName','BirthDate','Hiredate','EndDate','EmailAddress','Phone','EmergencyContactPhone','JobTitleID','Status','StoreID']),
   EmployeeStatus = pd.DataFrame (columns =['StatusID','StatusDescription']),
   InternetSales = pd.DataFrame (columns =['SaleID','CustomerID','DateOfSale','SaleAmount','SalesTax','SaleTotal']),
   InternetSaleItem = pd.DataFrame (columns =['SaleID','ProductID','Quantity','DateShipped','ShippingType']),
   Job = pd.DataFrame (columns =['JobTitleID','JobTitle','JobDescription','Salaried','PayRate','PayFrequencyID','VacationHours','SickLeaveAllowance','SalesPersonFlag']),
   PayFrequency = pd.DataFrame (columns =['PayFrequencyID','FrequencyName']),
   Supplier = pd.DataFrame (columns =['SupplierID','SupplierAddress','SupplierCity','SupplierStateProvince','SupplierCountry','SupplierPostCode','SupplierPhone'])
   CsvData = pd.DataFrame(columns = ['SaleID','FirstName','LastName','DateOfSale','Item','Quantity','SaleTotal'])
   JsonData = pd.DataFrame(columns =['Sale','SaleID','Customer','Delivery','DateOfSale','Sales','Product','Quantity','SubTotal','SaleTotal','TaxRate'])

   table = {'StoreODS.InternetSales',
            'StoreODS.StoreSale',
            'StoreODS.Product',
            'StoreODS.EmployeeStatus'}