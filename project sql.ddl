USE InsigniaDB; -- Ensure we're using the right database  

-- Create Lineage Table  
CREATE TABLE Lineage_Table (  
    Lineage_Id BIGINT IDENTITY(1,1) PRIMARY KEY,  
    Source_System VARCHAR(100),  
    Load_Start_Datetime DATETIME,  
    Load_End_Datetime DATETIME,  
    Rows_at_Source INT,  
    Rows_at_Destination_Fact INT,  
    Load_Status BIT  
);
-- Date Dimension Table  
CREATE TABLE Date_Dimension (  
    DateKey INT PRIMARY KEY,  
    Date DATETIME,  
    Day_Number INT,  
    Month_Name VARCHAR(100),  
    Short_Month CHAR(3),  
    Calendar_Month_Number INT,  
    Calendar_Year INT,  
    Fiscal_Month_Number INT,  
    Fiscal_Year INT,  
    Week_Number INT,  
    Lineage_Id BIGINT -- to trace the load  
);  

-- Employee Dimension Table (SCD Type 2)  
CREATE TABLE Employee_Dimension (  
    Employee_Id INT PRIMARY KEY,  
    Employee_Name VARCHAR(255),  
    Department VARCHAR(100),  
    Hire_Date DATETIME,  
    Is_Active BIT,  
    Effective_Start_Date DATETIME,  
    Effective_End_Date DATETIME,  
    Lineage_Id BIGINT  
);  

-- Customer Dimension Table (SCD Type 2)  
CREATE TABLE Customer_Dimension (  
    Customer_Id INT PRIMARY KEY,  
    Customer_Name VARCHAR(255),  
    Email VARCHAR(255),  
    Is_Active BIT,  
    Effective_Start_Date DATETIME,  
    Effective_End_Date DATETIME,  
    Lineage_Id BIGINT  
);  

-- Geography Dimension Table (SCD Type 3)  
CREATE TABLE Geography_Dimension (  
    Geography_Id INT PRIMARY KEY,  
    Country VARCHAR(100),  
    State VARCHAR(100),  
    City VARCHAR(100),  
    Population INT,  
    Previous_Population INT,  
    Lineage_Id BIGINT  
);  

-- Product Dimension Table (SCD Type 1)  
CREATE TABLE Product_Dimension (  
    Product_Id INT PRIMARY KEY,  
    Product_Name VARCHAR(255),  
    Category VARCHAR(100),  
    Price DECIMAL(10, 2),  
    Lineage_Id BIGINT  
);  

-- Sales Fact Table  
CREATE TABLE Sales_Fact (  
    Sale_Id INT PRIMARY KEY IDENTITY(1,1),  
    DateKey INT,  
    Employee_Id INT,  
    Customer_Id INT,  
    Geography_Id INT,  
    Product_Id INT,  
    Sale_Amount DECIMAL(10, 2),  
    Quantity INT,  
    Lineage_Id BIGINT,  
    FOREIGN KEY (DateKey) REFERENCES Date_Dimension(DateKey),  
    FOREIGN KEY (Employee_Id) REFERENCES Employee_Dimension(Employee_Id),  
    FOREIGN KEY (Customer_Id) REFERENCES Customer_Dimension(Customer_Id),  
    FOREIGN KEY (Geography_Id) REFERENCES Geography_Dimension(Geography_Id),  
    FOREIGN KEY (Product_Id) REFERENCES Product_Dimension(Product_Id)  
);
-- Insert date dimension data  
DECLARE @StartDate DATETIME = '2000-01-01';  
DECLARE @EndDate DATETIME = '2023-12-31';  
DECLARE @CurrentDate DATETIME = @StartDate;  

WHILE @CurrentDate <= @EndDate  
BEGIN  
    INSERT INTO Date_Dimension (DateKey, Date, Day_Number, Month_Name, Short_Month,  
        Calendar_Month_Number, Calendar_Year, Fiscal_Month_Number, Fiscal_Year, Week_Number)  
    VALUES (  
        CAST(CONVERT(VARCHAR(8), @CurrentDate, 112) AS INT),   
        @CurrentDate,  
        DAY(@CurrentDate),  
        DATENAME(MONTH, @CurrentDate),  
        UPPER(LEFT(DATENAME(MONTH, @CurrentDate), 3)),  
        MONTH(@CurrentDate),  
        YEAR(@CurrentDate),  
        (MONTH(@CurrentDate) + 5) % 12 + 1,  -- Fiscal month starts from July  
        CASE WHEN MONTH(@CurrentDate) >= 7 THEN YEAR(@CurrentDate) ELSE YEAR(@CurrentDate) - 1 END,  
        DATEPART(WEEK, @CurrentDate)  
    );  

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);  
END;
-- Create a copy of Insignia_staging table  
CREATE TABLE Insignia_staging_copy AS   
SELECT * FROM Insignia_staging WHERE 1=0;
CREATE PROCEDURE ETL_Insignia_Staging  
AS  
BEGIN  
    DECLARE @StartTime DATETIME = GETDATE();  
    DECLARE @EndTime DATETIME;  
    DECLARE @LoadStatus BIT = 1; -- assuming success  
    DECLARE @RowsAtSource INT;  
    DECLARE @RowsAtDestination INT;  
    DECLARE @Lineage_ID BIGINT;  

    -- 1. Truncate the staging copy  
    TRUNCATE TABLE Insignia_staging_copy;  

    -- 2. Copy data to staging copy  
    INSERT INTO Insignia_staging_copy  
    SELECT * FROM Insignia_staging;  

    SET @RowsAtSource = @@ROWCOUNT;  

    -- 3. Load data into the dimensions  
    -- Load Employee Dimension (SCD Type 2)  
    -- (Implement logic for handling SCD here - check for existing records, manage effective dates)  

    -- Load Customer Dimension (Same as Employee Dimension)  
    
    -- Load Geography Dimension (SCD Type 3)  
    -- (Implement logic for adding/updating populations)  

    -- Load Product Dimension (SCD Type 1)  
    -- (Simply overwrite existing data with the new records)  

    -- 4. Load Sales Fact Table  
    -- (Insert new sales records from Insignia_staging_copy)  

    -- 5. Insert data to lineage table  
    SET @RowsAtDestination = (SELECT COUNT(*) FROM Sales_Fact);  
    
    INSERT INTO Lineage_Table (Source_System, Load_Start_Datetime, Load_End_Datetime,   
        Rows_at_Source, Rows_at_Destination_Fact, Load_Status)  
    VALUES ('Insignia', @StartTime, GETDATE(), @RowsAtSource, @RowsAtDestination, @LoadStatus);  

    -- Finalize ETL run  
    SET @EndTime = GETDATE();  
END;
-- Load Example for Employee Dimension (SCD Type 2)  
WITH SourceEmp AS (  
    SELECT * FROM Insignia_staging_copy  
),  
ExistingEmp AS (  
    SELECT * FROM Employee_Dimension  
)  
-- Insert new and updated records  
INSERT INTO Employee_Dimension (Employee_Id, Employee_Name, Department, Hire_Date, Is_Active, Effective_Start_Date, Effective_End_Date, Lineage_Id)  
SELECT s.Employee_Id,   
       s.Employee_Name,   
       s.Department,   
       s.Hire_Date,  
       s.Is_Active,  
       GETDATE(),   
       NULL,   
       @Lineage_ID  
FROM SourceEmp s  
LEFT JOIN ExistingEmp e ON s.Employee_Id = e.Employee_Id  
WHERE e.Employee_Id IS NULL OR (s.Is_Active != e.Is_Active);
CREATE PROCEDURE Reconciliation_Module  
AS  
BEGIN  
    DECLARE @SourceCount INT = (SELECT COUNT(*) FROM Insignia_staging_copy);  
    DECLARE @FactCount INT = (SELECT COUNT(*) FROM Sales_Fact);  
    
    IF @SourceCount = @FactCount  
    BEGIN  
        PRINT 'Reconciliation Successful: Row counts match.';  
    END  
    ELSE  
    BEGIN  
        PRINT 'Reconciliation Failed: Source rows = ' + CONVERT(VARCHAR(10), @SourceCount) +   
              ', Destination rows = ' + CONVERT(VARCHAR(10), @FactCount);  
    END  
END;
