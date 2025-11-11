/*
Setting up a professional, production-grade data warehouse structure following industry best practices (Medallion Architecture):​
1) Destroys any old version of the DataWarehouse database (fresh start).​
2) Creates a new empty database called DataWarehouse.​
3) Sets up three logical layers (bronze, silver, gold) as separate schemas:​
i) Bronze: For raw, unprocessed data from your CSV files.​
ii) Silver: For cleaned, validated, transformation-ready data.​
iii) Gold: For analytics-optimized, business-ready data (star schema, aggregations).
*/

-- Create Warehouse Database
USE master;
GO

-- drop and recreate database 'DataWarehouse' if already exists

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create Database

CREATE DATABASE DataWarehouse;
GO 

USE DataWarehouse;
GO
-- Create Schema

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO