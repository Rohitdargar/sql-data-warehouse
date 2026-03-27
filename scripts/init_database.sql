/* 
==========================
Create Database and Schemas
===========================

Purpose:
This script creates a new database named 'DataWarehouse'. 
It will first check if the database 'DataWarehouse' already exists.
If exist --> drop the database 'DataWarehouse' and recreate it.

Aditionally, 3 schemas will be setup within the database --> 'broze', 'silver', 'gold'.

*/

-- Create Database 'Datawarehouse'

USE master;
GO

  -- Drop and recreate database --> 'DataWarehouse'.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse
END;
GO

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

