USE master:
GO
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 3 FROM sys.databases WHERE name = Databarehouse")
BEGIN
  ALTER DATABASE Dataarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Databarehouse;
END;
GO
- Create the 'Databarehouse' database
CREATE DATABASE DataWarehouse;
60
USE DataWarehouse;
GO
-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
