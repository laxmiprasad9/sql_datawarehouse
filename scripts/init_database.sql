USE master;
GO
IF EXISTS (SELECT 1 FROM SYS.DATABASES WHERE name = 'Db1')
BEGIN
  ALTER DATABASE Db1 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Db1;
END;
GO
CREATE DATABASE Db1;

USE Db1;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
