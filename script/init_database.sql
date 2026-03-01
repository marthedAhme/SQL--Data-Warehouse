/*

==============================================================

Create Database and Schemas

==============================================================

SCRIPT PURPOSE:

    This script provisions a fresh instance of the 'DataWarehouse' database.

    Execution workflow:
        1. Verifies whether the 'DataWarehouse' database already exists.
        2. If present, the database is safely dropped.
        3. Recreates the database from scratch.
        4. Establishes the foundational schema structure, including:
              - bronze   (Raw ingestion layer)
              - silver   (Cleansed and transformed layer)
              - gold     (Business-ready presentation layer)

    This layered architecture aligns with the Medallion Architecture design
    principles to support scalable, structured, and maintainable data processing.

    Intended Environment:
    Development and Testing only.

    Note:
    Ensure that proper backups and environment validation have been completed
    prior to execution.

CRITICAL WARNING:

Execution of this script will result in the immediate and permanent deletion
of the 'DataWarehouse' database, if present.

All associated data and database objects will be irrecoverably lost.
This operation is destructive and non-reversible.

Before proceeding:
- Verify that a full backup has been successfully completed.
- Confirm that you are connected to the correct SQL Server instance.
- Ensure proper authorization has been obtained.

This script is intended for Development or Test environments only.

Proceed at your own risk.
*/

USE master;
GO


-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO
-- Create the 'DatatWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
Go

-- Create Schemas
CREATE SCHEMA bronze;
Go

CREATE SCHEMA silver;
Go

CREATE SCHEMA Gold;
