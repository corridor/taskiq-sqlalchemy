IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'taskiq') CREATE DATABASE taskiq;
GO

USE [taskiq];
GO

IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'taskiq_user')
BEGIN
    CREATE LOGIN [taskiq_user] WITH PASSWORD = 'gOxN5hbl7geTwgvS', CHECK_POLICY = OFF;
    ALTER SERVER ROLE [sysadmin] ADD MEMBER [taskiq_user];
END
GO
