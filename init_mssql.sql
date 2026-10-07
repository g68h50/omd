CREATE DATABASE DWH_BCS;

USE DWH_BCS;

ALTER DATABASE DWH_BCS SET QUERY_STORE = ON (
    OPERATION_MODE = READ_WRITE,
    CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30),
    DATA_FLUSH_INTERVAL_SECONDS = 60,
    INTERVAL_LENGTH_MINUTES = 5
);

USE master;
-- Создаем логин на уровне сервера
CREATE LOGIN openmetadata_user WITH PASSWORD = 'OmPassword123!';

-- Даем право просматривать системные DMV (sys.dm_exec_sql_text и др.)
GRANT VIEW SERVER STATE TO openmetadata_user;


USE DWH_BCS;


-- Создаем пользователя базы данных
CREATE USER openmetadata_user FOR LOGIN openmetadata_user;

-- Назначаем права на чтение данных и DDL-определений
ALTER ROLE db_datareader ADD MEMBER openmetadata_user;
GRANT VIEW DEFINITION TO openmetadata_user;

-----
USE DWH_BCS;

CREATE SCHEMA DWH;