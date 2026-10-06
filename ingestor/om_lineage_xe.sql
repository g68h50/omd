/* =====================================================================
   Сессия Extended Events для mssql_lineage_ext: привязка динамического SQL к процедуре.

   Цель ring_buffer — события только в памяти сервера, файлов нет. Событий минимум:
     module_start / module_end  — стек вызовов процедур (без текста);
     sp_statement_completed     — только динамический SQL (PREPARED = 20816, ADHOC = 20801), без текста:
                                  инжестор сопоставляет его с Query Store по query_hash.

   Выполнять под логином с ALTER ANY EVENT SESSION. Сессия стартует вместе с сервером (STARTUP_STATE = ON).
   Пересоздание сессии очищает буфер: привязка динамического SQL восстановится с новыми запусками процедур.

   Параметры — в начале скрипта:
     @databases      базы через запятую (те же, что в параметре databases сервиса); пусто — все базы
     @exclude_like   служебные процедуры, которые не нужны в стеке (LIKE, через запятую), например N'sp[_]log[_]%'
     @exclude_logins логины, чьи параметризованные запросы не относятся к процедурам (через запятую):
                     логин инжестора OpenMetadata, BI, приложения. Их запросы приходят тем же событием
                     и вытесняют из буфера события процедур
     @max_events     предел событий в буфере. SQL Server отдаёт XML буфера с обрезкой (около 2300 событий):
                     при пределе 2000 выдача полная, буфер сам вытесняет старые события. Агент должен
                     запускаться чаще, чем накапливается 2000 событий (иначе ранние события не будут прочитаны)
     @max_memory_kb  память буфера
   ===================================================================== */
USE master;
GO
DECLARE @databases      nvarchar(max) = N'DWH_BCS';
DECLARE @exclude_like   nvarchar(max) = N'';
DECLARE @exclude_logins nvarchar(max) = N'openmetadata_user';
DECLARE @max_events     int           = 2000;
DECLARE @max_memory_kb  int           = 16384;

DECLARE @db_filter nvarchar(max) = N'', @proc_filter nvarchar(max) = N'', @login_filter nvarchar(max) = N'',
        @sql nvarchar(max);

SELECT @login_filter = STRING_AGG(CAST(N' AND NOT sqlserver.equal_i_sql_unicode_string(sqlserver.username, N'''
                                       + REPLACE(TRIM(value), N'''', N'''''') + N''')' AS nvarchar(max)), N'')
FROM STRING_SPLIT(@exclude_logins, N',') WHERE TRIM(value) <> N'';

SELECT @db_filter = STRING_AGG(CAST(N'sqlserver.database_name = N''' + REPLACE(TRIM(value), N'''', N'''''') + N''''
                                    AS nvarchar(max)), N' OR ')
FROM STRING_SPLIT(@databases, N',') WHERE TRIM(value) <> N'';

SELECT @proc_filter = STRING_AGG(CAST(N'NOT sqlserver.like_i_sql_unicode_string(object_name, N'''
                                      + REPLACE(TRIM(value), N'''', N'''''') + N''')' AS nvarchar(max)), N' AND ')
FROM STRING_SPLIT(@exclude_like, N',') WHERE TRIM(value) <> N'';

DECLARE @module_where nvarchar(max) =
    CASE WHEN @db_filter <> N'' OR @proc_filter <> N'' THEN N' WHERE ' END
    + CASE WHEN @db_filter <> N'' THEN N'(' + @db_filter + N')' ELSE N'' END
    + CASE WHEN @db_filter <> N'' AND @proc_filter <> N'' THEN N' AND ' ELSE N'' END
    + ISNULL(NULLIF(@proc_filter, N''), N'');
DECLARE @stmt_where nvarchar(max) = N' WHERE '
    + CASE WHEN @db_filter <> N'' THEN N'(' + @db_filter + N') AND ' ELSE N'' END
    + N'(object_type = 20816 OR object_type = 20801)' + ISNULL(@login_filter, N'');

IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'om_lineage')
    DROP EVENT SESSION om_lineage ON SERVER;

SET @sql = N'
CREATE EVENT SESSION om_lineage ON SERVER
ADD EVENT sqlserver.module_start (
    ACTION (sqlserver.session_id, sqlserver.database_name, package0.event_sequence)' + ISNULL(@module_where, N'') + N'),
ADD EVENT sqlserver.module_end (
    ACTION (sqlserver.session_id, sqlserver.database_name, package0.event_sequence)' + ISNULL(@module_where, N'') + N'),
ADD EVENT sqlserver.sp_statement_completed (
    SET collect_statement = (0), collect_object_name = (0)
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.query_hash, package0.event_sequence)'
    + @stmt_where + N')
ADD TARGET package0.ring_buffer (SET max_memory = ' + CAST(@max_memory_kb AS nvarchar(20)) + N', max_events_limit = ' + CAST(@max_events AS nvarchar(20)) + N')
WITH (EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 5 SECONDS,
      TRACK_CAUSALITY = OFF, STARTUP_STATE = ON);';
EXEC sys.sp_executesql @sql;
ALTER EVENT SESSION om_lineage ON SERVER STATE = START;
GO
SELECT s.name, s.create_time, t.target_name
FROM sys.dm_xe_sessions AS s
JOIN sys.dm_xe_session_targets AS t ON t.event_session_address = s.address
WHERE s.name = N'om_lineage';
GO
