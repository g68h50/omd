# Вывод на прод: расширенный lineage процедур MSSQL

Скрипты развёртывания решения из `lineage_mssql/` (описание и результаты — `lineage_mssql/PLAN.md`):
коннектор OpenMetadata `CustomDatabase` + сессия Extended Events + штатный агент `ms_sql` только для представлений.

Экспериментальные скрипты в `lineage_mssql/experiments/` завязаны на тестовый стенд (адрес API, имена сервисов,
контейнеры docker). Здесь — их версии для прода: параметры вынесены в конфигурацию, у каждого изменения есть откат.

## Шаги развёртывания (из PLAN.md, раздел 6)

| Шаг | Что | Где выполняется |
|---|---|---|
| 1 | Проверка Query Store (`READ_WRITE`, захват `ALL`) | SQL Server, DBA |
| 2 | Сессия Extended Events `om_lineage` | SQL Server, DBA (sa) |
| 3 | Права логина OpenMetadata: `VIEW SERVER STATE`, `VIEW DEFINITION` | SQL Server, DBA |
| 4 | Пакет коннектора `mssql_lineage_ext` в окружение агента OpenMetadata | сервер ingestion |
| 5 | Сервис `CustomDatabase` и агент с расписанием | API OpenMetadata |
| 6 | Штатный агент lineage: только представления; удаление ложных петель | API OpenMetadata |
| 7 | Проверка после развёртывания | API OpenMetadata |
| — | Откат каждого шага | |

## Содержимое

| Файл | Назначение |
|---|---|
| `01_deploy_replica.py` | накат реплики кода (`<база>/<схема>/<категория>/*.sql`) на тестовый SQL Server |
| `02_fill_test_data.py` | синтетические данные во все таблицы; значения и связи выводятся из кода процедур |
| `03_run_procedures.py` | запуск всех процедур для Query Store: граф вызовов, конфиги, джобы, метрика покрытия |
| `ingestor/mssql_lineage_ext.py` | дополнительный инжестор lineage — сервис CustomDatabase OpenMetadata |
| `ingestor/om_lineage_xe.sql` | сессия Extended Events для привязки динамического SQL к процедуре |
| `ingestor/DEPLOY.md` | развёртывание инжестора |
