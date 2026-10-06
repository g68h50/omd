# Дополнительный инжестор lineage MSSQL: развёртывание

`mssql_lineage_ext.py` — сервис OpenMetadata типа **CustomDatabase**. Его по расписанию запускает штатный агент. Он строит lineage процедур SQL Server там, где штатный инжестор его теряет:

- временные таблицы и табличные переменные;
- динамический SQL;
- `MERGE`, `CROSS/OUTER APPLY`;
- `INSERT … EXEC`, `SELECT … INTO`.

Рёбра пишутся в каталог основного сервиса MSSQL, вместе с процедурой и SQL.

Проверка на стенде (DWH_BCS, 102 эталонных ребра): полнота 1,0, точность 1,0, все рёбра привязаны к процедуре, запуск около 11 с. У штатного инжестора — 0,578 и 0,894. Как выбирали решение — `lineage_mssql/PLAN.md`.

| Файл | Назначение |
|---|---|
| `mssql_lineage_ext.py` | инжестор — один модуль, без зависимостей кроме пакета `openmetadata-ingestion` |
| `om_lineage_xe.sql` | сессия Extended Events для привязки динамического SQL к процедуре |
| `DEPLOY.md` | эта инструкция |

## Как работает

```
SQL Server                                      OpenMetadata
Query Store каждой базы ─┐                      сервис ms_sql_lineage_ext (CustomDatabase)
Extended Events om_lineage ─┼─> MssqlLineageExtSource ─> sink metadata-rest ─> lineage сервиса ms_sql
(буфер в памяти)            │    подключение берёт у сервиса ms_sql (свой пароль не хранит)
```

1. Из Query Store читаются операторы за `queryLogDays` дней. Оператор процедуры привязан к ней штатно, по `object_id`.
2. Динамический SQL привязывается к процедуре, которая его выполнила. Сначала ищется метка `/* lineage:proc=… */` в тексте оператора, иначе используется стек вызовов из Extended Events: сопоставление по `query_hash`.
3. T-SQL подготавливается для разбора:
   - `MERGE` → `INSERT`;
   - `APPLY` → `JOIN`;
   - убираются `OUTPUT … INTO` и проверки `NOT EXISTS` против самой цели.
4. Разбор выполняет штатный `LineageParser` OpenMetadata.
5. Временные объекты сшиваются отдельно внутри каждой процедуры, `INSERT … EXEC` — через вызванную процедуру.
6. Схема таблицы определяется по порядку: из текста оператора (`DWH.fact_x`), иначе схема процедуры, иначе единственная таблица с таким именем в базе по каталогу OpenMetadata.

Файлы не используются: данные читаются из БД и уходят в API OpenMetadata.

## Требования

| | |
|---|---|
| OpenMetadata | 2.0.2: проверено. Используются публичные классы SDK: `Source`, `LineageParser`, `get_connection`, `AddLineageRequest` |
| SQL Server | 2016 и выше (Query Store); проверено на 2019 |
| Основной сервис MSSQL в OpenMetadata | уже создан, метаданные загружены (таблицы и процедуры в каталоге) — далее `ms_sql` |
| Логин основного сервиса | права из шага 2 |
| Агент OpenMetadata | Airflow из поставки OpenMetadata или свой; модуль должен импортироваться в его окружении |

## Шаг 1. Query Store в базах

В каждой базе, для которой нужен lineage (выполняет DBA):

```sql
ALTER DATABASE [DWH_BCS] SET QUERY_STORE = ON
    (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL,
     DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60,
     CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30));
```

`QUERY_CAPTURE_MODE = ALL` обязателен: в режиме `AUTO` редкие операторы процедур не сохраняются. Глубина хранения (`STALE_QUERY_THRESHOLD_DAYS`) должна быть не меньше `queryLogDays` сервиса.

## Шаг 2. Права логина основного сервиса

Инжестор ходит в БД под логином сервиса `ms_sql` (на стенде — `openmetadata_user`):

```sql
USE master;
GRANT VIEW SERVER STATE TO [openmetadata_user];       -- сессия Extended Events: sys.dm_xe_*
-- в каждой базе:
USE [DWH_BCS];
GRANT VIEW DATABASE STATE TO [openmetadata_user];     -- представления Query Store
GRANT VIEW DEFINITION TO [openmetadata_user];         -- список процедур
```

## Шаг 3. Сессия Extended Events

Выполните `om_lineage_xe.sql` под логином с `ALTER ANY EVENT SESSION`. Параметры задаются в начале скрипта:

| Параметр | Значение | Зачем |
|---|---|---|
| `@databases` | базы через запятую | те же, что в параметре `databases` сервиса; меньше событий — дольше живёт буфер |
| `@exclude_like` | служебные процедуры, например `N'sp[_]log[_]%'` | процедуры журналирования вызываются постоянно и вытесняют полезные события |
| `@exclude_logins` | `N'openmetadata_user'` + логины BI и приложений | их параметризованные запросы приходят тем же событием и вытесняют события процедур |
| `@max_events` | `2000` | см. ниже |

Сессия стартует вместе с сервером (`STARTUP_STATE = ON`). События хранятся только в памяти.

**Почему 2000 событий.** SQL Server отдаёт содержимое буфера с обрезкой, примерно до 2300 событий. С пределом 2000 выдача всегда полная, а буфер сам вытесняет старые события. Агент должен запускаться чаще, чем накапливается 2000 событий. На стенде полный прогон 22 процедур — 1641 событие.

Сколько событий сейчас в буфере:

```sql
SELECT CAST(t.target_data AS xml).value('(RingBufferTarget/@totalEventsProcessed)[1]', 'bigint') AS events_total
FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_targets t ON t.event_session_address = s.address
WHERE s.name = N'om_lineage';
```

Без сессии инжестор тоже работает. Тогда динамический SQL привязывается только по меткам, а без меток полнота падает примерно до 0,9: теряются цепочки «динамический SQL → `#temp` → таблица».

## Шаг 4. Установка модуля в окружение агента

Модуль должен импортироваться как `mssql_lineage_ext` там, где выполняются задачи агента.

**Вариант А. Агент OpenMetadata в Docker** (контейнер `openmetadata_ingestion`). Каталог DAG-ов `/opt/airflow/dags` уже есть в `PYTHONPATH` и лежит на постоянном томе:

```bash
docker cp mssql_lineage_ext.py openmetadata_ingestion:/opt/airflow/dags/mssql_lineage_ext.py
docker exec -u root openmetadata_ingestion chown airflow:root /opt/airflow/dags/mssql_lineage_ext.py
docker exec -w /opt/airflow/dags openmetadata_ingestion python -c "import mssql_lineage_ext as m; print(m.MssqlLineageExtSource)"
```

**Вариант Б. Kubernetes или свой образ агента.** Положите файл в каталог из `PYTHONPATH` воркеров: в образе (`COPY mssql_lineage_ext.py /opt/airflow/dags/`) или через ConfigMap, смонтированный в каталог DAG-ов. При нескольких воркерах файл нужен на каждом.

Обновление — замена файла, перезапуск агента не нужен: модуль импортируется заново при каждом запуске.

## Шаг 5. Сервис CustomDatabase

**Через UI.** Settings → Services → Databases → Add New Service → **Custom Database**:

- Name: `ms_sql_lineage_ext`
- Source Python Class: `mssql_lineage_ext.MssqlLineageExtSource`
- Connection Options:

| Ключ | Значение | Описание |
|---|---|---|
| `targetService` | `ms_sql` | основной сервис MSSQL: у него берётся подключение и в его каталог пишутся рёбра |
| `databases` | `DWH_BCS` или пусто | пусто — все пользовательские базы с включённым Query Store |
| `xeSession` | `om_lineage` | пусто — привязка динамического SQL только по меткам |
| `queryLogDays` | `3` | глубина истории; должна покрывать интервал между запусками с запасом |

**Через API** (токен бота ingestion-bot — в переменной `OM_TOKEN`):

```bash
curl -X PUT "$OM/api/v1/services/databaseServices" -H "Authorization: Bearer $OM_TOKEN" -H "Content-Type: application/json" -d '{
  "name": "ms_sql_lineage_ext",
  "serviceType": "CustomDatabase",
  "description": "Расширенный lineage процедур ms_sql: временные таблицы, динамический SQL, MERGE, APPLY, INSERT ... EXEC",
  "connection": {"config": {
    "type": "CustomDatabase",
    "sourcePythonClass": "mssql_lineage_ext.MssqlLineageExtSource",
    "connectionOptions": {"targetService": "ms_sql", "databases": "DWH_BCS", "xeSession": "om_lineage", "queryLogDays": "3"}
  }}
}'
```

**Test Connection** проверяет три шага:
- `TargetService` — сервис найден, подключение получено;
- `QueryStore` — включён во всех базах;
- `ExtendedEvents` — сессия запущена; это необязательный шаг.

## Шаг 6. Агент по расписанию

UI: сервис `ms_sql_lineage_ext` → Agents → Add Metadata Agent → расписание, например каждый час `15 * * * *` → Deploy.

Через API:

```bash
SERVICE_ID=$(curl -s "$OM/api/v1/services/databaseServices/name/ms_sql_lineage_ext" -H "Authorization: Bearer $OM_TOKEN" | jq -r .id)
PIPELINE_ID=$(curl -s -X POST "$OM/api/v1/services/ingestionPipelines" -H "Authorization: Bearer $OM_TOKEN" -H "Content-Type: application/json" -d '{
  "name": "ms_sql_lineage_ext_daily", "pipelineType": "metadata",
  "service": {"id": "'"$SERVICE_ID"'", "type": "databaseService"},
  "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
  "airflowConfig": {"scheduleInterval": "15 * * * *"}}' | jq -r .id)
curl -X POST "$OM/api/v1/services/ingestionPipelines/deploy/$PIPELINE_ID" -H "Authorization: Bearer $OM_TOKEN"
curl -X POST "$OM/api/v1/services/ingestionPipelines/trigger/$PIPELINE_ID" -H "Authorization: Bearer $OM_TOKEN"   # первый запуск
```

Частота запуска должна укладываться в окно буфера Extended Events (шаг 3) и в `queryLogDays`.

## Шаг 7. Штатный агент lineage основного сервиса — только представления

Lineage процедур теперь строит дополнительный инжестор. Штатный агент lineage сервиса `ms_sql` оставьте только для представлений — по ним он полный:

| Параметр агента lineage `ms_sql` | Значение |
|---|---|
| `processViewLineage` | `true` |
| `processQueryLineage` | `false` |
| `processStoredProcedureLineage` | `false` |

Если штатный агент уже строил lineage процедур, удалите его ложные петли — рёбра таблицы в саму себя от проверок `NOT EXISTS`. Это `DELETE /api/v1/lineage/table/{id}/table/{id}`. На стенде их было 7: `hub_*`, `lnk_*`, `fact_transaction`. Настоящие самоссылки (процедура читает свою цель) инжестор создаст заново.

## Шаг 8. Проверка

1. **Статус агента.** Success, 0 ошибок.
2. **Лог агента.** По каждой базе — три строки:
   ```
   Базы: DWH_BCS; событий привязки Extended Events: 80
   [DWH_BCS] операторов Query Store 254; привязка: {'xe': 63, 'none': 67, 'proc': 124}
   [DWH_BCS] рёбер отправлено: 116; не найдено в каталоге (или неоднозначно): {}
   ```
   - `xe` и `marker` — динамический SQL, привязанный к процедуре;
   - `none` — не привязанные операторы: служебные запросы и ad-hoc вне процедур;
   - «не найдено в каталоге» — таблицы, которых нет в OpenMetadata: перезапустите metadata-агент `ms_sql`.
3. **Lineage таблицы в UI.** На ребре указана процедура (Pipeline), SQL и описание `mssql_lineage_ext: <процедура>`.

## Метка в динамическом SQL — для нового кода

Привязка через Extended Events автоматическая, но зависит от буфера в памяти: перезапуск сервера, всплеск событий. Метка надёжна всегда. Её нужно ставить **внутри** оператора, после `INSERT`/`DELETE`, потому что Query Store хранит текст оператора без комментариев перед ним:

```sql
SET @sql = N'INSERT /* lineage:proc=' + OBJECT_NAME(@@PROCID) + N' */ INTO DWH.target (...) SELECT ...';
EXEC sys.sp_executesql @sql;
```

## Эксплуатация и ограничения

| Ситуация | Что происходит | Что делать |
|---|---|---|
| Перезапуск SQL Server | буфер Extended Events очищается; рёбра, уже записанные в OpenMetadata, остаются | ничего; новые запуски процедур привяжутся заново |
| Динамического SQL между запусками агента больше 2000 событий | ранние события вытеснены, часть динамического SQL без процедуры | запускать агент чаще, сузить `@databases`, добавить `@exclude_like` / `@exclude_logins`, метки в коде |
| Новая база | — | добавить в `databases` сервиса и в `@databases` сессии, включить Query Store (шаг 1) |
| Таблица с одинаковым именем в нескольких схемах и без схемы в тексте | схема берётся по схеме процедуры, иначе ребро не пишется (счётчик «неоднозначно» в логе) | указывать схему в коде |
| Повторный запуск агента | рёбра записываются идемпотентно | — |
| Обновление OpenMetadata | могут измениться внутренние классы SDK | после обновления — Test Connection и ручной запуск агента |

Не проверено: кнопка Test Connection в UI. На стенде проверялся запуск агента: success, 0 ошибок.

## Откат

1. В штатном агенте lineage `ms_sql` вернуть `processQueryLineage` и `processStoredProcedureLineage` в `true`, переразвернуть агент и запустить.
2. Удалить агент и сервис `ms_sql_lineage_ext`: UI или `DELETE /api/v1/services/databaseServices/{id}?hardDelete=true&recursive=true`.
3. При необходимости удалить рёбра инжестора. Их описание начинается с `mssql_lineage_ext`; так помечены рёбра, которых не было до установки. Готовый сценарий — `lineage_mssql/experiments/e7_om_connector.py cleanup`.
4. Удалить сессию: `DROP EVENT SESSION om_lineage ON SERVER;`
5. Удалить файл `mssql_lineage_ext.py` из окружения агента.
