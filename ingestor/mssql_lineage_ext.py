"""mssql_lineage_ext — дополнительный инжестор lineage процедур SQL Server для OpenMetadata.

Закрывает то, что не покрывает штатный инжестор lineage MSSQL: временные таблицы (#t, ##t), табличные
переменные (@t), динамический SQL (sp_executesql, EXEC(@sql)), MERGE с CTE, CROSS/OUTER APPLY, скалярные
подзапросы, INSERT ... EXEC, SELECT ... INTO, ложные петли от NOT EXISTS против цели.

Работает как сервис OpenMetadata типа CustomDatabase и запускается штатным агентом по расписанию из UI:

    sourcePythonClass: mssql_lineage_ext.MssqlLineageExtSource

Источники данных — только механизмы БД и OpenMetadata, без файлов:
  история     Query Store каждой базы (через подключение целевого сервиса OpenMetadata, свой пароль не хранится);
  привязка    динамический SQL -> процедура: метка /* lineage:proc=<имя> */ внутри оператора, иначе сессия
              Extended Events в памяти сервера (стек module_start/module_end, сопоставление по query_hash);
  разбор      штатный LineageParser OpenMetadata (SqlFluff, запасной SqlGlot) после предобработки T-SQL;
  сшивка      граф временных объектов отдельно на каждую процедуру, INSERT ... EXEC — через вызванную процедуру;
  запись      рёбра table -> table с процедурой (lineageDetails.pipeline) и SQL через штатный sink metadata-rest.

Параметры сервиса (connectionOptions):
  targetService   сервис MSSQL в OpenMetadata, в каталог которого пишутся рёбра            (ms_sql)
  databases       базы через запятую; пусто — все пользовательские базы с Query Store      ("")
  xeSession       сессия Extended Events для привязки динамического SQL; пусто — только метки (om_lineage)
  queryLogDays    глубина истории Query Store, дней                                        (3)

Развёртывание — DEPLOY.md рядом с этим файлом.
"""

from __future__ import annotations

import re
import traceback
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Optional

from sqlalchemy import text

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.storedProcedure import StoredProcedure
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    StatusType,
    TestConnectionResult,
    TestConnectionStepResult,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.metadataIngestion.parserconfig.queryParserConfig import QueryParserType
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityLineage import Source as LineageSourceType
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either, StackTraceError
from metadata.ingestion.api.steps import Source
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection as om_get_connection
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

EDGE_MARK = "mssql_lineage_ext"
DEFAULTS = {"targetService": "ms_sql", "databases": "", "xeSession": "om_lineage", "queryLogDays": "3"}


# =========================================================================== история и привязка

QS_STATEMENTS = """
SELECT ISNULL(OBJECT_SCHEMA_NAME(q.object_id), '') AS proc_schema,
       ISNULL(OBJECT_NAME(q.object_id), '')        AS proc_name,
       CONVERT(varchar(20), q.query_hash, 1)       AS query_hash,
       qt.query_sql_text                           AS query_text
FROM sys.query_store_query AS q
JOIN sys.query_store_query_text AS qt ON qt.query_text_id = q.query_text_id
JOIN sys.query_store_plan AS p ON p.query_id = q.query_id
JOIN sys.query_store_runtime_stats AS rs ON rs.plan_id = p.plan_id
WHERE rs.last_execution_time > DATEADD(DAY, -:days, SYSUTCDATETIME())
GROUP BY q.object_id, q.query_hash, qt.query_sql_text
"""
QS_DATABASES = """
SELECT name FROM sys.databases
WHERE database_id > 4 AND state = 0 AND is_query_store_on = 1 AND HAS_DBACCESS(name) = 1
ORDER BY name
"""
XE_RING = """
SELECT CAST(t.target_data AS nvarchar(max))
FROM sys.dm_xe_sessions AS s
JOIN sys.dm_xe_session_targets AS t ON t.event_session_address = s.address
WHERE s.name = :session AND t.target_name = N'ring_buffer'
"""
PROCEDURES = "SELECT SCHEMA_NAME(schema_id), name FROM sys.procedures"


@dataclass
class Statement:
    procedure: str              # schema.name; "" — не привязан
    text: str
    attribution: str            # proc | marker | xe | none


def read_query_store(conn, days: int) -> list[dict]:
    return [dict(r._mapping) for r in conn.execute(text(QS_STATEMENTS), {"days": days})]


def read_xe_mapping(conn, session: str) -> dict[tuple[str, str], str]:
    """(база, query_hash в десятичном виде) -> процедура, по стеку module_start/module_end каждой сессии.
    Если в сессии нет действия database_name, база в ключе пустая."""
    row = conn.execute(text(XE_RING), {"session": session}).fetchone()
    if not row or not row[0]:
        return {}
    root = ET.fromstring(row[0])

    def fld(ev, name):
        for d in ev.findall("data") + ev.findall("action"):
            if d.get("name") == name:
                return d.findtext("text") or d.findtext("value")
        return None

    events = sorted(root.findall("event"), key=lambda e: int(fld(e, "event_sequence") or 0))
    stack, mapping = defaultdict(list), {}
    for ev in events:
        sess, name = fld(ev, "session_id"), ev.get("name")
        if name == "module_start":
            stack[sess].append(fld(ev, "object_name"))
        elif name == "module_end":
            st, obj = stack[sess], fld(ev, "object_name")
            if obj in st:
                del st[len(st) - 1 - st[::-1].index(obj):]
        elif name == "sp_statement_completed":
            qh = fld(ev, "query_hash")
            if qh and qh != "0" and stack[sess]:
                mapping[((fld(ev, "database_name") or "").lower(), qh)] = stack[sess][-1]
    return mapping


MARKER = re.compile(r"/\*\s*lineage:proc=([\w.\[\]]+)\s*\*/", re.IGNORECASE)


def attribute(rows: list[dict], xe_map: dict, db: str, proc_schemas: dict[str, list[str]]) -> list[Statement]:
    """Привязка операторов к процедуре: Query Store (object_id) -> метка в тексте -> Extended Events."""

    def qualify(name: str) -> str:
        parts = [p.strip("[]") for p in name.split(".")]
        if len(parts) >= 2:
            return f"{parts[-2]}.{parts[-1]}"
        schemas = proc_schemas.get(parts[-1].lower(), [])
        return f"{schemas[0] if schemas else 'dbo'}.{parts[-1]}"

    out = []
    for r in rows:
        if r["proc_name"]:
            out.append(Statement(f"{r['proc_schema']}.{r['proc_name']}", r["query_text"], "proc"))
            continue
        marker = MARKER.search(r["query_text"])
        hash_dec = str(int(r["query_hash"], 16)) if r["query_hash"] else ""
        proc = xe_map.get((db.lower(), hash_dec)) or xe_map.get(("", hash_dec))
        if marker:
            out.append(Statement(qualify(marker.group(1)), r["query_text"], "marker"))
        elif proc:
            out.append(Statement(qualify(proc), r["query_text"], "xe"))
        else:
            out.append(Statement("", r["query_text"], "none"))
    return out


# =========================================================================== предобработка T-SQL

def _balanced(s: str, start: int) -> int:
    """Индекс закрывающей скобки для s[start] == '('."""
    depth = 0
    for i in range(start, len(s)):
        depth += s[i] == "("
        depth -= s[i] == ")"
        if depth == 0:
            return i
    return len(s) - 1


def strip_comments(sql: str) -> str:
    s = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", s)


def strip_params(sql: str) -> str:
    """Без комментариев и без префикса параметров (@p int, ...) параметризованного запроса."""
    s = strip_comments(sql).strip()
    if s.startswith("(@"):
        s = s[_balanced(s, 0) + 1:]
    return s


def apply_to_join(sql: str) -> str:
    """CROSS APPLY -> CROSS JOIN, OUTER APPLY (...) a -> LEFT JOIN (...) a ON 1 = 1: источники внутри APPLY
    штатный парсер иначе теряет."""
    s = re.sub(r"\bCROSS\s+APPLY\b", "CROSS JOIN", sql, flags=re.IGNORECASE)
    out, i = [], 0
    for m in re.finditer(r"\bOUTER\s+APPLY\s*\(", s, flags=re.IGNORECASE):
        if m.start() < i:
            continue
        open_idx = m.end() - 1
        close_idx = _balanced(s, open_idx)
        alias = re.match(r"\s*(?:AS\s+)?(\w+)", s[close_idx + 1:])
        end = close_idx + 1 + (alias.end() if alias else 0)
        out.append(s[i:m.start()] + "LEFT JOIN " + s[open_idx:end] + " ON 1 = 1")
        i = end
    out.append(s[i:])
    return "".join(out)


def strip_output_into(sql: str) -> str:
    """OUTPUT ... INTO @var парсер принимает за цель."""
    return re.sub(r"\bOUTPUT\b(?:(?!\bOUTPUT\b|\bSELECT\b).)*?\bINTO\s+[@#]?[\w.\[\]]+\s*(\([^()]*\))?",
                  " ", sql, flags=re.IGNORECASE | re.DOTALL)


def merge_to_insert(sql: str) -> str:
    """MERGE target USING src ... -> INSERT INTO target SELECT * FROM src (CTE-префикс сохраняется):
    у MERGE штатный парсер цель не находит."""
    m = re.search(r"\bMERGE\s+(?:INTO\s+)?([\w.\[\]#@]+)(?:\s+(?:AS\s+)?\w+)?\s+USING\s+", sql, flags=re.IGNORECASE)
    if not m:
        return sql
    prefix, target, rest_idx = sql[:m.start()], m.group(1), m.end()
    if sql[rest_idx:rest_idx + 1] == "(":
        source = sql[rest_idx:_balanced(sql, rest_idx) + 1]
    else:
        source = re.match(r"[\w.\[\]#@]+", sql[rest_idx:]).group(0)
    return f"{prefix} INSERT INTO {target} SELECT * FROM {source} AS merge_src"


def drop_self_not_exists(sql: str, target: str | None) -> str:
    """NOT EXISTS (...) против самой цели — проверка, а не источник данных (иначе ложная петля)."""
    if not target:
        return sql
    t = norm(target)
    out, i = [], 0
    for m in re.finditer(r"\bNOT\s+EXISTS\s*\(", sql, flags=re.IGNORECASE):
        if m.start() < i:
            continue
        open_idx = m.end() - 1
        close_idx = _balanced(sql, open_idx)
        body = sql[open_idx:close_idx + 1].lower()
        names = {norm(n) for n in re.findall(r"\bfrom\s+([\w.\[\]#]+)", body)}
        if names == {t}:
            out.append(sql[i:m.start()] + "1 = 1")
            i = close_idx + 1
    out.append(sql[i:])
    return "".join(out)


INSERT_EXEC = re.compile(r"\bINSERT\s+(?:INTO\s+)?([#@\w.\[\]]+)\s*(?:\([^()]*\))?\s*EXEC(?:UTE)?\s+([\w.\[\]]+)",
                         re.IGNORECASE)
TARGET_HINT = re.compile(r"^\s*(?:WITH\b.*?\)\s*)?(?:INSERT\s+(?:INTO\s+)?|UPDATE\s+|MERGE\s+(?:INTO\s+)?)([#@\w.\[\]]+)",
                         re.IGNORECASE | re.DOTALL)


def preprocess(sql: str) -> str:
    s = strip_params(sql)
    s = re.sub(r"\bOPTION\s*\([^)]*\)", " ", s, flags=re.IGNORECASE)
    s = merge_to_insert(s)
    s = strip_output_into(s)
    s = apply_to_join(s)
    hint = TARGET_HINT.search(s)
    return drop_self_not_exists(s, hint.group(1) if hint else None)


# =========================================================================== разбор и граф

def norm(name: str) -> str:
    """Имя объекта без базы и схемы, в нижнем регистре (схема восстанавливается при записи)."""
    return str(name).strip().strip("`\"").split(".")[-1].strip("[]").lower()


def is_temp(name: str) -> bool:
    return name.startswith(("#", "@"))


@dataclass
class ProcLineage:
    edges: dict = field(default_factory=lambda: defaultdict(set))       # цель -> источники (в т.ч. temp)
    exec_calls: dict = field(default_factory=lambda: defaultdict(set))  # temp/цель -> вызванные процедуры
    result_sources: set = field(default_factory=set)                    # источники итоговых SELECT (INSERT ... EXEC)
    sql: dict = field(default_factory=dict)                             # цель -> пример оператора
    texts: list = field(default_factory=list)                           # тексты операторов (для схем таблиц)


RESULT_START = re.compile(r"^\s*(?:WITH|SELECT)\b(?!\s+@)", re.IGNORECASE)
WRITES = re.compile(r"\b(?:INSERT|UPDATE|MERGE|DELETE|INTO)\b", re.IGNORECASE)


def is_result_select(sql: str) -> bool:
    """Итоговый набор строк процедуры: SELECT/WITH без записи и без присваивания переменным."""
    return bool(RESULT_START.match(sql)) and not WRITES.search(sql) and not re.search(r"\bSELECT\s+@", sql, re.IGNORECASE)


ALIAS = re.compile(r"\b(?:FROM|JOIN)\s+([#@\w.\[\]]+)\s+(?:AS\s+)?(\w+)", re.IGNORECASE)
RESERVED = {"on", "where", "join", "left", "right", "inner", "cross", "outer", "group", "order", "with", "set"}


def _fix_names(names: set[str], sql: str) -> set[str]:
    """Вернуть префикс # / @ временным объектам (SqlGlot его теряет) и раскрыть алиасы (UPDATE p ... FROM #t AS p)."""
    temps = {m.lower() for m in re.findall(r"[#@]{1,2}\w+", sql)}
    by_bare = {t.lstrip("#@"): t for t in temps}
    aliases = {}
    for tbl, alias in ALIAS.findall(sql):
        if alias.lower() not in RESERVED:
            aliases[alias.lower()] = norm(tbl)
    fixed = set()
    for n in names:
        n = aliases.get(n, n) if n not in by_bare.values() else n
        if not n.startswith(("#", "@")) and n in by_bare:
            n = by_bare[n]
        fixed.add(n)
    return fixed


def parse_tables(sql: str, parser_factory) -> tuple[set[str], set[str]]:
    """Источники и цели через LineageParser OpenMetadata (SqlFluff, запасной SqlGlot)."""
    for parser_type in ("SqlFluff", "SqlGlot"):
        try:
            p = parser_factory(sql, parser_type)
            src = _fix_names({norm(t) for t in p.source_tables}, sql)
            tgt = _fix_names({norm(t) for t in p.target_tables}, sql)
            if tgt:
                return src, tgt
        except Exception:  # noqa: BLE001,S112
            continue
    return set(), set()


FROM_JOIN = re.compile(r"\b(?:FROM|JOIN)\s+([#@\w.\[\]]+)", re.IGNORECASE)
CTE_NAME = re.compile(r"(?:\bWITH\b|,)\s*(\w+)\s+AS\s*\(", re.IGNORECASE)


def referenced_tables(sql: str) -> set[str]:
    """Все таблицы из FROM/JOIN, включая скалярные подзапросы в списке SELECT (их штатный парсер пропускает)."""
    ctes = {c.lower() for c in CTE_NAME.findall(sql)}
    return {norm(n) for n in FROM_JOIN.findall(sql) if norm(n) not in ctes and not norm(n).startswith("sys")}


def self_reference_is_data(sql: str, target: str) -> bool:
    """Самоссылка на цель — данные, если цель читается в основном FROM или подзапрос с ней попадает в SELECT.
    Проверка изменений LEFT JOIN (SELECT TOP 1 t.hashdiff FROM <цель> ...) AS cur ... данными не считается."""
    lookups, i = [], 0
    for m in re.finditer(r"\bJOIN\s*\(", sql, flags=re.IGNORECASE):
        if m.start() < i:
            continue
        open_idx = m.end() - 1
        close_idx = _balanced(sql, open_idx)
        alias = re.match(r"\s*(?:AS\s+)?(\w+)", sql[close_idx + 1:])
        if alias and target in referenced_tables(sql[open_idx:close_idx + 1]):
            lookups.append((open_idx, close_idx, alias.group(1)))
        i = close_idx + 1
    for m in FROM_JOIN.finditer(sql):
        if norm(m.group(1)) == target and not any(a <= m.start() <= b for a, b, _ in lookups):
            return True
    for _, _, alias in lookups:
        if re.search(r"\bSELECT\b(?:(?!\bFROM\b).)*?\b" + re.escape(alias) + r"\.", sql, flags=re.IGNORECASE | re.DOTALL):
            return True
    return False


def build(statements: list[Statement], parser_factory) -> dict[str, ProcLineage]:
    procs: dict[str, ProcLineage] = defaultdict(ProcLineage)
    for st in statements:
        body = strip_params(st.text)
        pl = procs[st.procedure]
        pl.texts.append(body)
        call = INSERT_EXEC.search(body)
        if call:
            pl.exec_calls[norm(call.group(1))].add(norm(call.group(2)))
            continue
        if st.procedure and is_result_select(body) and re.search(r"\bFROM\b", body, flags=re.IGNORECASE):
            pl.result_sources |= _fix_names(referenced_tables(apply_to_join(body)), body)
            continue
        if not re.search(r"\b(INSERT|UPDATE|MERGE|INTO)\b", body, flags=re.IGNORECASE):
            continue
        pre = preprocess(st.text)
        src, tgt = parse_tables(pre, parser_factory)
        src = {s for s in src if s and " " not in s}
        extra = _fix_names(referenced_tables(pre), pre)          # скалярные подзапросы, которые парсер пропустил
        is_update = re.match(r"\s*UPDATE\b", pre, flags=re.IGNORECASE) is not None
        for t in tgt:
            if " " in t:
                continue
            found = (src | extra) - {t}
            # UPDATE цели «на месте» — не источник; иначе самоссылка — данные только по self_reference_is_data
            if t in (src | extra) and not is_temp(t) and not is_update and self_reference_is_data(pre, t):
                found.add(t)
            pl.edges[t] |= found
            pl.sql.setdefault(t, st.text)
    return procs


def resolve(procs: dict[str, ProcLineage]) -> list[tuple[str, str, str, str]]:
    """Рёбра между постоянными таблицами: (источник, цель, процедура, sql) со сшивкой через temp и INSERT ... EXEC."""
    proc_sources: dict[str, set[str]] = {}

    def ultimate(pl: ProcLineage, node: str, seen: set[str]) -> set[str]:
        if node in seen:
            return set()
        seen = seen | {node}
        result = set()
        for src in pl.edges.get(node, set()):
            result |= ultimate(pl, src, seen) if is_temp(src) else {src}
        for called in pl.exec_calls.get(node, set()):
            result |= proc_sources.get(called, set())
        return result

    for _ in range(2):          # источники каждой процедуры (для INSERT ... EXEC), затем рёбра
        for name, pl in procs.items():
            srcs = set()
            for tgt in pl.edges:
                srcs |= ultimate(pl, tgt, set())
            for node in pl.result_sources:
                srcs |= ultimate(pl, node, set()) if is_temp(node) else {node}
            proc_sources[name.split(".")[-1].lower()] = {s for s in srcs if not is_temp(s)}

    out = []
    for name, pl in procs.items():
        for tgt in list(pl.edges) + [t for t in pl.exec_calls if t not in pl.edges]:
            if is_temp(tgt):
                continue
            for src in ultimate(pl, tgt, set()):
                out.append((src, tgt, name, pl.sql.get(tgt, "")))
    return out


QUALIFIED = re.compile(r"(?:\[?(\w+)\]?\s*\.\s*)?\[?(\w+)\]?\s*\.\s*\[?(\w+)\]?")


def schema_hints(pl: ProcLineage, schemas: set[str]) -> dict[str, str]:
    """Схема таблицы по тексту операторов процедуры: DWH.fact_x / [DWH].[fact_x] / db.DWH.fact_x."""
    hints = {}
    for body in pl.texts:
        for _, schema, name in QUALIFIED.findall(body):
            if schema.lower() in schemas:
                hints.setdefault(name.lower(), schema.lower())
    return hints


# =========================================================================== OpenMetadata

def _options(connection) -> dict:
    raw = getattr(connection, "connectionOptions", None)
    opts = dict(DEFAULTS)
    if raw is not None:
        opts.update({k: str(v) for k, v in (raw.root if hasattr(raw, "root") else raw).items()})
    return opts


def _target_engine(metadata: OpenMetadata, opts: dict):
    service = metadata.get_by_name(entity=DatabaseService, fqn=opts["targetService"], fields=["connection"])
    if service is None or service.connection is None:
        raise RuntimeError(f"Целевой сервис {opts['targetService']} не найден или без подключения")
    return om_get_connection(service.connection.config)


def _databases(conn, opts: dict) -> list[str]:
    listed = [d.strip() for d in opts.get("databases", "").split(",") if d.strip()]
    return listed or [r[0] for r in conn.execute(text(QS_DATABASES))]


def get_connection(connection):
    """Клиент коннектора — его настройки; подключение к БД берётся у целевого сервиса при запуске."""
    return _options(connection)


def test_connection(metadata: OpenMetadata, client, service_connection, *args, **kwargs) -> TestConnectionResult:
    """Проверка подключения из UI: целевой сервис, Query Store в базах, сессия Extended Events."""
    opts = client if isinstance(client, dict) else _options(service_connection)
    steps, state = [], {}

    def step(name: str, mandatory: bool, fn) -> None:
        try:
            fn()
            steps.append(TestConnectionStepResult(name=name, mandatory=mandatory, passed=True))
        except Exception as exc:  # noqa: BLE001
            steps.append(TestConnectionStepResult(name=name, mandatory=mandatory, passed=False,
                                                  message=str(exc)[:500], errorLog=traceback.format_exc()[-2000:]))

    def query_store():
        with state["engine"].connect() as conn:
            dbs = _databases(conn, opts)
            if not dbs:
                raise RuntimeError("Нет баз с включённым Query Store")
            for db in dbs:
                conn.execute(text(f"USE [{db}]"))
                st = conn.execute(text("SELECT actual_state_desc FROM sys.database_query_store_options")).scalar()
                if st not in ("READ_WRITE", "READ_ONLY"):
                    raise RuntimeError(f"{db}: Query Store {st}")

    def xe_session():
        if not opts.get("xeSession"):
            return
        with state["engine"].connect() as conn:
            if conn.execute(text("SELECT 1 FROM sys.dm_xe_sessions WHERE name = :n"), {"n": opts["xeSession"]}).scalar() != 1:
                raise RuntimeError(f"Сессия Extended Events {opts['xeSession']} не запущена")

    step("TargetService", True, lambda: state.setdefault("engine", _target_engine(metadata, opts)))
    step("QueryStore", True, query_store)
    step("ExtendedEvents", False, xe_session)
    failed = any(s.mandatory and not s.passed for s in steps)
    return TestConnectionResult(steps=steps, status=StatusType.Failed if failed else StatusType.Successful)


class MssqlLineageExtSource(Source):
    """Query Store + привязка динамического SQL + предобработка + LineageParser + граф temp на процедуру."""

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.service_connection = config.serviceConnection.root.config
        self.opts = _options(self.service_connection)
        self.connection_obj = self.opts

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None):  # noqa: UP045
        return cls(WorkflowSource.model_validate(config_dict), metadata)

    def prepare(self):
        pass

    def test_connection(self) -> None:
        result = test_connection(self.metadata, self.opts, self.service_connection)
        for s in result.steps:
            logger.info(f"{s.name}: {'OK' if s.passed else 'FAILED'} {s.message or ''}")
        if result.status == StatusType.Failed:
            raise RuntimeError("Проверка подключения не пройдена")

    def _table_index(self, db: str) -> dict[str, dict[str, Table]]:
        """Таблицы и представления базы из каталога OpenMetadata: имя -> {схема: сущность}."""
        index: dict[str, dict[str, Table]] = defaultdict(dict)
        for t in self.metadata.list_all_entities(entity=Table, params={"database": f"{self.opts['targetService']}.{db}"}):
            schema = t.databaseSchema.name if t.databaseSchema else ""
            index[t.name.root.lower() if hasattr(t.name, "root") else str(t.name).lower()][schema.lower()] = t
        return index

    def _iter(self) -> Iterable[Either]:
        service = self.opts["targetService"]
        engine = _target_engine(self.metadata, self.opts)

        def parser_factory(sql, parser_type):
            return LineageParser(sql, dialect=Dialect.TSQL, timeout_seconds=30, parser_type=QueryParserType(parser_type))

        with engine.connect() as conn:
            databases = _databases(conn, self.opts)
            xe_map = {}
            if self.opts.get("xeSession"):
                try:
                    xe_map = read_xe_mapping(conn, self.opts["xeSession"])
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Extended Events недоступны, привязка динамического SQL только по меткам: {exc}")
            per_db = {}
            for db in databases:
                try:
                    conn.execute(text(f"USE [{db}]"))
                    rows = read_query_store(conn, days=int(self.opts["queryLogDays"]))
                    proc_schemas = defaultdict(list)
                    for schema, name in conn.execute(text(PROCEDURES)):
                        proc_schemas[name.lower()].append(schema)
                    per_db[db] = (rows, proc_schemas)
                except Exception as exc:  # noqa: BLE001
                    yield Either(left=StackTraceError(name=f"{db}: Query Store", error=str(exc),
                                                      stackTrace=traceback.format_exc()))
        logger.info(f"Базы: {', '.join(databases)}; событий привязки Extended Events: {len(xe_map)}")

        for db, (rows, proc_schemas) in per_db.items():
            statements = attribute(rows, xe_map, db, proc_schemas)
            logger.info(f"[{db}] операторов Query Store {len(rows)}; привязка: "
                        f"{dict(Counter(s.attribution for s in statements))}")
            procs = build(statements, parser_factory)
            edges = resolve(procs)
            tables = self._table_index(db)
            all_schemas = {s for by_schema in tables.values() for s in by_schema}
            hints = {name: schema_hints(pl, all_schemas) for name, pl in procs.items()}
            proc_cache, sent, missing = {}, set(), Counter()

            def table(name: str, proc: str) -> Table | None:
                candidates = tables.get(name, {})
                if not candidates:
                    return None
                proc_schema = proc.split(".")[0].lower() if "." in proc else ""
                for schema in (hints.get(proc, {}).get(name), proc_schema, "dbo"):
                    if schema and schema in candidates:
                        return candidates[schema]
                return next(iter(candidates.values())) if len(candidates) == 1 else None

            def procedure(proc: str):
                if proc and proc not in proc_cache:
                    proc_cache[proc] = self.metadata.get_by_name(entity=StoredProcedure, fqn=f"{service}.{db}.{proc}")
                return proc_cache.get(proc)

            for src, tgt, proc, sql in edges:
                src_t, tgt_t = table(src, proc), table(tgt, proc)
                if src_t is None or tgt_t is None:
                    missing[src if src_t is None else tgt] += 1
                    continue
                key = (src_t.id.root, tgt_t.id.root)
                if key in sent:
                    continue
                proc_e = procedure(proc)
                try:
                    yield Either(right=AddLineageRequest(edge=EntitiesEdge(
                        fromEntity=EntityReference(id=src_t.id, type="table"),
                        toEntity=EntityReference(id=tgt_t.id, type="table"),
                        lineageDetails=LineageDetails(
                            sqlQuery=strip_params(sql)[:10000] if sql else None,
                            source=LineageSourceType.QueryLineage,
                            pipeline=EntityReference(id=proc_e.id, type="storedProcedure") if proc_e else None,
                            description=f"{EDGE_MARK}: {proc or 'без процедуры'}",
                        ),
                    )))
                    sent.add(key)
                except Exception as exc:  # noqa: BLE001
                    yield Either(left=StackTraceError(name=f"{db}: {src}->{tgt}", error=str(exc),
                                                      stackTrace=traceback.format_exc()))
            logger.info(f"[{db}] рёбер отправлено: {len(sent)}; не найдено в каталоге (или неоднозначно): {dict(missing)}")

    def close(self):
        pass
