"""Запускает все процедуры баз реплики, чтобы их SQL попал в Query Store (источник lineage для OpenMetadata).

Порядок работы:

  1. Query Store в каждой базе включается в READ_WRITE с захватом ALL (если ещё не включён).
  2. Строится граф вызовов: статические EXEC и имена процедур внутри строк динамического SQL
     (EXEC DWH.sp_x в sp_executesql). Процедуры, которые никто не вызывает, — корневые.
  3. Корневые процедуры запускаются в порядке потока данных: сначала те, что пишут таблицы,
     потом те, что их читают (по sys.dm_sql_referenced_entities, с учётом вызываемых процедур).
  4. Имена процедур из конфигурационных таблиц (их читает процедура с динамическим SQL) — тоже вызовы.
  5. После каждого запуска отмечаются процедуры, реально выполненные внутри (sys.dm_exec_procedure_stats
     и Query Store). Так учитываются вызовы через конфигурационную таблицу, которые статически не видны:
     такие процедуры повторно не запускаются.
  6. Процедуры, до которых вызовы не дошли (конфиг пуст, ветка не выполнилась), запускаются напрямую —
     тоже в порядке потока данных. Упавшая процедура повторяется со следующим набором параметров.
  7. Джобы SQL Agent, созданные процедурами, сразу отключаются; их шаги T-SQL выполняются скриптом
     (их код тоже должен попасть в Query Store), после чего джобы удаляются (--keep-jobs — оставить отключёнными).
  8. Доисследование: процедуры с непокрытыми операторами записи перезапускаются с другими наборами параметров.
  9. Query Store сбрасывается на диск (sp_query_store_flush_db); выводится покрытие — доля операторов записи
     из кода процедур, попавших в Query Store (непокрытый оператор — ветка без lineage).

Конфигурационные таблицы с синтетическими данными (содержимое с прода недоступно): скрипт записывает в них
корневые процедуры и подбирает формат записи, который понимает читающая процедура (schema.name / [schema].[name] /
name, параметры в отдельной колонке или в той же, готовая команда EXEC ...), проверяя по факту вызова на пробной
процедуре; условия читающей процедуры (is_active = 1, enabled = 'Y') проставляются. --no-config-fill — выключить.

Параметры процедур: значения по умолчанию из заголовка процедуры не передаются; для остальных —
по порядку — литерал, с которым процедуру вызывают другие процедуры (EXEC ... @status = 'OK', в том числе
внутри строк динамического SQL); литерал, с которым параметр сравнивается в её коде (@mode = 'FULL');
частое значение колонки с тем же именем (сначала из таблиц, которые процедура читает или пишет).
Даты — период DATE_FROM/DATE_TO и значения одноимённых колонок, OUTPUT — переменные, табличные параметры —
несколько строк из справочника. Согласованные наборы: значения параметров из одной строки таблицы процедуры
и та же строка с новым идентификатором (MAX + 1, для номеров-дат — следующий день).

    python 03_run_procedures.py                                # базы из <корень проекта>/prod_repl
    python 03_run_procedures.py --databases DWH_BCS --dry-run  # план запуска и параметры без выполнения
    python 03_run_procedures.py --exclude "sp_purge|sp_archive" --report run.csv

Код возврата: 0 — все процедуры выполнены, 1 — есть ошибки, 2 — ошибка запуска.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------- параметры по умолчанию (тестовый стенд)

DEFAULT_ROOT = Path(__file__).resolve().parents[1] / "prod_repl"   # <корень проекта>/prod_repl: имена баз = папки
DEFAULT_SERVER = "127.0.0.1"
DEFAULT_PORT = "1433"
DEFAULT_USER = "sa"
DEFAULT_PASSWORD = 'YourStrong_Password123!'
DEFAULT_DRIVER = None              # None — новейший установленный ODBC Driver NN for SQL Server
DEFAULT_TIMEOUT = 1800             # таймаут одной процедуры, с
MAX_ATTEMPTS = 3                   # повторов упавшей процедуры с другими значениями параметров
EXPLORE_ATTEMPTS = 6               # запусков процедуры с непокрытыми операторами (доисследование по покрытию)
DATE_FROM = dt.date(2026, 1, 1)    # период для параметров-дат (данные 02_fill_test_data.py: 2020-01-01..2026-09-30)
DATE_TO = dt.date(2026, 6, 30)

SYSTEM_DATABASES = {"master", "model", "msdb", "tempdb"}


# --------------------------------------------------------------------------- модель

@dataclass
class Param:
    name: str
    type: str
    length: int | None
    precision: int
    scale: int
    output: bool
    table_type: str | None       # schema.type для табличного параметра
    has_default: bool = False


@dataclass
class Proc:
    db: str
    id: int
    schema: str
    name: str
    definition: str
    params: list[Param] = field(default_factory=list)
    calls: set[int] = field(default_factory=set)         # статически видимые вызовы
    reads: set[int] = field(default_factory=set)
    writes: set[int] = field(default_factory=set)
    creates_jobs: bool = False
    dynamic: bool = False
    # результат
    phase: str = ""              # root | via | direct
    via: str = ""
    status: str = "pending"      # ok | failed | skipped
    seconds: float = 0.0
    error: str = ""
    sql: str = ""
    attempts: int = 0            # запусков с разными значениями параметров
    static_writes: int = 0       # операторов записи в коде процедуры
    qs_writes: int = 0           # из них попали в Query Store

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def sql_name(self) -> str:
        return f"{q(self.schema)}.{q(self.name)}"


def q(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


# --------------------------------------------------------------------------- подключение

def pick_driver(pyodbc, wanted: str | None) -> str:
    if wanted:
        return wanted
    found = sorted((d for d in pyodbc.drivers() if re.fullmatch(r"ODBC Driver \d+ for SQL Server", d)),
                   key=lambda d: int(re.search(r"\d+", d).group()))
    if not found:
        sys.exit("Не найден ODBC Driver for SQL Server; укажите --driver")
    return found[-1]


class Server:
    def __init__(self, args):
        import pyodbc
        self.pyodbc = pyodbc
        self.driver = pick_driver(pyodbc, args.driver)
        parts = [f"DRIVER={{{self.driver}}}", f"SERVER={args.server},{args.port}", "TrustServerCertificate=yes"]
        if args.trusted:
            parts.append("Trusted_Connection=yes")
        else:
            password = os.environ.get(args.password_env) or DEFAULT_PASSWORD
            parts += [f"UID={args.user}", f"PWD={password}"]
        self.base = ";".join(parts)
        self.timeout = args.timeout
        self.connections = {}

    def conn(self, db: str):
        if db not in self.connections:
            c = self.pyodbc.connect(self.base + f";DATABASE={db}", autocommit=True)
            c.timeout = self.timeout
            self.connections[db] = c
        return self.connections[db]

    def rows(self, db: str, sql: str, params=()):
        return self.conn(db).cursor().execute(sql, params).fetchall()

    def run(self, db: str, sql: str, params=()) -> None:
        cur = self.conn(db).cursor()
        cur.execute(sql, params)
        while True:
            if cur.description is not None:
                cur.fetchall()
            if not cur.nextset():
                break

    def reset(self, db: str) -> None:
        c = self.connections.pop(db, None)
        if c is not None:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    def close(self):
        for db in list(self.connections):
            self.reset(db)


def short(exc: Exception) -> str:
    text = str(exc)
    found = re.findall(r"\[SQL Server\](.*?)(?=\s*\(SQLExecDirectW\)|;\s*\[\w+\]\s*\[Microsoft\]|[\"']\)?$)", text)
    text = "; ".join(s.strip() for s in found) if found else text
    return text.replace("\\'", "'").replace("\r", " ").replace("\n", " ").strip()[:400]


# --------------------------------------------------------------------------- Query Store

def ensure_query_store(srv: Server, db: str, log) -> None:
    state = srv.rows(db, "SELECT actual_state_desc, query_capture_mode_desc FROM sys.database_query_store_options")
    st, mode = state[0] if state else ("OFF", "")
    if st == "READ_WRITE" and mode == "ALL":
        log(f"[{db}] Query Store: READ_WRITE, захват ALL")
        return
    srv.run("master", f"ALTER DATABASE {q(db)} SET QUERY_STORE = ON "
                      f"(OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL)")
    log(f"[{db}] Query Store: было {st}/{mode or '-'} → READ_WRITE, захват ALL")


EXECUTED_SQL = """
SELECT ps.object_id FROM sys.dm_exec_procedure_stats ps
WHERE ps.database_id = DB_ID() AND ps.last_execution_time >= ?
UNION
SELECT q.object_id FROM sys.query_store_query q
JOIN sys.query_store_plan p ON p.query_id = q.query_id
JOIN sys.query_store_runtime_stats rs ON rs.plan_id = p.plan_id
WHERE q.object_id > 0 AND rs.last_execution_time >= ?
"""


def executed_since(srv: Server, db: str, since: dt.datetime) -> set[int]:
    return {r[0] for r in srv.rows(db, EXECUTED_SQL, (since, since))}


# --------------------------------------------------------------------------- каталог процедур

PROCS_SQL = """
SELECT p.object_id, s.name, p.name, m.definition
FROM sys.procedures p JOIN sys.schemas s ON s.schema_id = p.schema_id
JOIN sys.sql_modules m ON m.object_id = p.object_id
WHERE p.is_ms_shipped = 0
"""
PARAMS_SQL = """
SELECT pr.object_id, pr.name, TYPE_NAME(pr.system_type_id), pr.max_length, pr.precision, pr.scale, pr.is_output,
       CASE WHEN tt.user_type_id IS NOT NULL THEN SCHEMA_NAME(tt.schema_id) + '.' + tt.name END
FROM sys.parameters pr
JOIN sys.procedures p ON p.object_id = pr.object_id AND p.is_ms_shipped = 0
LEFT JOIN sys.table_types tt ON tt.user_type_id = pr.user_type_id
WHERE pr.parameter_id > 0
ORDER BY pr.object_id, pr.parameter_id
"""
DEPS_SQL = """
SELECT d.referencing_id, d.referenced_id
FROM sys.sql_expression_dependencies d
JOIN sys.procedures a ON a.object_id = d.referencing_id
JOIN sys.procedures b ON b.object_id = d.referenced_id
WHERE d.referencing_id <> d.referenced_id
"""
REFS_SQL = """
SELECT DISTINCT r.referenced_id, CAST(r.is_updated AS int), CAST(r.is_selected AS int)
FROM sys.dm_sql_referenced_entities(?, 'OBJECT') r
WHERE r.referenced_id IS NOT NULL AND r.referenced_minor_id = 0
"""
JOB_PATTERN = re.compile(r"\bsp_add_job(step|schedule|server)?\b", re.I)
DYNAMIC_PATTERN = re.compile(r"\bsp_executesql\b|\bEXEC(UTE)?\s*\(\s*@", re.I)


def load_procs(srv: Server, db: str, log) -> dict[int, Proc]:
    procs = {r[0]: Proc(db, r[0], r[1], r[2], r[3] or "") for r in srv.rows(db, PROCS_SQL)}
    for oid, name, typ, max_len, prec, scale, output, table_type in srv.rows(db, PARAMS_SQL):
        if oid in procs:
            length = None if max_len == -1 else (max_len // 2 if typ in ("nvarchar", "nchar") else max_len)
            procs[oid].params.append(Param(name, (typ or "").lower(), length, prec, scale, bool(output), table_type))
    for p in procs.values():
        mark_defaults(p)
        p.creates_jobs = bool(JOB_PATTERN.search(p.definition))
        p.dynamic = bool(DYNAMIC_PATTERN.search(p.definition))
    for a, b in srv.rows(db, DEPS_SQL):
        if a in procs and b in procs:
            procs[a].calls.add(b)
    # имена процедур после EXEC, в том числе внутри строк динамического SQL
    by_name = defaultdict(list)
    for p in procs.values():
        by_name[p.name.lower()].append(p.id)
    exec_re = re.compile(r"EXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?(?:\[?[\w ]+\]?\.)?\[?(\w+)\]?", re.I)
    for p in procs.values():
        for m in exec_re.finditer(p.definition):
            for oid in by_name.get(m.group(1).lower(), []):
                if oid != p.id:
                    p.calls.add(oid)
    for p in procs.values():
        try:
            for ref, upd, sel in srv.rows(db, REFS_SQL, (f"{p.schema}.{p.name}",)):
                (p.writes if upd else p.reads).add(ref)
                if upd and sel:
                    p.reads.add(ref)
        except srv.pyodbc.Error:
            pass          # процедуры с временными таблицами и т. п. — без потока данных
    return procs


def mark_defaults(p: Proc) -> None:
    """Параметры со значением по умолчанию в заголовке процедуры ( @p INT = 0 )."""
    text = strip_comments(p.definition)
    positions = []
    start = 0
    for prm in p.params:
        m = re.search(re.escape(prm.name) + r"\b", text[start:], re.I)
        if not m:
            return
        positions.append(start + m.start())
        start += m.end()
    if not positions:
        return
    end = re.search(r"\bAS\b", text[positions[-1]:], re.I)
    header_end = positions[-1] + (end.start() if end else len(text))
    for k, prm in enumerate(p.params):
        seg = text[positions[k] + len(prm.name):positions[k + 1] if k + 1 < len(positions) else header_end]
        prm.has_default = "=" in seg


def strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


# --------------------------------------------------------------------------- значения параметров

class ParamValues:
    def __init__(self, srv: Server, db: str, procs: dict[int, Proc]):
        self.srv, self.db = srv, db
        self._columns = None
        self._cache = {}
        self.call_args = call_site_args(procs)
        self.call_args_by_name: dict[str, list[str]] = defaultdict(list)
        for args in self.call_args.values():
            for name, v in args.items():
                if v not in self.call_args_by_name[name]:
                    self.call_args_by_name[name].append(v)

    def columns(self) -> dict[str, list[tuple[int, str]]]:
        """имя колонки -> [(object_id, таблица)], где она есть."""
        if self._columns is None:
            self._columns = defaultdict(list)
            for col, oid, tbl in self.srv.rows(self.db, """
                    SELECT c.name, t.object_id, QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                    FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id AND t.is_ms_shipped = 0
                    JOIN sys.schemas s ON s.schema_id = t.schema_id"""):
                self._columns[col.lower()].append((oid, tbl))
        return self._columns

    def samples(self, column: str, prefer: set[int] = frozenset()) -> list:
        """Частые значения колонки с таким именем — сначала из таблиц, с которыми работает процедура."""
        key = (column.lower(), frozenset(prefer))
        if key not in self._cache:
            self._cache[key] = []
            tables = sorted(self.columns().get(column.lower(), []), key=lambda x: (x[0] not in prefer, x[1]))
            for _, tbl in tables:
                try:
                    rows = self.srv.rows(self.db, f"SELECT TOP 3 {q(column)} FROM {tbl} WHERE {q(column)} IS NOT NULL "
                                                  f"GROUP BY {q(column)} ORDER BY COUNT(*) DESC")
                except self.srv.pyodbc.Error:
                    continue
                self._cache[key] += [r[0] for r in rows if r[0] not in self._cache[key]]
                if len(self._cache[key]) >= 3:
                    break
        return self._cache[key]

    def next_value(self, column: str, prefer: set[int]):
        key = ("next", column.lower(), frozenset(prefer))
        if key not in self._cache:
            self._cache[key] = None
            tables = sorted(self.columns().get(column.lower(), []), key=lambda x: (x[0] not in prefer, x[1]))
            for _, tbl in tables[:1]:
                try:
                    v = self.srv.rows(self.db, f"SELECT MAX({q(column)}) FROM {tbl}")[0][0]
                except self.srv.pyodbc.Error:
                    continue
                if isinstance(v, int):
                    d = as_date(v)      # номер-дата ГГГГММДД: следующий — следующий день
                    self._cache[key] = int((d + dt.timedelta(days=1)).strftime("%Y%m%d")) if d else v + 1
        return self._cache[key]

    def candidates(self, proc: Proc, prm: Param) -> list:
        """Значения параметра по убыванию уверенности; попытка k берёт k-й кандидат."""
        n = prm.name.lstrip("@").lower()
        t = prm.type
        if n.startswith("parent_") or n == "run_id":
            return [None]
        if n in ("debug", "verbose", "dry_run", "whatif"):
            return [0]
        if t in ("date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"):
            if re.search(r"(to|end|till|finish)$|_to_|_end_", n):
                first = DATE_TO
            elif re.search(r"(month|period)", n):
                first = DATE_TO.replace(day=1)
            elif re.search(r"(from|start|begin)$|_from_|_start_", n):
                first = DATE_FROM
            else:
                first = DATE_TO
            # даты из одноимённых колонок: условие snapshot_date = @snapshot_date найдёт строки
            found = [d for d in (as_date(v) for v in self.samples(n, proc.reads | proc.writes)) if d and d != first]
            return [first] + found
        literals = [self.call_args.get(proc.id, {}).get(n), first_literal(proc.definition, prm.name)]
        literals += self.call_args_by_name.get(n, [])          # тот же параметр в вызовах других процедур
        literals = [v for v in literals if v is not None]
        sampled = self.samples(n, proc.reads | proc.writes)
        out = []
        if t in ("char", "varchar", "nchar", "nvarchar"):
            out = literals + [str(v) for v in sampled]
        elif t in ("tinyint", "smallint", "int", "bigint", "decimal", "numeric", "money", "float", "real"):
            out = [float(v) if "." in v else int(v) for v in literals if re.fullmatch(r"-?\d+(\.\d+)?", v)]
            out += [v for v in sampled if isinstance(v, (int, float)) or type(v).__name__ == "Decimal"]
            # новое значение (MAX + 1): для проверок «такой пакет уже загружен — выходим»
            nxt = self.next_value(n, proc.reads | proc.writes) if t in ("int", "bigint") else None
            if nxt is not None:
                out.append(nxt)
            out = out or [1 if re.search(r"(id|key|no|num)$", n) else 0]
        elif t == "bit":
            out = [int(v) for v in literals if v in ("0", "1")] or [0]
        elif t == "uniqueidentifier":
            out = sampled
        unique = []
        for v in out:
            if v not in unique:
                unique.append(v)
        return unique or [None]

    def combos(self, proc: Proc) -> list[dict]:
        """Наборы значений параметров по порядку попыток:
        0 — лучшие кандидаты каждого параметра;
        затем согласованные наборы из одной строки таблицы процедуры (дата снимка и пакет из одной строки)
        и та же строка с новыми идентификаторами (проверка «пакет уже загружен» пропустит);
        затем следующие кандидаты каждого параметра."""
        key = ("combos", proc.id)
        if key in self._cache:
            return self._cache[key]
        params = [p for p in proc.params if not (p.has_default or p.output or p.table_type)]
        cands = {p.name: self.candidates(proc, p) for p in params}

        def base(k: int) -> dict:
            return {n: c[min(k, len(c) - 1)] for n, c in cands.items()}

        combos = [base(0)]
        for row, oid in self.param_rows(proc, params):
            combos.append({**base(0), **row})
            fresh = {}
            for p in params:
                if p.name in row and p.type in ("int", "bigint"):
                    nxt = self.next_value(p.name.lstrip("@"), {oid})
                    if nxt is not None:
                        fresh[p.name] = nxt
            if fresh:
                combos.append({**base(0), **row, **fresh})
        combos += [base(k) for k in range(1, max([len(c) for c in cands.values()] or [1]))]
        unique = []
        for c in combos:
            if c not in unique:
                unique.append(c)
        self._cache[key] = unique
        return unique

    def param_rows(self, proc: Proc, params: list[Param]) -> list[tuple[dict, int]]:
        """До трёх строк таблицы процедуры, где больше всего колонок совпадает с именами параметров."""
        wanted = {p.name.lstrip("@").lower(): p for p in params if not p.name.lower().startswith("@parent_")}
        tables = defaultdict(dict)
        for col, places in self.columns().items():
            if col in wanted:
                for oid, tbl in places:
                    if oid in proc.reads | proc.writes:
                        tables[(oid, tbl)][col] = wanted[col]
        best = max(tables.items(), key=lambda kv: (len(kv[1]), kv[0][1]), default=None)
        if not best or (len(best[1]) < 2 and len(wanted) > 1):
            return []
        (oid, tbl), matched = best
        cols = sorted(matched)
        try:
            rows = self.srv.rows(self.db, f"SELECT DISTINCT TOP 3 {', '.join(q(c) for c in cols)} FROM {tbl} "
                                          f"WHERE {' AND '.join(f'{q(c)} IS NOT NULL' for c in cols)} "
                                          f"ORDER BY {q(cols[0])} DESC")
        except self.srv.pyodbc.Error:
            return []
        out = []
        for r in rows:
            values = {}
            for c, v in zip(cols, r):
                p = matched[c]
                if p.type in ("date", "datetime", "datetime2", "smalldatetime"):
                    v = as_date(v)
                elif p.type in ("int", "bigint", "smallint", "tinyint"):
                    v = int(v) if str(v).lstrip("-").isdigit() else None
                elif p.type in ("char", "varchar", "nchar", "nvarchar"):
                    v = str(v)
                if v is not None:
                    values[p.name] = v
            if values:
                out.append((values, oid))
        return out

    def value(self, proc: Proc, prm: Param, attempt: int = 0):
        combos = self.combos(proc)
        combo = combos[min(attempt, len(combos) - 1)]
        if prm.name in combo:
            return combo[prm.name]
        options = self.candidates(proc, prm)
        return options[min(attempt, len(options) - 1)]

    def attempts(self, proc: Proc) -> int:
        """Сколько разных наборов параметров можно попробовать."""
        return len(self.combos(proc))


def as_date(v) -> dt.date | None:
    """Дата из значения колонки: date/datetime, число ГГГГММДД, строка ДД.ММ.ГГГГ / ГГГГ-ММ-ДД / ГГГГММДД."""
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    s = str(v).strip()
    for fmt in ("%Y%m%d", "%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            d = dt.datetime.strptime(s[:10], fmt).date()
            return d if 1990 <= d.year <= 2100 else None
        except ValueError:
            continue
    return None


SIMPLE_LITERAL = r"N?'{1,2}([\w .:/\-]{0,60})'{1,2}"    # 'FULL', N'DV', ''20260529'' внутри динамической строки


def first_literal(definition: str, name: str) -> str | None:
    """Первый литерал, с которым параметр сравнивается в коде: @mode = 'FULL', @mode IN ('A', 'B')."""
    body = strip_comments(definition)
    m = re.search(re.escape(name) + r"\b\s*(?:=|<>|!=)\s*" + SIMPLE_LITERAL, body, re.I) \
        or re.search(re.escape(name) + r"\b\s+IN\s*\(\s*" + SIMPLE_LITERAL, body, re.I) \
        or re.search(re.escape(name) + r"\b\s*(?:=|<>)\s*\(?(-?\d+)\b", body, re.I)
    return m.group(1) if m else None


def call_site_args(procs: dict[int, Proc]) -> dict[int, dict[str, str]]:
    """Литералы, с которыми другие процедуры вызывают процедуру: EXEC DWH.sp_x @status = 'OK', @batch_id = 1."""
    by_name = defaultdict(list)
    for p in procs.values():
        by_name[p.name.lower()].append(p.id)
    out: dict[int, dict[str, str]] = defaultdict(dict)
    exec_re = re.compile(r"EXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?(?:\[?[\w ]+\]?\.)?\[?(\w+)\]?([^;\n]*)", re.I)
    arg_re = re.compile(r"@(\w+)\s*=\s*(?:" + SIMPLE_LITERAL + r"|(-?\d+(?:\.\d+)?)\b)")
    for caller in procs.values():
        for m in exec_re.finditer(strip_comments(caller.definition)):
            for target in by_name.get(m.group(1).lower(), []):
                if target == caller.id:
                    continue
                for name, text, number in arg_re.findall(m.group(2)):
                    out[target].setdefault(name.lower(), text if not number else number)
    return out


def sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)) or type(v).__name__ == "Decimal":
        return str(v)
    if isinstance(v, dt.datetime):
        return "'" + v.strftime("%Y-%m-%dT%H:%M:%S") + "'"
    if isinstance(v, dt.date):
        return "'" + v.strftime("%Y%m%d") + "'"
    if isinstance(v, (bytes, bytearray)):
        return "0x" + v.hex()
    return "N'" + str(v).replace("'", "''") + "'"


def type_sql(prm: Param) -> str:
    t = prm.type
    if t in ("varchar", "char", "nvarchar", "nchar", "varbinary", "binary"):
        return f"{t}({'max' if prm.length is None else prm.length})"
    if t in ("decimal", "numeric"):
        return f"{t}({prm.precision},{prm.scale})"
    if t in ("datetime2", "datetimeoffset", "time"):
        return f"{t}({prm.scale})"
    return t


def build_call(proc: Proc, values: ParamValues, attempt: int = 0) -> str:
    """Пакет T-SQL: объявления OUTPUT/табличных переменных и EXEC с именованными параметрами."""
    decl, args = call_args(proc, values, attempt)
    call = f"EXEC {proc.sql_name}" + (" " + ", ".join(args) if args else "") + ";"
    return "\n".join(decl + [call])


def call_args(proc: Proc, values: ParamValues, attempt: int = 0) -> tuple[list[str], list[str]]:
    decl, args = [], []
    for k, prm in enumerate(proc.params):
        if prm.has_default and not prm.output and not prm.table_type:
            continue
        if prm.table_type:
            var = f"@tvp{k}"
            schema, _, tname = prm.table_type.partition(".")
            decl.append(f"DECLARE {var} {q(schema)}.{q(tname)};")
            fill = tvp_fill(values, prm.table_type, var)
            if fill:
                decl.append(fill)
            args.append(f"{prm.name} = {var}")
        elif prm.output:
            var = f"@out{k}"
            decl.append(f"DECLARE {var} {type_sql(prm)};")
            args.append(f"{prm.name} = {var} OUTPUT")
        else:
            args.append(f"{prm.name} = {sql_literal(values.value(proc, prm, attempt))}")
    return decl, args


def tvp_fill(values: ParamValues, table_type: str, var: str) -> str | None:
    schema, _, name = table_type.partition(".")
    cols = [r[0] for r in values.srv.rows(values.db, """
        SELECT c.name FROM sys.table_types tt JOIN sys.columns c ON c.object_id = tt.type_table_object_id
        WHERE tt.name = ? AND SCHEMA_NAME(tt.schema_id) = ? AND c.is_computed = 0 AND c.is_identity = 0
        ORDER BY c.column_id""", (name, schema))]
    if not cols:
        return None
    tables = None
    for c in cols:
        has = {tbl for _, tbl in values.columns().get(c.lower(), [])}
        tables = has if tables is None else tables & has
    if not tables:
        return None
    # справочник сущности из имени первой колонки (employee_id -> ...employee...), иначе первая по имени
    entity = re.sub(r"_(id|key|code|sk)$", "", cols[0].lower())
    table = sorted(tables, key=lambda t: (entity not in t.lower(), len(t), t))[0]
    col_list = ", ".join(q(c) for c in cols)
    return f"INSERT INTO {var} ({col_list}) SELECT DISTINCT TOP (5) {col_list} FROM {table};"


# --------------------------------------------------------------------------- порядок запуска

def run_order(procs: dict[int, Proc], subset: list[Proc] | None = None) -> list[Proc]:
    """Корневые процедуры (никем не вызываются) — или заданный набор — в порядке потока данных:
    писатели раньше читателей (с учётом того, что пишут и читают вызываемые процедуры)."""
    if subset is None:
        called = {c for p in procs.values() for c in p.calls}
        roots = [p for p in procs.values() if p.id not in called]
    else:
        roots = list(subset)

    def closure(p: Proc, attr: str, seen=None) -> set[int]:
        seen = seen or set()
        if p.id in seen:
            return set()
        seen.add(p.id)
        out = set(getattr(p, attr))
        for c in p.calls:
            if c in procs:
                out |= closure(procs[c], attr, seen)
        return out

    writes = {p.id: closure(p, "writes") for p in roots}
    reads = {p.id: closure(p, "reads") for p in roots}
    before = {p.id: {o.id for o in roots if o.id != p.id and writes[o.id] & reads[p.id]} for p in roots}
    order, done = [], set()
    remaining = {p.id: p for p in roots}
    while remaining:
        ready = [p for p in remaining.values() if before[p.id] <= done]
        if not ready:      # взаимная зависимость: меньше всего невыполненных предшественников, затем по имени
            ready = [min(remaining.values(), key=lambda p: (len(before[p.id] - done), p.fqn.lower()))]
        for p in sorted(ready, key=lambda p: p.fqn.lower()):
            order.append(p)
            done.add(p.id)
            remaining.pop(p.id)
    return order


# --------------------------------------------------------------------------- джобы SQL Agent

class Jobs:
    def __init__(self, srv: Server, log):
        self.srv, self.log = srv, log
        self.known = self.job_ids()
        self.created: dict[str, dict] = {}     # job_id -> {name, by, steps}

    def job_ids(self) -> set[str]:
        return {str(r[0]) for r in self.srv.rows("msdb", "SELECT job_id FROM dbo.sysjobs")}

    def capture(self, by: Proc) -> list[str]:
        new = self.job_ids() - self.known
        names = []
        for job_id in sorted(new):
            name = self.srv.rows("msdb", "SELECT name FROM dbo.sysjobs WHERE job_id = ?", (job_id,))[0][0]
            steps = self.srv.rows("msdb", """
                SELECT step_id, step_name, subsystem, database_name, command
                FROM dbo.sysjobsteps WHERE job_id = ? ORDER BY step_id""", (job_id,))
            self.srv.run("msdb", "EXEC dbo.sp_update_job @job_id = ?, @enabled = 0", (job_id,))
            self.created[job_id] = {"name": name, "by": f"{by.db}.{by.fqn}", "steps": steps}
            self.known.add(job_id)
            names.append(name)
        return names

    def run_steps(self, results: list) -> None:
        """Шаги ещё не выполненных джобов — то, что джоб сделал бы по расписанию."""
        for job_id, job in self.created.items():
            if job.get("ran"):
                continue
            job["ran"] = True
            for step_id, step_name, subsystem, db, command in job["steps"]:
                label = f"джоб «{job['name']}» шаг {step_id} «{step_name}»"
                if subsystem != "TSQL":
                    results.append((label, "skipped", 0.0, f"подсистема {subsystem} не выполняется"))
                    continue
                if "$(" in (command or ""):
                    results.append((label, "skipped", 0.0, "в команде токены SQL Agent $(...)"))
                    continue
                started = time.perf_counter()
                try:
                    self.srv.run(db or "master", command)
                    self.srv.run(db or "master", "IF @@TRANCOUNT > 0 ROLLBACK")
                    results.append((label, "ok", time.perf_counter() - started, ""))
                except self.srv.pyodbc.Error as exc:
                    self.srv.reset(db or "master")
                    results.append((label, "failed", time.perf_counter() - started, short(exc)))
                self.log(f"  {results[-1][1].upper():7} {label}" + (f" — {results[-1][3]}" if results[-1][3] else ""))

    def cleanup(self, keep: bool) -> None:
        for job_id, job in self.created.items():
            if keep:
                continue
            try:
                self.srv.run("msdb", "EXEC dbo.sp_delete_job @job_id = ?", (job_id,))
            except self.srv.pyodbc.Error as exc:
                self.log(f"  джоб «{job['name']}» не удалён: {short(exc)}")


# --------------------------------------------------------------------------- запуск

def execute(srv: Server, proc: Proc, values: ParamValues, log) -> None:
    """Запуск; при ошибке — повтор со следующими кандидатами в значения параметров (до MAX_ATTEMPTS)."""
    started = time.perf_counter()
    errors = []
    for attempt in range(min(values.attempts(proc), MAX_ATTEMPTS)):
        proc.sql = build_call(proc, values, attempt)
        try:
            srv.run(proc.db, proc.sql)
            proc.status, proc.error = "ok", ""
        except srv.pyodbc.Error as exc:
            proc.status = "failed"
            errors.append(short(exc))
            srv.reset(proc.db)
        try:
            srv.run(proc.db, "IF @@TRANCOUNT > 0 ROLLBACK")
        except srv.pyodbc.Error:
            srv.reset(proc.db)
        if proc.status == "ok":
            break
    proc.attempts = attempt + 1
    if proc.status == "failed":
        proc.error = errors[0] + (f" (попыток {len(errors)})" if len(errors) > 1 else "")
    proc.seconds = time.perf_counter() - started


def explore(srv: Server, db: str, procs: dict[int, Proc], values: ParamValues, excluded: set[int], log) -> None:
    """Доисследование: процедуры с непокрытыми операторами записи запускаются с другими кандидатами
    в значения параметров — другие значения ведут в другие ветки кода."""
    coverage(srv, db, procs)
    gaps = [p for p in procs.values() if p.static_writes and p.qs_writes < p.static_writes and p.id not in excluded
            and values.attempts(p) > 1]
    if not gaps:
        return
    before = sum(p.qs_writes for p in procs.values())
    runs = 0
    for p in sorted(gaps, key=lambda x: x.fqn.lower()):
        for attempt in range(1, min(values.attempts(p), EXPLORE_ATTEMPTS)):
            try:
                srv.run(db, build_call(p, values, attempt))
            except srv.pyodbc.Error:
                srv.reset(db)
            try:
                srv.run(db, "IF @@TRANCOUNT > 0 ROLLBACK")
            except srv.pyodbc.Error:
                srv.reset(db)
            runs += 1
    coverage(srv, db, procs)
    after = sum(p.qs_writes for p in procs.values())
    log(f"  доисследование: процедур с непокрытыми операторами {len(gaps)}, запусков {runs}, "
        f"покрыто операторов записи: было {before}, стало {after}")


def config_calls(srv: Server, db: str, procs: dict[int, Proc], log) -> list[dict]:
    """Вызовы через конфигурационные таблицы: процедура с динамическим SQL читает таблицу (которую процедуры
    не пишут — в отличие от журналов), а в таблице записаны имена процедур. Такие вызовы добавляются в граф."""
    by_name = defaultdict(set)
    for p in procs.values():
        by_name[p.name.lower()].add(p.id)
    written = {t for p in procs.values() for t in p.writes}
    readers = defaultdict(list)
    for p in procs.values():
        if p.dynamic:
            for t in p.reads - written:
                readers[t].append(p)
    if not readers:
        return []
    found = []
    cols = defaultdict(list)
    for oid, table, col in srv.rows(db, f"""
        SELECT c.object_id, QUOTENAME(s.name) + '.' + QUOTENAME(t.name), c.name
        FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE c.object_id IN ({','.join(str(i) for i in readers)}) AND TYPE_NAME(c.system_type_id) IN ('varchar','nvarchar')
          AND (c.name LIKE '%proc%' OR c.name LIKE '%sp[_]%' OR c.name LIKE '%routine%' OR c.name LIKE '%command%'
               OR c.name LIKE '%object%' OR c.name LIKE '%handler%' OR c.name LIKE '%sql%')"""):
        cols[(oid, table)].append(col)
    for (oid, table), columns in cols.items():
        total = srv.rows(db, f"SELECT COUNT(*) FROM {table}")[0][0]
        targets: set[int] = set()
        for col in columns:
            for (v,) in srv.rows(db, f"SELECT TOP 10000 {q(col)} FROM {table} WHERE {q(col)} IS NOT NULL"):
                for token in set(re.findall(r"\w+", str(v).lower())):
                    targets |= by_name.get(token, set())
        callers = readers[oid]
        for p in callers:
            p.calls |= targets - {p.id}
        log(f"  конфигурационная таблица {table} (колонки {', '.join(columns)}): строк {total}, "
            f"процедур в ней {len(targets)}; читает {', '.join(sorted(p.fqn for p in callers))}")
        found.append({"oid": oid, "table": table, "columns": columns, "readers": callers, "targets": targets,
                      "rows": total})
    return found


# --------------------------------------------------------------------------- заполнение конфигурационных таблиц
#
# Содержимое конфигов с прода недоступно, синтетика (02_fill_test_data.py) пишет в них случайные строки.
# Скрипт подставляет в конфиг процедуры базы и подбирает формат записи, который понимает читающая процедура:
# пробует варианты на одной процедуре и проверяет по sys.dm_exec_procedure_stats, вызвалась ли она.

NAME_COL = re.compile(r"proc|routine|sp_|handler|object", re.I)
CMD_COL = re.compile(r"command|sql|cmd|statement", re.I)
PARAM_COL = re.compile(r"param|arg", re.I)


def reader_filters(readers: list[Proc], columns: list[str]) -> dict[str, str]:
    """Условия читающих процедур на другие колонки конфига: is_active = 1, enabled = 'Y'."""
    out = {}
    for col in columns:
        for p in readers:
            m = re.search(r"(?<![@\w])(?:\w+\.)?\[?" + re.escape(col) + r"\]?\s*=\s*(N?'[^']*'|-?\d+)",
                          strip_comments(p.definition), re.I)
            if m:
                out[col] = m.group(1)
                break
    return out


def prepare_configs(srv: Server, db: str, procs: dict[int, Proc], values: ParamValues,
                    configs: list[dict], log) -> bool:
    """Вернёт True, если хотя бы один конфиг заполнен (граф вызовов нужно перечитать)."""
    changed = False
    readers_all = {p.id for c in configs for p in c["readers"]}
    called = {c for p in procs.values() for c in p.calls}
    children = []
    for p in sorted(procs.values(), key=lambda x: x.fqn.lower()):
        if p.id in called or p.id in readers_all:
            continue
        decl, args = call_args(p, values)
        if not decl:                      # OUTPUT и табличные параметры через конфиг не передать
            children.append((p, ", ".join(args)))
    children.sort(key=lambda x: (x[1] != "", x[0].fqn.lower()))   # проба — процедура без параметров
    for cfg in configs:
        if cfg["targets"] or not cfg["rows"] or not children:
            if not cfg["targets"] and not cfg["rows"]:
                log(f"  конфиг {cfg['table']} пуст — заполнить нечем, процедуры будут запущены напрямую")
            continue
        info = srv.rows(db, "SELECT c.name, TYPE_NAME(c.system_type_id), c.max_length FROM sys.columns c "
                            "WHERE c.object_id = ? AND c.is_computed = 0 AND c.is_identity = 0", (cfg["oid"],))
        lengths = {n: (None if ml == -1 else (ml // 2 if t in ("nvarchar", "nchar") else ml)) for n, t, ml in info}
        string_cols = [n for n, t, _ in info if t in ("varchar", "nvarchar", "char", "nchar")]
        name_col = next((c for c in cfg["columns"] if NAME_COL.search(c)), None) \
            or next((c for c in cfg["columns"] if CMD_COL.search(c)), None)
        param_col = next((c for c in string_cols if PARAM_COL.search(c) and c != name_col), None)
        filters = reader_filters(cfg["readers"], [n for n, _, _ in info if n not in (name_col, param_col)])
        table = cfg["table"]

        def formats(p: Proc, args: str) -> list[dict]:
            """Варианты записи в фиксированном порядке (номер варианта одинаков для всех процедур)."""
            two, br, one = f"{p.schema}.{p.name}", p.sql_name, p.name
            out = []
            if param_col:
                out += [{name_col: two, param_col: args}, {name_col: br, param_col: args}, {name_col: one, param_col: args}]
            full = [{name_col: f"EXEC {two} {args}".strip()}, {name_col: f"{two} {args}".strip()}]
            return (full + out) if CMD_COL.search(name_col) else (out + full + [{name_col: two}, {name_col: one}])

        def fit(fmt: dict) -> bool:
            return all(lengths.get(c) is None or len(v) <= lengths[c] for c, v in fmt.items())

        def set_all(values_by_col: dict) -> None:
            sets = ", ".join(f"{q(c)} = {v if c in filters and v == filters[c] else sql_literal(v)}"
                             for c, v in {**values_by_col, **filters}.items())
            srv.run(db, f"UPDATE {table} SET {sets}")

        probe, probe_args = children[0]
        chosen = None
        for k, fmt in enumerate(formats(probe, probe_args)):
            if not fit(fmt):
                continue
            try:
                try:
                    set_all(fmt)
                except srv.pyodbc.Error:      # уникальность колонки имени — оставить одну строку
                    srv.run(db, f"WITH r AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM {table}) "
                                f"DELETE FROM r WHERE rn > 1")
                    set_all(fmt)
            except srv.pyodbc.Error as exc:
                log(f"    вариант {k + 1}: не записать в конфиг — {short(exc)}")
                continue
            since = srv.rows(db, "SELECT SYSDATETIME()")[0][0] - dt.timedelta(seconds=1)
            for r in cfg["readers"]:
                try:
                    srv.run(db, build_call(r, values))
                    srv.run(db, "IF @@TRANCOUNT > 0 ROLLBACK")
                except srv.pyodbc.Error:
                    srv.reset(db)
            if probe.id in executed_since(srv, db, since):
                chosen = k
                break
        if chosen is None:
            log(f"  конфиг {table}: формат записи не подобран (проба {probe.fqn}) — процедуры будут запущены напрямую")
            continue
        sample = formats(probe, probe_args)[chosen]
        n_rows = srv.rows(db, f"SELECT COUNT(*) FROM {table}")[0][0]
        placed = [formats(p, a)[chosen] for p, a in children if fit(formats(p, a)[chosen])][:n_rows]
        case = {c: "CASE rn " + " ".join(f"WHEN {i + 1} THEN {sql_literal(f[c])}" for i, f in enumerate(placed)) + " END"
                for c in sample}
        sets = ", ".join(f"{q(c)} = {expr}" for c, expr in case.items())
        srv.run(db, f"WITH r AS (SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM {table}) "
                    f"UPDATE r SET {sets} WHERE rn <= {len(placed)}")
        try:
            srv.run(db, f"WITH r AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM {table}) "
                        f"DELETE FROM r WHERE rn > {len(placed)}")
        except srv.pyodbc.Error:
            pass
        shown = ", ".join(f"{c} = {v!r}" for c, v in sample.items())
        log(f"  конфиг {table}: формат подобран ({shown}); записано процедур {len(placed)}"
            + (f", не поместились {len(children) - len(placed)} — будут запущены напрямую"
               if len(children) > len(placed) else "")
            + (f"; условия читающей процедуры: {filters}" if filters else ""))
        changed = True
    return changed


# --------------------------------------------------------------------------- покрытие

WRITE_PATTERNS = [
    re.compile(r"\bINSERT\s+(?:INTO\s+)?(?!\(|VALUES\b|EXEC)[\[#@\w]", re.I),
    re.compile(r"\bUPDATE\s+(?!SET\b|STATISTICS\b)[\[#@\w]", re.I),
    re.compile(r"\bDELETE\s+(?:FROM\s+)?(?!WHEN\b|OUTPUT\b)[\[#@\w]", re.I),
    re.compile(r"\bMERGE\s+(?:INTO\s+)?[\[#\w]", re.I),
    re.compile(r"\bSELECT\b(?:(?!\bFROM\b|;|\bSELECT\b).)*?\bINTO\s+[\[#\w]", re.I | re.S),
]


def count_writes(sql: str) -> int:
    code = strip_comments(sql)
    code = re.sub(r"N?'(?:[^']|'')*'", "''", code)          # динамический SQL в строках не считается
    return sum(len(p.findall(code)) for p in WRITE_PATTERNS)


def coverage(srv: Server, db: str, procs: dict[int, Proc]) -> None:
    """Покрытие: операторы записи процедуры, попавшие в Query Store, к операторам записи в её коде.
    Непокрытый оператор — ветка, до которой выполнение не дошло: lineage для него не построится."""
    seen = defaultdict(int)
    for oid, text in srv.rows(db, """
            SELECT DISTINCT q.object_id, qt.query_sql_text
            FROM sys.query_store_query q JOIN sys.query_store_query_text qt ON qt.query_text_id = q.query_text_id
            WHERE q.object_id > 0"""):
        if oid in procs and count_writes(re.sub(r"^\s*\(@[^)]*\)", "", text or "")):
            seen[oid] += 1
    for p in procs.values():
        p.static_writes = count_writes(p.definition)
        p.qs_writes = min(seen.get(p.id, 0), p.static_writes)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path, nargs="?", default=DEFAULT_ROOT,
                    help=f"корень реплики — имена баз берутся из папок (по умолчанию {DEFAULT_ROOT})")
    ap.add_argument("--databases", nargs="+", help="базы (по умолчанию — все папки реплики)")
    ap.add_argument("--exclude", help="регулярное выражение по schema.procedure — не запускать")
    ap.add_argument("--no-direct", action="store_true", help="не запускать напрямую процедуры, до которых не дошли вызовы")
    ap.add_argument("--keep-jobs", action="store_true", help="не удалять созданные процедурами джобы (останутся отключёнными)")
    ap.add_argument("--no-qs-setup", action="store_true", help="не включать Query Store")
    ap.add_argument("--qs-clear", action="store_true", help="очистить Query Store перед запуском (чистый замер покрытия)")
    ap.add_argument("--no-explore", action="store_true",
                    help="не перезапускать процедуры с непокрытыми операторами с другими значениями параметров")
    ap.add_argument("--no-config-fill", action="store_true",
                    help="не записывать процедуры в конфигурационные таблицы с синтетическими данными")
    ap.add_argument("--report", type=Path, help="CSV с результатом по каждой процедуре")
    ap.add_argument("--dry-run", action="store_true", help="показать порядок и вызовы без выполнения")
    ap.add_argument("--server", default=os.getenv("MSSQL_HOST", DEFAULT_SERVER))
    ap.add_argument("--port", default=os.getenv("MSSQL_PORT", DEFAULT_PORT))
    ap.add_argument("--user", default=os.getenv("MSSQL_USER", DEFAULT_USER))
    ap.add_argument("--password-env", default="MSSQL_PASSWORD",
                    help="переменная окружения с паролем; если не задана — пароль по умолчанию")
    ap.add_argument("--trusted", action="store_true", help="Windows-аутентификация")
    ap.add_argument("--driver", default=DEFAULT_DRIVER)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="таймаут одной процедуры, с")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    log = lambda s: print(s, flush=True)  # noqa: E731
    if args.databases:
        databases = args.databases
    elif args.root.is_dir():
        databases = sorted(p.name for p in args.root.iterdir() if p.is_dir() and p.name.lower() not in SYSTEM_DATABASES)
    else:
        log(f"Нет папки реплики {args.root}; укажите --databases")
        return 2
    exclude = re.compile(args.exclude, re.I) if args.exclude else None

    srv = Server(args)
    try:
        srv.conn("master")
    except srv.pyodbc.Error as exc:
        log(f"Нет подключения к {args.server},{args.port}: {short(exc)}")
        return 2
    log(f"Сервер {args.server},{args.port}; базы: {', '.join(databases)}; период параметров {DATE_FROM}..{DATE_TO}")
    all_procs: list[Proc] = []
    db_procs: dict[str, dict[int, Proc]] = {}
    job_results: list = []
    jobs = Jobs(srv, log) if not args.dry_run else None
    try:
        for db in databases:
            if not srv.rows("master", "SELECT 1 FROM sys.databases WHERE name = ?", (db,)):
                log(f"[{db}] базы нет на сервере — пропущена")
                continue
            if not args.dry_run and not args.no_qs_setup:
                ensure_query_store(srv, db, log)
            procs = load_procs(srv, db, log)
            values = ParamValues(srv, db, procs)
            log(f"\n[{db}] процедур {len(procs)}; с динамическим SQL {sum(p.dynamic for p in procs.values())}; "
                f"создают джобы {sum(p.creates_jobs for p in procs.values())}")
            configs = config_calls(srv, db, procs, log)
            if configs and not args.dry_run and not args.no_config_fill:
                if prepare_configs(srv, db, procs, values, configs, log):
                    config_calls(srv, db, procs, lambda s: None)      # в конфиге теперь процедуры — обновить граф
            if args.qs_clear and not args.dry_run:
                srv.run("master", f"ALTER DATABASE {q(db)} SET QUERY_STORE CLEAR")
                log(f"  Query Store очищен (--qs-clear)")
            order = run_order(procs)
            excluded = {p.id for p in procs.values() if exclude and exclude.search(p.fqn)}
            log(f"  корневых процедур {len(order)}")
            if args.dry_run:
                for p in order:
                    log(f"  root  {p.fqn}" + (f"  → вызывает: {', '.join(sorted(procs[c].name for c in p.calls))}"
                                              if p.calls else ""))
                    log("        " + build_call(p, values).replace("\n", "\n        "))
                rest = [p for p in procs.values() if p not in order]
                log(f"  вызываются из других: {', '.join(sorted(p.fqn for p in rest)) or '—'}")
                all_procs += procs.values()
                continue
            done: set[int] = set()

            def run(p: Proc, phase: str) -> None:
                server_now = srv.rows(db, "SELECT SYSDATETIME()")[0][0] - dt.timedelta(seconds=1)
                p.phase = phase
                execute(srv, p, values, log)
                inner = executed_since(srv, db, server_now) - {p.id}
                done.add(p.id)
                fresh = [procs[i] for i in inner if i in procs and i not in done]
                for c in fresh:
                    c.phase, c.via, c.status = "via", p.fqn, "ok"
                    done.add(c.id)
                created = jobs.capture(p)
                extra = []
                if fresh:
                    extra.append(f"внутри выполнено {len(fresh)}: {', '.join(sorted(c.name for c in fresh))}")
                if created:
                    extra.append(f"создано джобов {len(created)} (отключены): {', '.join(created)}")
                mark = {"ok": "OK", "failed": "FAIL"}[p.status]
                retry = f" (с попытки {p.attempts})" if p.status == "ok" and p.attempts > 1 else ""
                log(f"  {mark:5} {phase:6} {p.fqn} — {p.seconds:.1f} с{retry}" + (f"; {p.error}" if p.error else "")
                    + "".join(f"\n               {e}" for e in extra))

            for p in order:
                if p.id in excluded:
                    p.status, p.error = "skipped", "--exclude"
                    continue
                if p.id not in done:
                    run(p, "root")
            if any(not j.get("ran") for j in jobs.created.values()):
                # джобы, созданные процедурами: их шаги выполняются до прямого запуска остальных процедур
                log(f"  шаги джобов, созданных процедурами:")
                server_now = srv.rows(db, "SELECT SYSDATETIME()")[0][0] - dt.timedelta(seconds=1)
                jobs.run_steps(job_results)
                for i in executed_since(srv, db, server_now):
                    if i in procs and i not in done:
                        procs[i].phase, procs[i].via, procs[i].status = "job", "шаг джоба", "ok"
                        done.add(i)
                        log(f"         выполнена шагом джоба: {procs[i].fqn}")
            if not args.no_direct:
                rest = [p for p in procs.values() if p.id not in done and p.id not in excluded]
                for p in run_order(procs, rest):           # тоже по потоку данных: загрузчик раньше витрины
                    if p.id not in done:
                        run(p, "direct")
            if not args.no_explore:
                explore(srv, db, procs, values, excluded, log)
            for p in procs.values():
                if p.status == "pending":
                    p.status, p.error = "skipped", p.error or ("--exclude" if p.id in excluded else "не запускалась")
            srv.run(db, "EXEC sys.sp_query_store_flush_db")
            all_procs += procs.values()
            db_procs[db] = procs
        if not args.dry_run and jobs.created:
            if any(not j.get("ran") for j in jobs.created.values()):   # созданы на прямом запуске
                log("\nШаги джобов, созданных при прямом запуске:")
                jobs.run_steps(job_results)
                for db in databases:
                    try:
                        srv.run(db, "EXEC sys.sp_query_store_flush_db")
                    except srv.pyodbc.Error:
                        pass
            jobs.cleanup(args.keep_jobs)
        for db, procs in db_procs.items():      # после шагов джобов: их операторы тоже в Query Store
            coverage(srv, db, procs)
    finally:
        srv.close()
    if args.dry_run:
        return 0
    if args.report:
        with open(args.report, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["database", "procedure", "phase", "via", "status", "seconds", "error", "call",
                        "writes_in_code", "writes_in_query_store"])
            for p in all_procs:
                w.writerow([p.db, p.fqn, p.phase, p.via, p.status, f"{p.seconds:.2f}", p.error, p.sql,
                            p.static_writes, p.qs_writes])
            for label, status, seconds, error in job_results:
                w.writerow(["", label, "job", "", status, f"{seconds:.2f}", error, "", "", ""])
        log(f"Отчёт: {args.report}")
    counts = defaultdict(int)
    for p in all_procs:
        counts[(p.phase or "-", p.status)] += 1
    failed = [p for p in all_procs if p.status == "failed"] + [r for r in job_results if r[1] == "failed"]
    log(f"\n=== Итог: процедур {len(all_procs)}; "
        f"корневых OK {counts[('root', 'ok')]}, выполнено внутри других {counts[('via', 'ok')]}, "
        f"шагами джобов {counts[('job', 'ok')]}, напрямую OK {counts[('direct', 'ok')]}; с ошибкой {len(failed)}; "
        f"пропущено {sum(1 for p in all_procs if p.status == 'skipped')}; "
        f"шагов джобов {len(job_results)}" + (" (джобы оставлены отключёнными)" if args.keep_jobs and job_results else ""))
    for p in all_procs:
        if p.status == "failed":
            log(f"  ошибка: {p.db}.{p.fqn} ({p.phase}) — {p.error}")
    for label, status, _, error in job_results:
        if status == "failed":
            log(f"  ошибка: {label} — {error}")
    writers = [p for p in all_procs if p.static_writes]
    total, hit = sum(p.static_writes for p in writers), sum(p.qs_writes for p in writers)
    if total:
        log(f"\n=== Покрытие: операторов записи в коде процедур {total}, попали в Query Store {hit} "
            f"({hit / total:.0%}); процедур с полным покрытием {sum(p.qs_writes == p.static_writes for p in writers)} "
            f"из {len(writers)}")
        low = sorted((p for p in writers if p.qs_writes < p.static_writes),
                     key=lambda p: (p.qs_writes / p.static_writes, p.fqn))
        for p in low[:20]:
            log(f"  {p.qs_writes:3}/{p.static_writes:<3} {p.db}.{p.fqn}" + (f" — {p.error}" if p.error else ""))
        if len(low) > 20:
            log(f"  … ещё {len(low) - 20} процедур (полный список — в --report)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
