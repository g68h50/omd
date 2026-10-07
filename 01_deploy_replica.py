"""Накатывает реплику кода боевого стенда на тестовый SQL Server.

Структура реплики:

    <root>/<база>/<схема>/<категория>/*.sql      категория: tables, views, procedures, ...
                                                 (на любом уровне может лежать readme.md — здесь не используется)

Порядок: базы и схемы создаются, затем файлы выполняются по категориям
(типы → последовательности → таблицы → функции → синонимы → представления → процедуры → триггеры).
Файлы, упавшие из-за ещё не созданных зависимостей (представление на представление, внешний ключ
на таблицу другой схемы, ссылки между базами), повторяются следующими проходами, пока есть прогресс.

Повторный запуск безопасен: CREATE PROCEDURE/VIEW/FUNCTION/TRIGGER выполняется как CREATE OR ALTER,
ошибки «объект уже существует» у таблиц, индексов и ограничений считаются статусом exists.

Зависимости: только pyodbc и ODBC Driver 17/18 for SQL Server.

Параметры по умолчанию — блок «Параметры по умолчанию» ниже (тестовый стенд). Приоритет:
аргумент командной строки → переменная окружения (MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD) → умолчание.

    python 01_deploy_replica.py                                        # реплика <корень проекта>/prod_repl
    python 01_deploy_replica.py --dry-run                              # только порядок выполнения
    python 01_deploy_replica.py D:/replica --databases DWH_BCS --report deploy.csv

Код возврата: 0 — все файлы выполнены, 1 — есть ошибки, 2 — ошибка запуска.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------- параметры по умолчанию (тестовый стенд)

DEFAULT_ROOT = Path(__file__).resolve().parents[1] / "prod_repl"   # <корень проекта>/prod_repl
DEFAULT_SERVER = "127.0.0.1"
DEFAULT_PORT = "1433"
DEFAULT_USER = "sa"
DEFAULT_PASSWORD = 'YourStrong_Password123!'
DEFAULT_DRIVER = None              # None — новейший установленный ODBC Driver NN for SQL Server
DEFAULT_MAX_PASSES = 5
DEFAULT_TIMEOUT = 600              # таймаут пакета, с

# категория -> (порядок, имена папок в нижнем регистре)
CATEGORIES = {
    "types": (0, {"types", "type", "user defined types", "типы"}),
    "sequences": (1, {"sequences", "sequence", "последовательности"}),
    "tables": (2, {"tables", "table", "таблицы"}),
    "functions": (3, {"functions", "function", "функции"}),
    "synonyms": (4, {"synonyms", "synonym", "синонимы"}),
    "views": (5, {"views", "view", "представления"}),
    "procedures": (6, {"procedures", "procedure", "procs", "stored procedures", "storedprocedures", "процедуры"}),
    "triggers": (7, {"triggers", "trigger", "триггеры"}),
}
FOLDER_TO_CATEGORY = {name: cat for cat, (_, names) in CATEGORIES.items() for name in names}
MODULE_CATEGORIES = {"functions", "views", "procedures", "triggers"}

# Скрипты SSMS начинаются с SET ANSI_NULLS / QUOTED_IDENTIFIER объекта, иногда OFF. Для таблиц и индексов это ломает
# создание индексов на вычисляемых колонках, фильтрованных, XML- и пространственных индексов (ошибки 1934/1935),
# а настройка сохраняется в таблице — повторный проход её не исправит. Поэтому перед каждым пакетом таблиц, типов,
# последовательностей и синонимов выставляются обязательные настройки. Модули (процедуры, функции, представления,
# триггеры) выполняются с настройками из файла: SQL Server хранит их вместе с кодом, и код может от них зависеть
# (строки в двойных кавычках при QUOTED_IDENTIFIER OFF).
REQUIRED_SET_OPTIONS = ("SET ANSI_NULLS, QUOTED_IDENTIFIER, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, "
                        "CONCAT_NULL_YIELDS_NULL ON; SET NUMERIC_ROUNDABORT OFF;")
SET_ONLY_BATCH = re.compile(r"^\s*(SET\s+(ANSI_NULLS|QUOTED_IDENTIFIER|ANSI_PADDING|ANSI_WARNINGS|ARITHABORT|"
                            r"CONCAT_NULL_YIELDS_NULL|NUMERIC_ROUNDABORT)\s+(ON|OFF)\s*;?\s*)+$", re.I)
SYSTEM_DATABASES = {"master", "model", "msdb", "tempdb"}

# ошибки «уже существует»: объект, индекс, ограничение, колонка, тип, схема
EXISTS_ERRORS = {2714, 1913, 1781, 2705, 2715, 2759, 1779, 15233}
CREATE_MODULE = re.compile(r"CREATE\s+(PROCEDURE|PROC|VIEW|FUNCTION|TRIGGER)\b", re.I)
ERROR_NUMBER = re.compile(r"\((\d{3,6})\)")


# --------------------------------------------------------------------------- чтение и разбор файлов

def read_sql(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8")
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1251")


GO_LINE = re.compile(r"^\s*GO(?:\s+(\d+))?\s*(?:--.*)?$", re.I)


def split_batches(sql: str) -> list[str]:
    """Делит скрипт по строкам GO вне комментариев и строк; GO N повторяет пакет N раз."""
    batches, current = [], []
    depth, in_string = 0, False
    for line in sql.splitlines():
        if depth == 0 and not in_string:
            m = GO_LINE.match(line)
            if m:
                batch = "\n".join(current).strip()
                if batch:
                    batches.extend([batch] * int(m.group(1) or 1))
                current = []
                continue
        current.append(line)
        i = 0
        while i < len(line):
            two = line[i:i + 2]
            if in_string:
                if line[i] == "'":
                    in_string = False
            elif depth:
                if two == "*/":
                    depth, i = depth - 1, i + 1
                elif two == "/*":
                    depth, i = depth + 1, i + 1
            elif two == "--":
                break
            elif two == "/*":
                depth, i = 1, i + 1
            elif line[i] == "'":
                in_string = True
            i += 1
    batch = "\n".join(current).strip()
    if batch:
        batches.append(batch)
    return batches


def skip_leading_comments(sql: str) -> int:
    i = 0
    while i < len(sql):
        if sql[i].isspace():
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j < 0 else j + 1
        elif sql.startswith("/*", i):
            j = sql.find("*/", i)
            i = len(sql) if j < 0 else j + 2
        else:
            break
    return i


def create_or_alter(batch: str) -> str:
    """CREATE PROC/VIEW/FUNCTION/TRIGGER в начале пакета -> CREATE OR ALTER (повторный запуск обновляет код)."""
    i = skip_leading_comments(batch)
    m = CREATE_MODULE.match(batch, i)
    if not m:
        return batch
    return batch[:i] + "CREATE OR ALTER " + m.group(1) + batch[m.end():]


# --------------------------------------------------------------------------- обход реплики

@dataclass
class Item:
    database: str
    schema: str
    category: str
    path: Path
    status: str = "pending"          # pending | ok | exists | failed
    error: str = ""
    attempts: int = 0
    seconds: float = 0.0

    @property
    def order(self) -> tuple:
        return CATEGORIES[self.category][0], self.database.lower(), self.schema.lower(), str(self.path).lower()


@dataclass
class Scan:
    items: list[Item] = field(default_factory=list)
    databases: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    readme: int = 0
    unknown: list[str] = field(default_factory=list)


def scan(root: Path, only: set[str] | None) -> Scan:
    result = Scan()
    for db_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if only and db_dir.name.lower() not in only:
            continue
        if db_dir.name.lower() in SYSTEM_DATABASES:
            result.unknown.append(f"{db_dir.name}: системная база пропущена")
            continue
        result.databases[db_dir.name]
        for path in sorted(db_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(db_dir).parts
            if path.name.lower() == "readme.md":
                result.readme += 1
                continue
            if path.suffix.lower() != ".sql":
                result.unknown.append(f"{path.relative_to(root)}: не .sql")
                continue
            if len(rel) < 3:
                result.unknown.append(f"{path.relative_to(root)}: файл вне папки <схема>/<категория>")
                continue
            schema, folder = rel[0], rel[1].lower()
            category = FOLDER_TO_CATEGORY.get(folder)
            if category is None:
                result.unknown.append(f"{path.relative_to(root)}: неизвестная категория «{rel[1]}»")
                continue
            result.databases[db_dir.name].add(schema)
            result.items.append(Item(db_dir.name, schema, category, path))
    result.items.sort(key=lambda it: it.order)
    return result


# --------------------------------------------------------------------------- выполнение

def pick_driver(pyodbc, wanted: str | None) -> str:
    if wanted:
        return wanted
    found = sorted((d for d in pyodbc.drivers() if re.fullmatch(r"ODBC Driver \d+ for SQL Server", d)),
                   key=lambda d: int(re.search(r"\d+", d).group()))
    if not found:
        sys.exit("Не найден ODBC Driver for SQL Server; укажите --driver")
    return found[-1]


def datetimeoffset_value(raw: bytes):
    """datetimeoffset (тип ODBC -155) -> datetime с часовым поясом (pyodbc не читает его сам)."""
    import datetime
    import struct
    y, mo, d, h, mi, s, ns, oh, om = struct.unpack("<6hI2h", raw)
    tz = datetime.timezone(datetime.timedelta(hours=oh, minutes=om))
    return datetime.datetime(y, mo, d, h, mi, s, ns // 1000, tzinfo=tz)


class Server:
    def __init__(self, args):
        import pyodbc
        self.pyodbc = pyodbc
        driver = pick_driver(pyodbc, args.driver)
        parts = [f"DRIVER={{{driver}}}", f"SERVER={args.server},{args.port}", "TrustServerCertificate=yes",
                 "MARS_Connection=yes"]   # иначе «Connection is busy with results for another command»
        if args.trusted:
            parts.append("Trusted_Connection=yes")
        else:
            password = os.environ.get(args.password_env) or DEFAULT_PASSWORD
            if not password:
                sys.exit(f"Пароль не задан: DEFAULT_PASSWORD, переменная {args.password_env} или --trusted")
            parts += [f"UID={args.user}", f"PWD={password}"]
        self.base = ";".join(parts)
        self.timeout = args.timeout
        self.connections = {}
        self.driver = driver

    def conn(self, database: str):
        if database not in self.connections:
            c = self.pyodbc.connect(self.base + f";DATABASE={database}", autocommit=True)
            c.timeout = self.timeout
            c.add_output_converter(-155, datetimeoffset_value)     # datetimeoffset: pyodbc не читает его сам
            self.connections[database] = c
        return self.connections[database]

    def run(self, database: str, sql: str, params=()):
        cur = self.conn(database).cursor()
        cur.execute(sql, params)
        rows = None
        while True:
            if cur.description is not None and rows is None:
                rows = cur.fetchall()
            if not cur.nextset():
                break
        return rows

    def reset(self, database: str):
        c = self.connections.pop(database, None)
        if c is not None:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    def close(self):
        for db in list(self.connections):
            self.reset(db)


def error_numbers(exc: Exception) -> set[int]:
    return {int(n) for n in ERROR_NUMBER.findall(str(exc))}


def short(exc: Exception) -> str:
    text = str(exc)
    found = re.findall(r"\[SQL Server\](.*?)(?=\s*\(SQLExecDirectW\)|;\s*\[\w+\]\s*\[Microsoft\]|[\"']\)?$)", text)
    text = "; ".join(s.strip() for s in found) if found else text
    return text.replace("\r", " ").replace("\n", " ").strip()[:400]


def prepare_databases(srv: Server, found: Scan, recreate: bool, log) -> None:
    for db, schemas in found.databases.items():
        exists = srv.run("master", "SELECT 1 FROM sys.databases WHERE name = ?", (db,))
        if exists and recreate:
            log(f"[база] {db}: удаление (--recreate)")
            srv.run("master", f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            srv.run("master", f"DROP DATABASE [{db}]")
            exists = None
        if not exists:
            srv.run("master", f"CREATE DATABASE [{db}]")
            log(f"[база] {db}: создана")
        else:
            log(f"[база] {db}: существует")
        for schema in sorted(schemas):
            srv.run(db, "DECLARE @s sysname = ?, @q nvarchar(300); "
                        "IF SCHEMA_ID(@s) IS NULL BEGIN SET @q = N'CREATE SCHEMA ' + QUOTENAME(@s); EXEC(@q); END",
                    (schema,))


def execute_item(srv: Server, item: Item) -> None:
    started = time.perf_counter()
    item.attempts += 1
    batches = split_batches(read_sql(item.path))
    existed, skipped, errors = 0, 0, []
    module = item.category in MODULE_CATEGORIES
    for batch in batches:
        if not module and SET_ONLY_BATCH.match(batch):
            skipped += 1                  # SET ... OFF из скрипта таблицы не применяется (см. REQUIRED_SET_OPTIONS)
            continue
        sql = create_or_alter(batch) if module else batch
        try:
            if not module:
                srv.run(item.database, REQUIRED_SET_OPTIONS)
            srv.run(item.database, sql)
        except srv.pyodbc.Error as exc:
            if error_numbers(exc) & EXISTS_ERRORS:
                existed += 1
                continue
            errors.append(short(exc))
            # ошибка могла оставить USE другой базы или открытую транзакцию — начинаем с чистого соединения
            srv.reset(item.database)
    item.seconds += time.perf_counter() - started
    if errors:
        item.status, item.error = "failed", errors[0]
    else:
        item.status, item.error = ("exists" if existed and existed + skipped == len(batches) else "ok"), ""
    # USE внутри файла меняет контекст соединения — следующий файл начинает в своей базе
    srv.reset(item.database)


def deploy(srv: Server, items: list[Item], max_passes: int, log) -> None:
    pending = list(items)
    for n in range(1, max_passes + 1):
        log(f"--- проход {n}: файлов {len(pending)}")
        for item in pending:
            execute_item(srv, item)
            mark = {"ok": "OK", "exists": "EXISTS", "failed": "FAIL"}[item.status]
            log(f"{mark:6} {item.database}.{item.schema} {item.category}/{item.path.name}"
                + (f" — {item.error}" if item.error else ""))
        failed = [it for it in pending if it.status == "failed"]
        if not failed or len(failed) == len(pending):
            break
        pending = failed


def summary(found: Scan, root: Path, log) -> int:
    by = defaultdict(Counter)
    for it in found.items:
        by[(it.database, it.category)][it.status] += 1
    log("\n=== Итог")
    log(f"{'база':24} {'категория':12} {'ok':>5} {'exists':>7} {'failed':>7}")
    for (db, cat), c in sorted(by.items(), key=lambda kv: (kv[0][0].lower(), CATEGORIES[kv[0][1]][0])):
        log(f"{db:24} {cat:12} {c['ok']:5} {c['exists']:7} {c['failed']:7}")
    failed = [it for it in found.items if it.status == "failed"]
    log(f"Файлов: {len(found.items)}; ошибок: {len(failed)}; readme.md: {found.readme}; "
        f"не обработано: {len(found.unknown)}")
    for u in found.unknown:
        log(f"  пропущено: {u}")
    for it in failed:
        log(f"  ошибка: {it.path.relative_to(root)} — {it.error}")
    return 1 if failed else 0


def write_report(path: Path, items: list[Item]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["database", "schema", "category", "file", "status", "attempts", "seconds", "error"])
        for it in items:
            w.writerow([it.database, it.schema, it.category, str(it.path), it.status, it.attempts,
                        f"{it.seconds:.2f}", it.error])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path, nargs="?", default=DEFAULT_ROOT,
                    help=f"корень реплики: <база>/<схема>/<категория>/*.sql (по умолчанию {DEFAULT_ROOT})")
    ap.add_argument("--server", default=os.getenv("MSSQL_HOST", DEFAULT_SERVER))
    ap.add_argument("--port", default=os.getenv("MSSQL_PORT", DEFAULT_PORT))
    ap.add_argument("--user", default=os.getenv("MSSQL_USER", DEFAULT_USER))
    ap.add_argument("--password-env", default="MSSQL_PASSWORD",
                    help="переменная окружения с паролем; если не задана — пароль по умолчанию")
    ap.add_argument("--trusted", action="store_true", help="Windows-аутентификация")
    ap.add_argument("--driver", default=DEFAULT_DRIVER, help="имя ODBC-драйвера (по умолчанию — новейший установленный)")
    ap.add_argument("--databases", nargs="+", help="только эти базы (имена папок)")
    ap.add_argument("--recreate", action="store_true", help="удалить и создать заново базы из реплики (только тестовый стенд)")
    ap.add_argument("--max-passes", type=int, default=DEFAULT_MAX_PASSES, help="проходов для файлов с неготовыми зависимостями")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="таймаут пакета, с")
    ap.add_argument("--report", type=Path, help="CSV с результатом по каждому файлу")
    ap.add_argument("--dry-run", action="store_true", help="показать порядок выполнения без подключения")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    log = lambda s: print(s, flush=True)  # noqa: E731
    if not args.root.is_dir():
        log(f"Нет папки реплики: {args.root}")
        return 2
    found = scan(args.root, {d.lower() for d in args.databases} if args.databases else None)
    log(f"Реплика {args.root}: баз {len(found.databases)}, файлов {len(found.items)}, readme.md {found.readme}")
    if args.dry_run:
        for it in found.items:
            log(f"{it.category:11} {it.database}.{it.schema} {it.path.relative_to(args.root)}")
        for u in found.unknown:
            log(f"пропущено: {u}")
        return 0

    srv = Server(args)
    log(f"Сервер {args.server},{args.port}, драйвер {srv.driver}")
    try:
        try:
            srv.conn("master")
        except srv.pyodbc.Error as exc:
            log(f"Нет подключения к {args.server},{args.port}: {short(exc)}")
            return 2
        prepare_databases(srv, found, args.recreate, log)
        deploy(srv, found.items, args.max_passes, log)
    finally:
        srv.close()
    if args.report:
        write_report(args.report, found.items)
        log(f"Отчёт: {args.report}")
    return summary(found, args.root, log)


if __name__ == "__main__":
    sys.exit(main())
