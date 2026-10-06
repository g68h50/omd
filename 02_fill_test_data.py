"""Заполняет таблицы баз реплики синтетическими тестовыми данными.

Структура таблиц читается с сервера (каталог SQL Server), поэтому скрипт работает с любыми базами,
накатанными 01_deploy_replica.py. Что учитывается:

  - внешние ключи: таблицы заполняются в порядке зависимостей, значения ключей берутся из родителя
    (в том числе IDENTITY); самоссылки и циклы — вставка с NULL / отключённым ключом и дозаполнение UPDATE;
  - PRIMARY KEY, UNIQUE-ограничения и уникальные индексы: значения не повторяются;
  - CHECK: списки значений (IN / OR), числовые и датовые границы, сравнение двух колонок, IS NOT NULL;
    строки, которые всё равно нарушают ограничения, отбрасываются и попадают в итог;
  - типы SQL Server с длиной, точностью и масштабом; IDENTITY, вычисляемые колонки, rowversion
    и колонки периода не заполняются;
  - смысл колонки по имени: ФИО, email, телефон, ИНН, счёт, валюта, город, статус, суммы, ставки, даты
    начала/окончания (окончание не раньше начала), ключи дат ГГГГММДД; календарь — на весь период данных.

Подсказки из кода процедур, функций, представлений и триггеров (данные с прода недоступны, --no-code-hints — выкл.):
  - значения, с которыми код сравнивает колонку (is_active = 1, status IN ('OK','ERROR')), — в большинстве строк,
    чтобы фильтры находили строки и ветки выполнялись; в ключах — реже;
  - связи из условий JOIN и одноимённые ключи: общий набор значений, как при внешнем ключе
    (справочник заполняется раньше и получает значения из кода первыми строками);
  - строковые колонки, которые код приводит к дате или числу (TRY_CONVERT(DATE, x, 104)), — строки в этом формате;
  - числа, которые код читает как дату (CONVERT(CHAR(8), batch_id), 112), — в формате ГГГГММДД;
  - допустимые значения CHECK — на одноимённые колонки предыдущих слоёв (raw.trx_type -> fact.trx_type);
  - колонки с именами таблиц и схем (метаданные загрузчиков) — имена существующих объектов.

По умолчанию заполняются только пустые таблицы; --clean сначала очищает выбранные таблицы.
Данные воспроизводимы: одинаковый --seed даёт одинаковые значения.

    python 02_fill_test_data.py                          # базы из <корень проекта>/prod_repl, 100 строк на таблицу
    python 02_fill_test_data.py --rows 1000 --clean
    python 02_fill_test_data.py --databases DWH_BCS --rows-table DWH.fact_transaction=5000 --dry-run

Код возврата: 0 — все таблицы заполнены, 1 — есть таблицы с ошибками, 2 — ошибка запуска.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import re
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

# --------------------------------------------------------------------------- параметры по умолчанию (тестовый стенд)

DEFAULT_ROOT = Path(__file__).resolve().parents[1] / "prod_repl"   # <корень проекта>/prod_repl: имена баз = папки
DEFAULT_SERVER = "127.0.0.1"
DEFAULT_PORT = "1433"
DEFAULT_USER = "sa"
DEFAULT_PASSWORD = 'YourStrong_Password123!'
DEFAULT_DRIVER = None              # None — новейший установленный ODBC Driver NN for SQL Server
DEFAULT_ROWS = 100                 # строк на таблицу
DEFAULT_SEED = 42
DEFAULT_NULL_RATE = 0.0            # доля NULL в необязательных колонках: 0 — NULL из сырых колонок ломает
                                   # INSERT ... SELECT в обязательные колонки следующих слоёв, процедуры падают
DEFAULT_TIMEOUT = 600
DATE_FROM, DATE_TO = dt.date(2020, 1, 1), dt.date(2026, 9, 30)

SYSTEM_DATABASES = {"master", "model", "msdb", "tempdb"}
BATCH = 500
LITERAL_RATE = 0.7                 # доля строк со значением из кода (t.is_active = 1 -> 70% строк активны)
KEY_LITERAL_RATE = 0.15            # то же для ключей и связанных колонок (batch_id = 20260601 — не в каждой строке)
DOMAIN_RATE = 0.9                  # доля строк со значением из связанной колонки (неявный внешний ключ)

# --------------------------------------------------------------------------- словари для правдоподобных значений

LAST_M = ["Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев", "Петров", "Соколов", "Михайлов", "Новиков", "Фёдоров",
          "Морозов", "Волков", "Алексеев", "Лебедев", "Семёнов", "Егоров", "Павлов", "Козлов", "Степанов", "Николаев"]
FIRST_M = ["Александр", "Дмитрий", "Максим", "Сергей", "Андрей", "Алексей", "Артём", "Илья", "Кирилл", "Михаил"]
FIRST_F = ["Анна", "Мария", "Елена", "Ольга", "Наталья", "Екатерина", "Татьяна", "Ирина", "Светлана", "Юлия"]
MIDDLE_M = ["Александрович", "Дмитриевич", "Сергеевич", "Андреевич", "Алексеевич", "Михайлович", "Игоревич"]
MIDDLE_F = ["Александровна", "Дмитриевна", "Сергеевна", "Андреевна", "Алексеевна", "Михайловна", "Игоревна"]
CITIES = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань", "Нижний Новгород", "Самара",
          "Ростов-на-Дону", "Краснодар", "Уфа", "Воронеж", "Пермь"]
STREETS = ["ул. Ленина", "пр. Мира", "ул. Гагарина", "ул. Советская", "Невский пр.", "ул. Пушкина", "ул. Садовая"]
WORDS = ["отчёт", "клиент", "договор", "операция", "перевод", "счёт", "заявка", "звонок", "встреча", "платёж",
         "проверка", "консультация", "продукт", "остаток", "портфель"]
CURRENCIES = ["RUB", "USD", "EUR", "CNY", "GBP", "CHF", "JPY", "KZT", "BYN", "TRY"]
COUNTRIES2 = ["RU", "KZ", "BY", "AM", "CN", "TR", "AE", "US", "DE", "GB"]
COUNTRIES = ["Россия", "Казахстан", "Беларусь", "Армения", "Китай", "Турция", "ОАЭ"]
STATUSES = ["ACTIVE", "CLOSED", "BLOCKED", "PENDING", "NEW"]
CHANNELS = ["CALL", "EMAIL", "SMS", "VISIT", "CHAT", "PUSH"]
SEGMENTS = ["MASS", "AFFLUENT", "PRIVATE", "VIP", "SME"]

TRANSLIT = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i", "й": "y",
    "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f",
    "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
})


def translit(s: str) -> str:
    out = []
    for ch in s:
        t = ch.lower().translate(TRANSLIT)
        out.append(t.capitalize() if ch.isupper() and t else t)
    return "".join(out)


# --------------------------------------------------------------------------- модель таблиц

INT_RANGE = {"tinyint": (0, 255), "smallint": (-32768, 32767), "int": (-2**31, 2**31 - 1),
             "bigint": (-2**63, 2**63 - 1)}
STRING_TYPES = {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "sysname"}
UNICODE_TYPES = {"nchar", "nvarchar", "ntext", "sysname"}
DATE_TYPES = {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"}
NUMERIC_TYPES = {"decimal", "numeric", "money", "smallmoney", "float", "real"}
SKIP_TYPES = {"timestamp", "rowversion"}
CLR_TYPES = {"hierarchyid", "geography", "geometry"}
SLOW_TYPES = {"xml", "text", "ntext", "image", "sql_variant"} | CLR_TYPES

START_TOKENS = ("from", "start", "begin", "open", "beg", "valid_from", "effective")
END_TOKENS = ("to", "end", "close", "till", "finish", "expire", "valid_to")


@dataclass
class Column:
    id: int
    name: str
    type: str
    length: int | None          # символов/байт; None — MAX или не применимо
    precision: int
    scale: int
    nullable: bool
    identity: bool
    computed: bool
    generated: bool
    choices: list | None = None  # из CHECK
    lo: object = None            # границы из CHECK
    hi: object = None
    lo_strict: bool = False
    hi_strict: bool = False
    required: bool = False       # CHECK ... IS NOT NULL
    month_start: bool = False    # CHECK DATEPART(day, col) = 1
    literals: list | None = None  # значения, с которыми колонку сравнивает код процедур и представлений
    domain: int | None = None     # общий набор значений со связанными колонками других таблиц
    cast_to: tuple | None = None  # строковая колонка, которую код приводит к типу: ("date", стиль) / ("number", None)
    fixed_values: list | None = None  # значения только из списка: имена таблиц (метаданные загрузчиков)
                                      # или допустимые значения CHECK одноимённой колонки следующего слоя

    yyyymmdd: bool = False        # код читает число как дату: CONVERT(DATE, CONVERT(CHAR(8), batch_id), 112)

    @property
    def date_key(self) -> bool:
        """Целочисленный ключ даты ГГГГММДД (date_key, dt_id, day_sk) — так его строит CONVERT(..., 112)."""
        return self.type in ("int", "bigint") and (self.yyyymmdd or bool(DATE_KEY.search(self.name)))

    @property
    def literal_rate(self) -> float:
        """Ключи получают значения из кода редко (иначе защита от повторной загрузки срабатывает почти всегда)."""
        return KEY_LITERAL_RATE if self.domain is not None or KEY_NAME.match(self.name) else LITERAL_RATE

    @property
    def insertable(self) -> bool:
        return not (self.identity or self.computed or self.generated or self.type in SKIP_TYPES)


@dataclass
class ForeignKey:
    name: str
    parent: int                  # object_id таблицы-ребёнка
    referenced: int
    columns: list[str]
    ref_columns: list[str]
    disabled: bool
    deferred: bool = False       # самоссылка или цикл: дозаполняется после вставки
    nocheck: bool = False        # отключён на время вставки

    def nullable_in(self, table: "Table") -> bool:
        return all(table.col(c).nullable for c in self.columns)


@dataclass
class Table:
    db: str
    id: int
    schema: str
    name: str
    rows_now: int
    columns: list[Column] = field(default_factory=list)
    unique_sets: list[tuple[str, ...]] = field(default_factory=list)
    fks: list[ForeignKey] = field(default_factory=list)
    comparisons: list[tuple[str, str, str]] = field(default_factory=list)   # (a, op, b) из CHECK
    skip_reason: str = ""
    # результат
    status: str = "pending"
    inserted: int = 0
    rejected: int = 0
    error: str = ""
    seconds: float = 0.0

    @property
    def generated_unique_sets(self) -> list[tuple[str, ...]]:
        """Уникальные наборы, значения которых задаёт скрипт (наборы с IDENTITY уникальны сами по себе)."""
        return [s for s in self.unique_sets if all(self.col(c).insertable for c in s)]

    def find(self, name: str) -> Column | None:
        return self._by_lower.get(name.lower())

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def sql_name(self) -> str:
        return f"[{self.schema.replace(']', ']]')}].[{self.name.replace(']', ']]')}]"

    def col(self, name: str) -> Column:
        return self._by_name[name]

    def index(self) -> None:
        self._by_name = {c.name: c for c in self.columns}
        self._by_lower = {c.name.lower(): c for c in self.columns}


def q(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


# --------------------------------------------------------------------------- чтение каталога

TABLES_SQL = """
SELECT t.object_id, s.name, t.name,
       (SELECT ISNULL(SUM(p.rows), 0) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)),
       t.temporal_type, t.is_node, t.is_edge, t.is_external, t.is_filetable
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
"""
COLUMNS_SQL = """
SELECT c.object_id, c.column_id, c.name,
       CASE WHEN ut.is_assembly_type = 1 THEN ut.name ELSE TYPE_NAME(c.system_type_id) END,
       c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, c.is_computed, c.generated_always_type
FROM sys.columns c
JOIN sys.types ut ON ut.user_type_id = c.user_type_id
JOIN sys.tables t ON t.object_id = c.object_id AND t.is_ms_shipped = 0
ORDER BY c.object_id, c.column_id
"""
UNIQUE_SQL = """
SELECT i.object_id, i.index_id, c.name
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.tables t ON t.object_id = i.object_id AND t.is_ms_shipped = 0
WHERE i.is_unique = 1 AND i.is_hypothetical = 0 AND i.is_disabled = 0
ORDER BY i.object_id, i.index_id, ic.key_ordinal
"""
FK_SQL = """
SELECT fk.object_id, fk.name, fk.parent_object_id, fk.referenced_object_id, fk.is_disabled, pc.name, rc.name
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
ORDER BY fk.object_id, fkc.constraint_column_id
"""
CHECK_SQL = "SELECT parent_object_id, definition FROM sys.check_constraints WHERE is_disabled = 0"


def load_tables(srv, db: str) -> dict[int, Table]:
    tables: dict[int, Table] = {}
    for oid, schema, name, rows, temporal, node, edge, external, filetable in srv.rows(db, TABLES_SQL):
        t = Table(db, oid, schema, name, int(rows))
        if temporal == 1:
            t.skip_reason = "таблица истории (temporal)"
        elif node or edge:
            t.skip_reason = "графовая таблица"
        elif external:
            t.skip_reason = "внешняя таблица"
        elif filetable:
            t.skip_reason = "FileTable"
        tables[oid] = t
    for oid, cid, name, typ, max_len, prec, scale, nullable, ident, comp, gen in srv.rows(db, COLUMNS_SQL):
        if oid not in tables:
            continue
        typ = typ.lower()
        length = None
        if typ in STRING_TYPES | {"binary", "varbinary"} and max_len != -1 and typ not in ("text", "ntext"):
            length = max_len // 2 if typ in UNICODE_TYPES else max_len
        tables[oid].columns.append(Column(cid, name, typ, length, prec, scale, bool(nullable), bool(ident),
                                          bool(comp), bool(gen)))
    for t in tables.values():
        t.index()
    sets = defaultdict(list)
    for oid, iid, col in srv.rows(db, UNIQUE_SQL):
        if oid in tables:
            sets[(oid, iid)].append(col)
    for (oid, _), cols in sets.items():
        tables[oid].unique_sets.append(tuple(cols))
    fks: dict[int, ForeignKey] = {}
    for fid, name, parent, ref, disabled, pcol, rcol in srv.rows(db, FK_SQL):
        if fid not in fks:
            fks[fid] = ForeignKey(name, parent, ref, [], [], bool(disabled))
        fks[fid].columns.append(pcol)
        fks[fid].ref_columns.append(rcol)
    for fk in fks.values():
        if fk.parent in tables and not fk.disabled:
            tables[fk.parent].fks.append(fk)
    for oid, definition in srv.rows(db, CHECK_SQL):
        if oid in tables:
            apply_check(tables[oid], definition)
    return tables


# --------------------------------------------------------------------------- разбор CHECK

LITERAL = r"\(*\s*(N?'(?:[^']|'')*'|-?\d+(?:\.\d+)?)\s*\)*"


def literal(text: str):
    text = text.strip()
    if text.upper().startswith("N'"):
        text = text[1:]
    if text.startswith("'"):
        return text[1:-1].replace("''", "'")
    return Decimal(text)


def apply_check(table: Table, definition: str) -> None:
    cols = set(re.findall(r"\[([^\]]+)\]", definition))
    cols = {c for c in cols if c in table._by_name}
    if len(cols) == 2:
        m = re.search(r"\[([^\]]+)\]\s*(<=|<|>=|>)\s*\[([^\]]+)\]", definition)
        if m:
            table.comparisons.append((m.group(1), m.group(2), m.group(3)))
        return
    if len(cols) != 1:
        return
    name = cols.pop()
    col = table.col(name)
    qn = re.escape(f"[{name}]")
    if re.search(qn + r"\s+IS\s+NOT\s+NULL", definition, re.I):
        col.required = True
    if re.search(r"(datepart\s*\(\s*(day|dd|d)\s*,\s*" + qn + r"\s*\)|\bday\s*\(\s*" + qn + r"\s*\))\s*=\s*\(?\s*1\s*\)?",
                 definition, re.I):
        col.month_start = True
        return
    eq = re.findall(qn + r"\s*=\s*" + LITERAL, definition)
    rest = re.sub(qn + r"\s*=\s*" + LITERAL, "", definition)
    if eq and not re.sub(r"(?i)[\s()]|\bOR\b", "", rest):
        col.choices = [literal(v) for v in eq]
        return
    for op, val in re.findall(qn + r"\s*(>=|<=|>|<)\s*" + LITERAL, definition):
        bound(col, op, literal(val))
    for val, op in re.findall(LITERAL + r"\s*(>=|<=|>|<)\s*" + qn, definition):
        bound(col, {">=": "<=", "<=": ">=", ">": "<", "<": ">"}[op], literal(val))
    m = re.search(qn + r"\s+BETWEEN\s+" + LITERAL + r"\s+AND\s+" + LITERAL, definition, re.I)
    if m:
        bound(col, ">=", literal(m.group(1)))
        bound(col, "<=", literal(m.group(2)))


def bound(col: Column, op: str, value) -> None:
    if isinstance(value, str):
        try:
            value = dt.date.fromisoformat(value[:10])
        except ValueError:
            return
    if op in (">=", ">"):
        col.lo, col.lo_strict = value, op == ">"
    else:
        col.hi, col.hi_strict = value, op == "<"


# --------------------------------------------------------------------------- подсказки из кода
#
# Данные с прода недоступны, поэтому правдоподобные значения выводятся из кода процедур, функций,
# представлений и триггеров:
#   - литералы, с которыми код сравнивает колонку (t.is_active = 1, status IN ('OK','ERROR'), SET x = 'Y'):
#     колонка получает их в большинстве строк — фильтры WHERE находят строки, ветки IF/CASE выполняются;
#   - связи из условий JOIN (a.client_id = b.client_id) и одинаковые имена ключевых колонок: связанные
#     колонки получают общий набор значений, как при объявленном внешнем ключе.
# Разбирается и текст динамического SQL внутри строковых литералов.

MODULES_SQL = """
SELECT s.name, m.definition
FROM sys.sql_modules m JOIN sys.objects o ON o.object_id = m.object_id JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0 AND o.type IN ('P', 'V', 'FN', 'IF', 'TF', 'TR')
"""
NAME = r"(?:\[[^\]]+\]|\w+)"
TABLE_REF = re.compile(r"\b(?:FROM|JOIN|UPDATE|INTO|USING|MERGE)\s+(" + NAME + r"(?:\s*\.\s*" + NAME + r"){0,2})"
                       r"(?:\s+(?:AS\s+)?(\w+))?", re.I)
NOT_ALIAS = {"on", "where", "set", "join", "inner", "left", "right", "full", "cross", "outer", "with", "group", "order",
             "using", "when", "values", "select", "as", "output", "option", "union", "except", "intersect", "pivot",
             "unpivot", "apply", "having", "exec", "execute", "insert", "update", "delete", "merge", "into", "from",
             "and", "or", "then", "else", "end", "top", "go", "begin", "if", "while", "declare", "return", "default"}
VALUE = r"(N?'(?:[^']|'')*'|-?\d+(?:\.\d+)?)"
COLREF = r"(?<![@\w.])(?:(\w+)\s*\.\s*)?\[?(\w+)\]?"
EQ_LITERAL = re.compile(COLREF + r"\s*=\s*" + VALUE)
REV_LITERAL = re.compile(r"(?<![\w'])" + VALUE + r"\s*=\s*" + COLREF)
IN_LIST = re.compile(COLREF + r"\s+IN\s*\(([^()]*)\)", re.I)
JOIN_EQ = re.compile(r"(?<![@\w.])(\w+)\s*\.\s*\[?(\w+)\]?\s*=\s*(\w+)\s*\.\s*\[?(\w+)\]?")
CAST_TYPES = r"(date|datetime2?|smalldatetime|int|bigint|smallint|decimal|numeric|money|float)"
WRAP = r"(?:\w+\s*\(\s*)*"          # REPLACE(TRIM(x)...) вокруг колонки
CAST_AS = re.compile(r"\b(?:TRY_)?CAST\s*\(\s*" + WRAP + COLREF + r"[^()]*?\bAS\s+" + CAST_TYPES, re.I)
CONVERT_TO = re.compile(r"\b(?:TRY_)?CONVERT\s*\(\s*" + CAST_TYPES + r"(?:\s*\([^)]*\))?\s*,\s*" + WRAP + COLREF
                        + r"[^,()]*(?:\)\s*)*(?:,\s*(\d+))?", re.I)
KEY_NAME = re.compile(r".+_(id|code|key|sk|hk|no|num|number|cd)$", re.I)
DATE_KEY = re.compile(r"(^|_)(date|dt|day|calendar)_?(key|id|sk)$", re.I)
NUMBER_AS_DATE = re.compile(r"CONVERT\s*\(\s*N?(?:VAR)?CHAR\s*\(\s*8\s*\)\s*,\s*@?(?:\w+\s*\.\s*)?\[?(\w+)\]?\s*\)", re.I)
OBJECT_COL = re.compile(r"(^|_)(table|tbl|view|object)(_?name)?($|_)", re.I)
SCHEMA_COL = re.compile(r"(^|_)schema(_?name)?$", re.I)


def type_family(col: Column) -> str:
    if col.type in INT_RANGE:
        return "int"
    if col.type in STRING_TYPES:
        return "str"
    if col.type in DATE_TYPES:
        return "date"
    return col.type


def sql_fragments(definition: str) -> list[str]:
    """Код без комментариев + текст динамического SQL из длинных строковых литералов."""
    code = re.sub(r"/\*.*?\*/", " ", definition, flags=re.S)
    code = re.sub(r"--[^\n]*", " ", code)
    fragments = [code]
    for s in re.findall(r"N?'((?:[^']|'')*)'", code):
        if len(s) > 30 and re.search(r"\b(SELECT|INSERT|UPDATE|DELETE|MERGE|FROM)\b", s, re.I):
            fragments.append(s.replace("''", "'"))
    return fragments


def literal_value(col: Column, text: str):
    """Литерал из кода в значение колонки; None — не подходит по типу, длине или диапазону."""
    text = text.strip()
    quoted = text.upper().startswith("N'") or text.startswith("'")
    raw = text[text.index("'") + 1:-1].replace("''", "'") if quoted else text
    t = col.type
    try:
        if t in STRING_TYPES:
            return raw if raw and (col.length is None or len(raw) <= col.length) else None
        if t == "bit":
            return int(raw) if raw in ("0", "1") else None
        if t in INT_RANGE:
            v = int(raw)
            lo, hi = INT_RANGE[t]
            return v if lo <= v <= hi else None
        if t in ("decimal", "numeric", "money", "smallmoney"):
            v = Decimal(raw)
            if t in ("decimal", "numeric") and abs(v) >= Decimal(10) ** (col.precision - col.scale):
                return None
            return v
        if t in ("float", "real"):
            return float(raw)
        if t in DATE_TYPES and quoted:
            digits = raw.replace("-", "")[:8]
            d = dt.date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
            return d if t == "date" else dt.datetime.combine(d, dt.time())
        if t == "uniqueidentifier" and re.fullmatch(r"[0-9a-fA-F-]{36}", raw):
            return raw
    except (ValueError, ArithmeticError):
        return None
    return None


class UnionFind:
    def __init__(self):
        self.parent: dict = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b) -> None:
        self.parent[self.find(a)] = self.find(b)


def apply_code_hints(srv, db: str, tables: dict[int, Table], log) -> None:
    by_name: dict[tuple[str, str], Table] = {(t.schema.lower(), t.name.lower()): t for t in tables.values()}
    by_short: dict[str, list[Table]] = defaultdict(list)
    for t in tables.values():
        by_short[t.name.lower()].append(t)

    def resolve(obj: str, default_schema: str) -> Table | None:
        parts = [p.strip().strip("[]").lower() for p in re.split(r"\s*\.\s*", obj)]
        name = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else default_schema.lower()
        t = by_name.get((schema, name))
        if t is None and len(by_short.get(name, [])) == 1:
            t = by_short[name][0]
        return t

    literals: dict[tuple[int, str], list] = defaultdict(list)
    casts: dict[tuple[int, str], tuple] = {}
    uf = UnionFind()
    joins = 0
    modules = srv.rows(db, MODULES_SQL)
    number_dates: set[str] = set()      # имена колонок и параметров, которые код читает как ГГГГММДД
    for schema, definition in modules:
        number_dates |= {m.lower() for m in NUMBER_AS_DATE.findall(definition or "")}
        for frag in sql_fragments(definition or ""):
            aliases: dict[str, Table] = {}
            for obj, alias in TABLE_REF.findall(frag):
                t = resolve(obj, schema)
                if t is None:
                    continue
                aliases[t.name.lower()] = t
                if alias and alias.lower() not in NOT_ALIAS:
                    aliases[alias.lower()] = t
            if not aliases:
                continue
            in_scope = list({t.id: t for t in aliases.values()}.values())

            def targets(qualifier: str, column: str) -> list[tuple[Table, Column]]:
                if qualifier:
                    t = aliases.get(qualifier.lower())
                    c = t.find(column) if t else None
                    return [(t, c)] if c else []
                found = [(t, t.find(column)) for t in in_scope if t.find(column)]
                return found if len(found) <= 3 else []

            for qual, column, value in EQ_LITERAL.findall(frag):
                for t, c in targets(qual, column):
                    literals[(t.id, c.name)].append(value)
            for value, qual, column in REV_LITERAL.findall(frag):
                for t, c in targets(qual, column):
                    literals[(t.id, c.name)].append(value)
            for qual, column, items in IN_LIST.findall(frag):
                if re.search(r"\bSELECT\b|@", items, re.I):
                    continue
                for t, c in targets(qual, column):
                    literals[(t.id, c.name)].extend(re.findall(VALUE, items))
            for qual, column, typ in CAST_AS.findall(frag):
                for t, c in targets(qual, column):
                    casts[(t.id, c.name)] = (cast_kind(typ), None)
            for typ, qual, column, style in CONVERT_TO.findall(frag):
                for t, c in targets(qual, column):
                    casts.setdefault((t.id, c.name), (cast_kind(typ), style or None))
            for qa, ca, qb, cb in JOIN_EQ.findall(frag):
                a, b = targets(qa, ca), targets(qb, cb)
                if len(a) == 1 and len(b) == 1 and (a[0][0].id, a[0][1].name) != (b[0][0].id, b[0][1].name) \
                        and type_family(a[0][1]) == type_family(b[0][1]):
                    uf.union((a[0][0].id, a[0][1].name), (b[0][0].id, b[0][1].name))
                    joins += 1
    # одинаковые имена ключевых колонок одного типа
    same = defaultdict(list)
    for t in tables.values():
        for c in t.columns:
            if KEY_NAME.match(c.name):
                same[(c.name.lower(), type_family(c))].append((t.id, c.name))
    for members in same.values():
        for m in members[1:]:
            uf.union(members[0], m)
    # домены: классы связанных колонок из двух и более таблиц
    groups = defaultdict(list)
    for node in list(uf.parent):
        groups[uf.find(node)].append(node)
    domains = 0
    for k, members in enumerate(groups.values()):
        if len({tid for tid, _ in members}) < 2:
            continue
        domains += 1
        for tid, cname in members:
            tables[tid].col(cname).domain = k
    with_literals = 0
    for (tid, cname), values in literals.items():
        col = tables[tid].col(cname)
        if not col.insertable:
            continue
        converted = []
        for v in values:
            lv = literal_value(col, v)
            if lv is not None and lv not in converted and (not col.choices or lv in col.choices
                                                           or str(lv) in map(str, col.choices)):
                converted.append(lv)
        if converted:
            col.literals = converted
            with_literals += 1
    for t in tables.values():
        for c in t.columns:
            if c.name.lower() in number_dates and c.type in ("int", "bigint"):
                c.yyyymmdd = True
    # колонки с именами таблиц и схем (метаданные, по которым процедуры строят динамический SQL)
    table_names = sorted({t.name for t in tables.values()})
    schema_names = sorted({t.schema for t in tables.values()})
    for t in tables.values():
        for c in t.columns:
            if c.type not in STRING_TYPES or not c.insertable or c.literals:
                continue
            pool = table_names if OBJECT_COL.search(c.name) else schema_names if SCHEMA_COL.search(c.name) else None
            if pool:
                c.fixed_values = [n for n in pool if c.length is None or len(n) <= c.length] or None
    # допустимые значения CHECK — на одноимённые колонки других таблиц (raw.trx_type -> ods -> fact с CHECK):
    # одно недопустимое значение роняет весь INSERT ... SELECT следующего слоя
    allowed = defaultdict(list)
    for t in tables.values():
        for c in t.columns:
            if c.choices:
                allowed[(c.name.lower(), type_family(c))].extend(v for v in c.choices if v not in allowed[(c.name.lower(), type_family(c))])
    propagated = 0
    for t in tables.values():
        for c in t.columns:
            key = (c.name.lower(), type_family(c))
            if key in allowed and not c.choices and c.insertable and not c.fixed_values:
                values = [str(v) if c.type in STRING_TYPES else v for v in allowed[key]]
                c.fixed_values = [v for v in values if c.type not in STRING_TYPES or c.length is None or len(v) <= c.length]
                propagated += bool(c.fixed_values)
    typed = 0
    for (tid, cname), cast in casts.items():
        col = tables[tid].col(cname)
        if col.type in STRING_TYPES and col.insertable:
            col.cast_to = cast
            typed += 1
    # значения из кода — на все колонки связи: справочник получит их, иначе связанная строка не найдётся
    by_domain = defaultdict(list)
    for t in tables.values():
        for c in t.columns:
            if c.domain is not None:
                by_domain[c.domain].append(c)
    for cols in by_domain.values():
        shared = [v for c in cols for v in (c.literals or [])]
        for c in cols:
            extra = [v for v in shared if fits(c, v) and v not in (c.literals or [])]
            if extra and c.insertable:
                c.literals = (c.literals or []) + extra
    log(f"  подсказки из кода: модулей {len(modules)}; колонок со значениями из кода {with_literals}; "
        f"строковых колонок с приведением типа {typed}; значений CHECK на одноимённые колонки {propagated}; "
        f"связей из JOIN {joins}; общих наборов значений {domains}")


def cast_kind(typ: str) -> str:
    typ = typ.lower()
    if typ.startswith(("date", "smalldate")):
        return "date"
    return "decimal" if typ in ("decimal", "numeric", "money", "float") else "int"


# --------------------------------------------------------------------------- генерация значений

class Generator:
    def __init__(self, seed: int, null_rate: float):
        self.rng = random.Random(seed)
        self.null_rate = null_rate
        self._person = None
        self.used_unique: dict[int, set] = defaultdict(set)

    def new_row(self) -> None:
        """Новая строка — новый человек: ФИО, пол и email внутри строки согласованы."""
        self._person = None

    # ---- имена и тексты
    def person(self) -> tuple[str, str, str, bool]:
        if self._person is None:
            female = self.rng.random() < 0.5
            self._person = (self.rng.choice(LAST_M) + ("а" if female else ""),
                            self.rng.choice(FIRST_F if female else FIRST_M),
                            self.rng.choice(MIDDLE_F if female else MIDDLE_M), female)
        return self._person

    def digits(self, n: int) -> str:
        return str(self.rng.randint(1, 9)) + "".join(str(self.rng.randint(0, 9)) for _ in range(n - 1))

    def semantic_string(self, col: Column, i: int) -> str:
        n, r = col.name.lower(), self.rng
        last, first, middle, female = self.person()
        if "email" in n or "mail" in n:
            return f"{translit(first).lower()}.{translit(last).lower()}{i}@example.com"
        if "phone" in n or "tel" in n or "mobile" in n:
            return "+79" + self.digits(9)
        if re.search(r"(^|_)inn($|_)", n):
            return self.digits(12 if (col.length or 12) >= 12 else 10)
        if "snils" in n:
            return self.digits(11)
        if re.search(r"(^|_)(acc|account)(_?(no|num|number))?($|_)", n) and (col.length or 20) >= 20:
            return "408" + self.digits(17)
        if "passport" in n:
            return self.digits(10)
        if "last_name" in n or "surname" in n or "lastname" in n:
            return last
        if "first_name" in n or "firstname" in n:
            return first
        if "middle" in n or "patronym" in n or "second_name" in n:
            return middle
        if "fio" in n or "full_name" in n or "fullname" in n or n in ("client_name", "employee_name", "manager_name"):
            return f"{last} {first} {middle}"
        if "city" in n or "town" in n:
            return r.choice(CITIES)
        if "address" in n or "addr" in n:
            return f"г. {r.choice(CITIES)}, {r.choice(STREETS)}, д. {r.randint(1, 120)}"
        if "currency" in n or "ccy" in n or "curr" in n:
            return r.choice(CURRENCIES)
        if "country" in n:
            return r.choice(COUNTRIES2) if (col.length or 99) <= 3 else r.choice(COUNTRIES)
        if "status" in n or "state" in n:
            return r.choice(STATUSES)
        if "channel" in n:
            return r.choice(CHANNELS)
        if "segment" in n:
            return r.choice(SEGMENTS)
        if "gender" in n or n == "sex":
            return "F" if female else "M"
        if "risk" in n:
            return r.choice(["LOW", "MEDIUM", "HIGH"])
        if n.endswith("_type") or n.endswith("_kind") or n in ("type", "kind"):
            return r.choice(["A", "B", "C"]) if (col.length or 99) < 6 else f"TYPE_{r.choice('ABCDE')}"
        if "hash" in n or "hk" in n.split("_") or n.endswith("_hk") or "hashdiff" in n:
            return "%032X" % r.getrandbits(128)
        if "code" in n or n.endswith("_cd") or n.endswith("_id"):
            return f"{n.split('_')[0][:3].upper()}{i + 1:05d}"
        if "comment" in n or "descr" in n or "note" in n or "text" in n or "message" in n or "subject" in n:
            return " ".join(r.choice(WORDS) for _ in range(r.randint(2, 6))).capitalize()
        if "name" in n:
            return f"{r.choice(WORDS).capitalize()} {i + 1}"
        return f"{n[:12]}_{r.randint(1, 99999)}"

    def string(self, col: Column, i: int) -> str:
        if col.choices:
            return str(self.rng.choice(col.choices))
        s = self.semantic_string(col, i)
        if col.type not in UNICODE_TYPES:
            s = translit(s)
        return s[:col.length] if col.length else s

    # ---- числа
    def number_range(self, col: Column) -> tuple:
        n = col.name.lower()
        if col.type in INT_RANGE:
            tlo, thi = INT_RANGE[col.type]
        elif col.type in ("decimal", "numeric"):
            thi = Decimal(10) ** (col.precision - col.scale) - Decimal(1) / (Decimal(10) ** col.scale)
            tlo = -thi
        elif col.type == "smallmoney":
            tlo, thi = -214748, 214748
        else:
            tlo, thi = -1e12, 1e12
        if any(k in n for k in ("rate", "pct", "percent", "share", "ratio", "coef", "weight")):
            lo, hi = 0, 1 if col.type not in INT_RANGE else 100
        elif any(k in n for k in ("amount", "amt", "sum", "balance", "price", "value", "cost", "salary", "bonus",
                                  "turnover", "volume", "limit", "income", "payment")):
            lo, hi = 0, 1_000_000
        elif any(k in n for k in ("qty", "quantity", "count", "cnt", "num", "days", "score")):
            lo, hi = 0, 1000
        elif "age" in n.split("_"):
            lo, hi = 18, 80
        elif "year" in n:
            lo, hi = 2018, 2026
        elif "month" in n:
            lo, hi = 1, 12
        elif n.endswith("_id") or n.endswith("_key") or n.endswith("_sk") or n == "id":
            lo, hi = 1, 100_000
        else:
            lo, hi = 0, 10_000
        lo, hi = max(Decimal(str(lo)), Decimal(str(tlo))), min(Decimal(str(hi)), Decimal(str(thi)))
        if col.lo is not None and not isinstance(col.lo, dt.date):
            lo = max(lo, Decimal(col.lo) + (1 if col.lo_strict else 0) * self.step(col))
            if lo > hi:
                hi = min(Decimal(str(thi)), lo + 1000)
        if col.hi is not None and not isinstance(col.hi, dt.date):
            hi = min(hi, Decimal(col.hi) - (1 if col.hi_strict else 0) * self.step(col))
            if hi < lo:
                lo = max(Decimal(str(tlo)), hi - 1000)
        return lo, hi

    @staticmethod
    def step(col: Column) -> Decimal:
        if col.type in INT_RANGE:
            return Decimal(1)
        if col.type in ("decimal", "numeric"):
            return Decimal(1) / (Decimal(10) ** col.scale)
        return Decimal("0.0001")

    def number(self, col: Column):
        if col.choices:
            return self.cast_number(col, self.rng.choice(col.choices))
        lo, hi = self.number_range(col)
        if col.type in INT_RANGE:
            return self.rng.randint(int(lo), int(hi))
        if col.type in ("decimal", "numeric"):
            return Decimal(str(round(self.rng.uniform(float(lo), float(hi)), col.scale))).quantize(self.step(col))
        if col.type in ("money", "smallmoney"):
            return Decimal(str(round(self.rng.uniform(float(lo), float(hi)), 2)))
        return round(self.rng.uniform(float(lo), float(hi)), 4)

    @staticmethod
    def cast_number(col: Column, v):
        if isinstance(v, str):
            return v
        if col.type in INT_RANGE:
            return int(v)
        if col.type in ("float", "real"):
            return float(v)
        return Decimal(v)

    # ---- даты
    def date(self, col: Column) -> dt.date:
        n = col.name.lower()
        lo, hi = DATE_FROM, DATE_TO
        if "birth" in n or "dob" in n:
            lo, hi = dt.date(1950, 1, 1), dt.date(2005, 12, 31)
        if isinstance(col.lo, dt.date):
            lo = max(lo, col.lo + dt.timedelta(days=1 if col.lo_strict else 0))
        if isinstance(col.hi, dt.date):
            hi = min(hi, col.hi - dt.timedelta(days=1 if col.hi_strict else 0))
        if hi < lo:
            hi = lo
        d = lo + dt.timedelta(days=self.rng.randint(0, (hi - lo).days))
        return d.replace(day=1) if col.month_start else d

    def temporal(self, col: Column):
        d = self.date(col)
        if col.type == "date":
            return d
        moment = dt.datetime.combine(d, dt.time()) + dt.timedelta(seconds=self.rng.randint(8 * 3600, 20 * 3600))
        if col.type == "datetimeoffset":
            return moment.strftime("%Y-%m-%d %H:%M:%S +03:00")
        return moment

    def cast_string(self, col: Column) -> str:
        """Строка, которую код приводит к дате или числу: дата в стиле CONVERT, число с точкой."""
        kind, style = col.cast_to
        if kind == "date":
            d = self.date(col)
            fmt = {"104": "%d.%m.%Y", "103": "%d/%m/%Y", "112": "%Y%m%d", "101": "%m/%d/%Y"}.get(style or "", "%Y-%m-%d")
            s = d.strftime(fmt)
        else:
            s = f"{self.rng.uniform(0, 100000):.2f}" if kind == "decimal" else str(self.rng.randint(1, 100000))
        return s[:col.length] if col.length else s

    # ---- значение колонки
    def value(self, col: Column, i: int, null_ok: bool = True):
        if null_ok and col.nullable and not col.required and self.rng.random() < self.null_rate:
            return None
        if col.literals and self.rng.random() < col.literal_rate:
            return self.rng.choice(col.literals)
        if col.cast_to and col.type in STRING_TYPES:
            return self.cast_string(col)
        if col.fixed_values:
            return self.rng.choice(col.fixed_values)
        if col.date_key:
            return int(self.date(col).strftime("%Y%m%d"))
        t = col.type
        if t == "bit":
            return int(self.rng.choice(col.choices)) if col.choices else self.rng.randint(0, 1)
        if t in INT_RANGE or t in NUMERIC_TYPES:
            return self.number(col)
        if t in STRING_TYPES:
            return self.string(col, i)
        if t in DATE_TYPES:
            return self.temporal(col)
        if t == "time":
            return dt.time(self.rng.randint(8, 20), self.rng.randint(0, 59), self.rng.randint(0, 59))
        if t == "uniqueidentifier":
            return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
        if t in ("binary", "varbinary", "image"):
            return self.rng.randbytes(min(col.length or 16, 16))
        if t == "xml":
            return f'<row id="{i + 1}"/>'
        if t == "hierarchyid":
            return f"/{i + 1}/"
        if t == "geography":
            return f"POINT({37 + self.rng.random():.4f} {55 + self.rng.random():.4f})"
        if t == "geometry":
            return f"POINT({self.rng.randint(0, 100)} {self.rng.randint(0, 100)})"
        if t == "sql_variant":
            return str(self.rng.randint(1, 1000))
        return None

    def unique_value(self, col: Column, n: int):
        """n-е уникальное значение колонки (n = 0, 1, 2 ...) или None, если ёмкость типа исчерпана.
        Сначала — значения из кода (справочник получит строки, которые ищут процедуры), затем последовательность."""
        if col.date_key:
            d = DATE_FROM + dt.timedelta(days=n)
            return int(d.strftime("%Y%m%d")) if d <= DATE_TO else None
        if col.literals:
            if n < len(col.literals):
                return col.literals[n]
            n -= len(col.literals)
        t = col.type
        if col.choices:
            return self.cast_number(col, col.choices[n]) if n < len(col.choices) else None
        if t == "bit":
            return n if n < 2 else None
        if t in INT_RANGE or t in ("decimal", "numeric", "money", "smallmoney", "float", "real"):
            lo, hi = self.number_range(col)
            lo = max(lo, Decimal(1)) if hi >= 1 else lo
            v = int(lo.to_integral_value(rounding="ROUND_CEILING")) + n
            return None if v > hi else self.cast_number(col, v)
        if t in STRING_TYPES:
            # смысловое значение, суффикс — только при совпадении с уже выданным
            used = self.used_unique[id(col)]
            cand = self.string(col, n)
            if not cand or cand in used:
                cand = f"{cand}-{n + 1}" if cand else str(n + 1)
            if col.length and len(cand) > col.length:
                cand = to_base36(n + 1, col.length)
            used.add(cand)
            return cand
        if t in DATE_TYPES:
            start = col.lo if isinstance(col.lo, dt.date) and col.lo > DATE_FROM else DATE_FROM
            if col.month_start:
                m = start.year * 12 + start.month - 1 + n
                v = dt.date(m // 12, m % 12 + 1, 1)
            else:
                v = start + dt.timedelta(days=n)
            if t == "date":
                return v
            moment = dt.datetime.combine(v, dt.time(12))
            return moment.strftime("%Y-%m-%d %H:%M:%S +03:00") if t == "datetimeoffset" else moment
        if t == "time":
            return (dt.datetime(2000, 1, 1) + dt.timedelta(seconds=n)).time() if n < 86400 else None
        if t == "uniqueidentifier":
            return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
        if t in ("binary", "varbinary"):
            size = col.length or 8
            return n.to_bytes(size, "big") if n < 256 ** min(size, 8) else None
        return None


def to_base36(n: int, width: int) -> str | None:
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    s = ""
    while n:
        n, r = divmod(n, 36)
        s = alphabet[r] + s
    s = s or "0"
    return s.rjust(width, "0") if len(s) <= width else s[-width:]


# --------------------------------------------------------------------------- порядок заполнения

def order_tables(tables: dict[int, Table]) -> list[Table]:
    """Топологический порядок по внешним ключам; ключи-самоссылки и ключи внутри циклов помечаются deferred."""
    deps = {oid: {fk.referenced for fk in t.fks if fk.referenced != oid and fk.referenced in tables}
            for oid, t in tables.items()}
    for t in tables.values():
        for fk in t.fks:
            if fk.referenced == t.id:
                fk.deferred = True
    # мягкие зависимости (связи из кода): справочник, где колонка связи уникальна, — раньше остальных таблиц домена
    owners = defaultdict(set)
    for t in tables.values():
        for s in t.unique_sets:
            if len(s) == 1 and t.col(s[0]).domain is not None:
                owners[t.col(s[0]).domain].add(t.id)
    soft = {oid: set() for oid in tables}
    for t in tables.values():
        for c in t.columns:
            if c.domain in owners and t.id not in owners[c.domain]:
                soft[t.id] |= owners[c.domain]
    done, order = set(), []
    remaining = dict(deps)
    while remaining:
        ready = sorted((oid for oid, d in remaining.items() if d <= done and soft[oid] - {oid} <= done),
                       key=lambda o: (tables[o].schema.lower(), tables[o].name.lower()))
        if not ready:
            # мягкие зависимости мешают — сначала отбрасываются они
            ready = sorted((oid for oid, d in remaining.items() if d <= done),
                           key=lambda o: (len(soft[o] - done), tables[o].schema.lower(), tables[o].name.lower()))[:1]
        if not ready:
            # цикл: берём таблицу, у которой больше всего ключей можно отложить (nullable), разрываем её ключи
            oid = min(remaining, key=lambda o: (
                -sum(fk.nullable_in(tables[o]) for fk in tables[o].fks if fk.referenced in remaining),
                tables[o].schema.lower(), tables[o].name.lower()))
            for fk in tables[oid].fks:
                if fk.referenced in remaining and fk.referenced not in done:
                    fk.deferred = True
            ready = [oid]
        for oid in ready:
            order.append(tables[oid])
            done.add(oid)
            remaining.pop(oid)
    return order


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

    def execute(self, db: str, sql: str, params=()) -> int:
        cur = self.conn(db).cursor()
        cur.execute(sql, params)
        n = cur.rowcount
        while cur.nextset():
            pass
        return n

    def close(self):
        for c in self.connections.values():
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass


def short(exc: Exception) -> str:
    text = str(exc)
    found = re.findall(r"\[SQL Server\](.*?)(?=\s*\(SQLExecDirectW\)|\s*\(SQLExecute\)|;\s*\[\w+\]\s*\[Microsoft\]"
                       r"|[\"']\)?$)", text)
    text = "; ".join(s.strip() for s in found) if found else text
    return text.replace("\\'", "'").replace("\r", " ").replace("\n", " ").strip()[:300]


# --------------------------------------------------------------------------- заполнение

class Filler:
    def __init__(self, srv: Server, gen: Generator, log):
        self.srv, self.gen, self.log = srv, gen, log
        self.pools: dict[tuple, list[tuple]] = {}
        self.domain_pools: dict[tuple[str, int], list] = defaultdict(list)   # (база, домен) -> значения

    def collect_domains(self, t: Table) -> None:
        """Значения колонок-доменов заполненной (или уже содержащей данные) таблицы — для связанных таблиц."""
        for c in t.columns:
            if c.domain is None:
                continue
            pool = self.domain_pools[(t.db, c.domain)]
            if len(pool) >= 20000:
                continue
            try:
                rows = self.srv.rows(t.db, f"SELECT DISTINCT TOP (5000) {q(c.name)} FROM {t.sql_name} "
                                           f"WHERE {q(c.name)} IS NOT NULL")
            except self.srv.pyodbc.Error:
                continue
            seen = set(pool)
            pool.extend(v for (v,) in rows if v not in seen)

    def pool(self, db: str, table: Table, cols: list[str]) -> list[tuple]:
        key = (db, table.id, tuple(cols))
        if key not in self.pools:
            sel = ", ".join(q(c) for c in cols)
            where = " AND ".join(f"{q(c)} IS NOT NULL" for c in cols)
            rows = self.srv.rows(db, f"SELECT DISTINCT TOP (20000) {sel} FROM {table.sql_name} WHERE {where}")
            self.pools[key] = [tuple(r) for r in rows]
        return self.pools[key]

    def make_rows(self, t: Table, tables: dict[int, Table], target: int) -> tuple[list[str], list[list]]:
        gen = self.gen
        cols = [c for c in t.columns if c.insertable]
        names = [c.name for c in cols]
        pos = {c.name: k for k, c in enumerate(cols)}
        fk_cols = {c for fk in t.fks for c in fk.columns}
        # колонки, уникальность которых обеспечивается последовательностью значений
        unique_sets = t.generated_unique_sets
        seq_cols = {c for s in unique_sets for c in s if c in pos and c not in fk_cols}
        counters = defaultdict(int)
        used = [set() for _ in unique_sets]
        pools, exclusive = {}, {}
        for fk in t.fks:
            if fk.deferred:
                continue
            pools[fk.name] = self.pool(t.db, tables[fk.referenced], fk.ref_columns)
            # ключ целиком покрывает уникальный набор (связь 1:1) — родители перебираются без повторов
            if any(set(s) <= set(fk.columns) for s in unique_sets):
                shuffled = list(pools[fk.name])
                gen.rng.shuffle(shuffled)
                exclusive[fk.name] = iter(shuffled)
        pairs = [(a, b, False) for a, b in date_pairs(t)] + [
            (a, b, op == "<") if op in ("<", "<=") else (b, a, op == ">") for a, op, b in t.comparisons]
        # связи из кода: значения уже заполненных связанных таблиц
        domain_values = {}
        for c in cols:
            if c.domain is not None and c.name not in fk_cols:
                vals = [v for v in self.domain_pools.get((t.db, c.domain), []) if fits(c, v)]
                if vals:
                    domain_values[c.name] = vals
        domain_iters = {n: iter(v) for n, v in domain_values.items() if n in seq_cols}   # уникальные — без повторов
        rows, misses = [], 0
        while len(rows) < target and misses < 200:
            i = len(rows)
            gen.new_row()
            row = [None] * len(cols)
            exhausted = False
            for c in cols:
                if c.name in seq_cols:
                    v = next(domain_iters[c.name], None) if c.name in domain_iters else None
                    if v is None:
                        v = gen.unique_value(c, counters[c.name])
                        counters[c.name] += 1
                    if v is None:
                        exhausted = True
                        break
                    row[pos[c.name]] = v
                elif c.name in domain_values:
                    r = gen.rng.random()
                    if c.nullable and not c.required and gen.rng.random() < gen.null_rate:
                        row[pos[c.name]] = None
                    elif c.literals and r < c.literal_rate:
                        row[pos[c.name]] = gen.rng.choice(c.literals)
                    elif r < DOMAIN_RATE:
                        row[pos[c.name]] = gen.rng.choice(domain_values[c.name])
                    else:
                        row[pos[c.name]] = gen.value(c, i)
                elif c.name not in fk_cols:
                    row[pos[c.name]] = gen.value(c, i)
            if exhausted:
                break
            for fk in t.fks:
                present = [c for c in fk.columns if c in pos]
                if fk.deferred:
                    for c in present:   # дозаполнится UPDATE; NOT NULL — временное значение при отключённом ключе
                        col = t.col(c)
                        row[pos[c]] = None if col.nullable else gen.value(col, i, null_ok=False)
                    continue
                if fk.name in exclusive:
                    values = next(exclusive[fk.name], None)
                    if values is None:
                        exhausted = True
                        break
                elif not pools[fk.name] or (fk.nullable_in(t) and gen.rng.random() < gen.null_rate):
                    for c in present:
                        row[pos[c]] = None
                    continue
                else:
                    values = gen.rng.choice(pools[fk.name])
                for c, v in zip(fk.columns, values):
                    if c in pos:
                        row[pos[c]] = v
            if exhausted:
                break
            for a, b, strict in pairs:
                if a in pos and b in pos:
                    va, vb = row[pos[a]], row[pos[b]]
                    if va is not None and vb is not None and type(va) is type(vb):
                        if va > vb:
                            row[pos[a]], row[pos[b]] = vb, va
                        elif strict and va == vb:
                            row[pos[b]] = bump(t.col(b), vb)
            keys = [tuple(row[pos[c]] for c in s) for s in unique_sets]
            if any(k in u for k, u in zip(keys, used)):
                misses += 1
                continue
            for k, u in zip(keys, used):
                u.add(k)
            rows.append(row)
        return names, rows

    def insert(self, t: Table, names: list[str], rows: list[list]) -> None:
        if not names:
            for _ in rows:
                self.srv.execute(t.db, f"INSERT INTO {t.sql_name} DEFAULT VALUES")
            t.inserted += len(rows)
            return
        # CLR-типы (hierarchyid, geography, geometry) — строкой: ODBC не описывает параметр такого типа для NULL
        marks = ", ".join("CAST(? AS nvarchar(max))" if t.col(n).type in CLR_TYPES else "?" for n in names)
        sql = f"INSERT INTO {t.sql_name} ({', '.join(q(n) for n in names)}) VALUES ({marks})"
        cur = self.srv.conn(t.db).cursor()
        # fast_executemany в pyodbc падает (segfault) на колонках (max), xml, CLR-типах и sql_variant
        fast = all(c.length is not None or c.type not in STRING_TYPES | {"varbinary"} for c in t.columns if c.insertable) \
            and not any(c.type in SLOW_TYPES for c in t.columns if c.insertable)
        for start in range(0, len(rows), BATCH):
            chunk = rows[start:start + BATCH]
            conn = self.srv.conn(t.db)
            try:
                conn.autocommit = False
                cur.fast_executemany = fast
                cur.executemany(sql, chunk)
                conn.commit()
                t.inserted += len(chunk)
                continue
            except self.srv.pyodbc.Error:
                conn.rollback()
            finally:
                conn.autocommit = True
            # пачка не прошла (CHECK, триггер, тип) — построчно, плохие строки отбрасываются
            cur.fast_executemany = False
            for row in chunk:
                try:
                    cur.execute(sql, row)
                    t.inserted += 1
                except self.srv.pyodbc.Error as exc:
                    t.rejected += 1
                    t.error = t.error or short(exc)

    def deferred_fks(self, t: Table, tables: dict[int, Table]) -> None:
        """Самоссылки и ключи внутри циклов: проставить ссылки UPDATE-ом, когда заполнены все таблицы базы."""
        for fk in t.fks:
            if not fk.deferred:
                continue
            try:
                self.deferred_fk(t, fk, tables)
            except self.srv.pyodbc.Error as exc:
                t.error = t.error or f"{fk.name}: {short(exc)}"
                if t.status == "filled":
                    t.status = "partial"
            if fk.nocheck:
                try:
                    self.srv.execute(t.db, f"ALTER TABLE {t.sql_name} WITH CHECK CHECK CONSTRAINT {q(fk.name)}")
                    fk.nocheck = False
                except self.srv.pyodbc.Error as exc:
                    t.error = t.error or f"ключ {fk.name} оставлен отключённым: {short(exc)}"
                    t.status = "failed"

    def deferred_fk(self, t: Table, fk: ForeignKey, tables: dict[int, Table]) -> None:
        parent = tables[fk.referenced]
        self.pools.pop((t.db, parent.id, tuple(fk.ref_columns)), None)
        pool = self.pool(t.db, parent, fk.ref_columns)
        if not pool:
            t.error = t.error or f"{fk.name}: у {parent.fqn} нет строк для ссылки"
            return
        sets = ", ".join(f"{q(c)} = ?" for c in fk.columns)
        # строки адресуются уникальным ключом, не включающим колонки самого внешнего ключа
        key = next((s for s in t.unique_sets if not set(s) & set(fk.columns)), None)
        if key is None:
            self.srv.execute(t.db, f"UPDATE {t.sql_name} SET {sets}", list(self.gen.rng.choice(pool)))
            return
        keys = self.srv.rows(t.db, f"SELECT {', '.join(q(c) for c in key)} FROM {t.sql_name}")
        where = " AND ".join(f"{q(c)} = ?" for c in key)
        params = [list(self.gen.rng.choice(pool)) + list(k) for k in keys
                  if not (fk.nullable_in(t) and self.gen.rng.random() < self.gen.null_rate)]
        if params:
            cur = self.srv.conn(t.db).cursor()
            cur.fast_executemany = True
            cur.executemany(f"UPDATE {t.sql_name} SET {sets} WHERE {where}", params)

    def fill(self, t: Table, tables: dict[int, Table], target: int) -> None:
        started = time.perf_counter()
        try:
            for fk in t.fks:
                if fk.deferred and not fk.nullable_in(t):
                    self.srv.execute(t.db, f"ALTER TABLE {t.sql_name} NOCHECK CONSTRAINT {q(fk.name)}")
                    fk.nocheck = True
                elif not fk.deferred and not fk.nullable_in(t):
                    if not self.pool(t.db, tables[fk.referenced], fk.ref_columns):
                        t.status = "failed"
                        t.error = f"нет строк в {tables[fk.referenced].fqn} для обязательного ключа {fk.name}"
                        return
            names, rows = self.make_rows(t, tables, target)
            self.insert(t, names, rows)
            if t.inserted == 0:
                t.status = "failed"
                t.error = t.error or "не сгенерировано ни одной строки"
            elif t.inserted < target:
                t.status = "partial"
                if not t.error:
                    t.error = "ёмкость уникальных значений исчерпана"
            else:
                t.status = "filled"
        except self.srv.pyodbc.Error as exc:
            t.status, t.error = "failed", short(exc)
            for fk in t.fks:
                if fk.nocheck:
                    try:
                        self.srv.execute(t.db, f"ALTER TABLE {t.sql_name} WITH CHECK CHECK CONSTRAINT {q(fk.name)}")
                    except self.srv.pyodbc.Error:
                        t.error += f"; ключ {fk.name} оставлен отключённым"
        finally:
            t.seconds = time.perf_counter() - started
            # таблица теперь может быть родителем: сбросить кеш её ключей, отдать значения связанным таблицам
            for key in [k for k in self.pools if k[0] == t.db and k[1] == t.id]:
                self.pools.pop(key)
            if t.inserted:
                self.collect_domains(t)


def calendar_days(t: Table) -> int | None:
    """Календарь (уникальный ключ — одна дата или date_key): строк столько, сколько дней в периоде данных,
    иначе ссылки фактов на календарь не найдутся."""
    for s in t.generated_unique_sets:
        if len(s) == 1:
            c = t.col(s[0])
            if (c.type in ("date", "datetime", "datetime2", "smalldatetime")
                    or (c.date_key and DATE_KEY.search(c.name))) and not c.literals:
                return (DATE_TO - DATE_FROM).days + 1
    return None


def fits(col: Column, v) -> bool:
    """Значение связанной колонки годится для этой колонки по типу, длине и CHECK."""
    if col.choices and str(v) not in {str(c) for c in col.choices}:
        return False
    t = col.type
    if t in STRING_TYPES:
        return isinstance(v, str) and (col.length is None or len(v) <= col.length)
    if t in INT_RANGE:
        lo, hi = INT_RANGE[t]
        return isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi
    if t in DATE_TYPES:
        return isinstance(v, (dt.date, dt.datetime))
    if t in ("binary", "varbinary"):
        return isinstance(v, (bytes, bytearray)) and (col.length is None or len(v) <= col.length)
    if t in ("decimal", "numeric"):
        try:
            return abs(Decimal(v)) < Decimal(10) ** (col.precision - col.scale)
        except (ValueError, ArithmeticError, TypeError):
            return False
    return True


def bump(col: Column, v):
    """Следующее значение после v (для строгого сравнения двух колонок)."""
    if isinstance(v, dt.datetime):
        return v + dt.timedelta(seconds=1)
    if isinstance(v, dt.date):
        return v + dt.timedelta(days=1)
    if isinstance(v, Decimal):
        return v + Generator.step(col)
    if isinstance(v, (int, float)):
        return v + 1
    return v


def date_pairs(t: Table) -> list[tuple[str, str]]:
    """Пары колонок «начало — окончание» по именам: valid_from/valid_to, start_dt/end_dt, open_date/close_date."""
    names = {c.name.lower(): c.name for c in t.columns if c.type in DATE_TYPES}
    pairs = []
    for low, real in names.items():
        parts = low.split("_")
        for k, part in enumerate(parts):
            if part in END_TOKENS:
                for start in START_TOKENS:
                    cand = "_".join(parts[:k] + [start] + parts[k + 1:])
                    if cand in names and cand != low:
                        pairs.append((names[cand], real))
    return pairs


def clean(srv: Server, order: list[Table], tables: dict[int, Table], log) -> None:
    inside = {t.id for t in order}
    fks = [(t, fk) for t in order for fk in t.fks if fk.referenced in inside]
    for t, fk in fks:
        srv.execute(t.db, f"ALTER TABLE {t.sql_name} NOCHECK CONSTRAINT {q(fk.name)}")
    referenced = {fk.referenced for t in tables.values() for fk in t.fks if fk.referenced != t.id}
    for t in reversed(order):
        if t.rows_now == 0:
            continue
        try:
            if t.id in referenced:
                raise srv.pyodbc.Error("referenced")
            srv.execute(t.db, f"TRUNCATE TABLE {t.sql_name}")
        except srv.pyodbc.Error:
            try:
                srv.execute(t.db, f"DELETE FROM {t.sql_name}")
                if any(c.identity for c in t.columns):
                    seed = srv.rows(t.db, "SELECT CAST(IDENT_SEED(?) - IDENT_INCR(?) AS bigint)",
                                    (t.sql_name, t.sql_name))[0][0]
                    name = t.sql_name.replace("'", "''")
                    srv.execute(t.db, f"DBCC CHECKIDENT ('{name}', RESEED, {seed}) WITH NO_INFOMSGS")
            except srv.pyodbc.Error as exc:
                log(f"  очистка {t.db}.{t.fqn}: {short(exc)}")
                continue
        log(f"  очищено {t.db}.{t.fqn} ({t.rows_now} строк)")
        t.rows_now = 0
    for t, fk in fks:
        try:
            srv.execute(t.db, f"ALTER TABLE {t.sql_name} WITH CHECK CHECK CONSTRAINT {q(fk.name)}")
        except srv.pyodbc.Error as exc:
            log(f"  ключ {t.db}.{t.fqn}.{fk.name} не включён: {short(exc)}")


# --------------------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path, nargs="?", default=DEFAULT_ROOT,
                    help=f"корень реплики — имена баз берутся из папок (по умолчанию {DEFAULT_ROOT})")
    ap.add_argument("--databases", nargs="+", help="базы (по умолчанию — все папки реплики)")
    ap.add_argument("--schemas", nargs="+", help="только эти схемы")
    ap.add_argument("--exclude", help="регулярное выражение по schema.table — не заполнять")
    ap.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="строк на таблицу")
    ap.add_argument("--rows-table", nargs="+", default=[], metavar="SCHEMA.TABLE=N", help="число строк для таблиц")
    ap.add_argument("--clean", action="store_true", help="очистить выбранные таблицы перед заполнением")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--null-rate", type=float, default=DEFAULT_NULL_RATE)
    ap.add_argument("--dry-run", action="store_true", help="показать порядок и план без записи")
    ap.add_argument("--no-code-hints", action="store_true",
                    help="не выводить значения и связи из кода процедур и представлений")
    ap.add_argument("--server", default=os.getenv("MSSQL_HOST", DEFAULT_SERVER))
    ap.add_argument("--port", default=os.getenv("MSSQL_PORT", DEFAULT_PORT))
    ap.add_argument("--user", default=os.getenv("MSSQL_USER", DEFAULT_USER))
    ap.add_argument("--password-env", default="MSSQL_PASSWORD",
                    help="переменная окружения с паролем; если не задана — пароль по умолчанию")
    ap.add_argument("--trusted", action="store_true", help="Windows-аутентификация")
    ap.add_argument("--driver", default=DEFAULT_DRIVER)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
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
    rows_for = {}
    for spec in args.rows_table:
        name, _, n = spec.rpartition("=")
        rows_for[name.lower()] = int(n)
    exclude = re.compile(args.exclude, re.I) if args.exclude else None
    schemas = {s.lower() for s in args.schemas} if args.schemas else None

    srv = Server(args)
    try:
        srv.conn("master")
    except srv.pyodbc.Error as exc:
        log(f"Нет подключения к {args.server},{args.port}: {short(exc)}")
        return 2
    log(f"Сервер {args.server},{args.port}; базы: {', '.join(databases)}; строк на таблицу {args.rows}; seed {args.seed}")
    gen = Generator(args.seed, args.null_rate)
    filler = Filler(srv, gen, log)
    all_tables: list[Table] = []
    try:
        for db in databases:
            if not srv.rows("master", "SELECT 1 FROM sys.databases WHERE name = ?", (db,)):
                log(f"[{db}] базы нет на сервере — пропущена")
                continue
            tables = load_tables(srv, db)
            log(f"\n[{db}] таблиц {len(tables)}")
            if not args.no_code_hints:
                apply_code_hints(srv, db, tables, log)
            for t in tables.values():
                if schemas and t.schema.lower() not in schemas:
                    t.skip_reason = t.skip_reason or "схема не выбрана"
                elif exclude and exclude.search(t.fqn):
                    t.skip_reason = t.skip_reason or "--exclude"
            order = order_tables(tables)
            selected = [t for t in order if not t.skip_reason]
            log(f"  к заполнению {len(selected)}")
            if args.dry_run:
                for t in order:
                    fks = ", ".join(("~" if fk.deferred else "") + tables[fk.referenced].fqn for fk in t.fks)
                    info = t.skip_reason or f"строк сейчас {t.rows_now}" + (f"; ссылки: {fks}" if fks else "")
                    hints = [f"{c.name}={c.literals[:4]}" for c in t.columns if c.literals]
                    linked = [c.name for c in t.columns if c.domain is not None]
                    log(f"  {t.fqn:50} {info}")
                    if hints:
                        log(f"  {'':50}   из кода: {'; '.join(hints)}")
                    if linked:
                        log(f"  {'':50}   связанные колонки: {', '.join(linked)}")
                all_tables += order
                continue
            if args.clean:
                clean(srv, selected, tables, log)
            for t in order:
                if t.skip_reason:
                    t.status = "skipped"
                    continue
                if t.rows_now > 0:
                    t.status, t.error = "skipped", f"есть данные ({t.rows_now} строк)"
                    log(f"  SKIP    {t.fqn} — {t.error}")
                    filler.collect_domains(t)
                    continue
                target = rows_for.get(t.fqn.lower(), calendar_days(t) or args.rows)
                filler.fill(t, tables, target)
                mark = {"filled": "OK", "partial": "PARTIAL", "failed": "FAIL"}[t.status]
                extra = f"; отброшено {t.rejected}" if t.rejected else ""
                log(f"  {mark:7} {t.fqn} — {t.inserted} строк{extra}" + (f"; {t.error}" if t.error else ""))
            for t in order:
                if t.status in ("filled", "partial") or any(fk.nocheck for fk in t.fks):
                    filler.deferred_fks(t, tables)
            all_tables += order
    finally:
        srv.close()
    if args.dry_run:
        return 0
    counts = defaultdict(int)
    for t in all_tables:
        counts[t.status] += 1
    log(f"\n=== Итог: заполнено {counts['filled']}, частично {counts['partial']}, с ошибкой {counts['failed']}, "
        f"пропущено {counts['skipped']}; строк вставлено {sum(t.inserted for t in all_tables)}, "
        f"отброшено {sum(t.rejected for t in all_tables)}")
    for t in all_tables:
        if t.status in ("failed", "partial"):
            log(f"  {t.status:8} {t.db}.{t.fqn}: {t.error}")
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
