"""
SQL Data Warehouse Analysis Toolkit

Provides Python wrappers for warehouse operations using SQLite,
including star schema construction, SCD Type 2 implementation,
ETL pipeline orchestration, OLAP cube simulation, and query optimization.

Author: Gabriel Demetrios Lafis
"""

import sqlite3
import hashlib
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any


class StarSchemaBuilder:
    """Builds and manages star schema structures in SQLite."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self._dimensions: Dict[str, List[str]] = {}
        self._facts: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Dimension helpers
    # ------------------------------------------------------------------
    def create_dimension_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        surrogate_key: str = "sk_id",
    ) -> None:
        """Create a dimension table with a surrogate key.

        Args:
            table_name: Name of the dimension table.
            columns: Mapping of column name -> SQLite type.
            surrogate_key: Name of the auto-increment surrogate key.
        """
        col_defs = [f"{surrogate_key} INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, dtype in columns.items():
            col_defs.append(f"{col} {dtype}")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        self.cursor.execute(sql)
        self.conn.commit()
        self._dimensions[table_name] = list(columns.keys())

    def create_fact_table(
        self,
        table_name: str,
        measures: Dict[str, str],
        foreign_keys: Dict[str, str],
    ) -> None:
        """Create a fact table referencing dimension surrogate keys.

        Args:
            table_name: Name of the fact table.
            measures: Mapping of measure name -> SQLite type.
            foreign_keys: Mapping of FK column -> referenced table.
        """
        col_defs = ["fact_id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for fk_col, ref_table in foreign_keys.items():
            col_defs.append(f"{fk_col} INTEGER REFERENCES {ref_table}(sk_id)")
        for measure, dtype in measures.items():
            col_defs.append(f"{measure} {dtype}")
        col_defs.append("created_at TEXT DEFAULT (datetime('now'))")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        self.cursor.execute(sql)
        self.conn.commit()
        self._facts[table_name] = list(measures.keys())

    def insert_dimension_row(self, table_name: str, row: Dict[str, Any]) -> int:
        """Insert a single row into a dimension table and return the surrogate key."""
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        self.cursor.execute(sql, list(row.values()))
        self.conn.commit()
        return self.cursor.lastrowid

    def insert_fact_row(self, table_name: str, row: Dict[str, Any]) -> int:
        """Insert a single row into a fact table and return the fact_id."""
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        self.cursor.execute(sql, list(row.values()))
        self.conn.commit()
        return self.cursor.lastrowid

    def get_schema_info(self) -> Dict[str, Any]:
        """Return metadata about the star schema."""
        return {
            "dimensions": dict(self._dimensions),
            "facts": dict(self._facts),
            "dimension_count": len(self._dimensions),
            "fact_count": len(self._facts),
        }

    def close(self) -> None:
        self.conn.close()


class SCDType2Manager:
    """Manages Slowly Changing Dimensions Type 2.

    Each dimension row carries ``valid_from``, ``valid_to``, and
    ``is_current`` columns to track historical changes.
    """

    def __init__(self, conn: sqlite3.Connection, table_name: str):
        self.conn = conn
        self.cursor = conn.cursor()
        self.table_name = table_name

    def create_scd2_table(self, columns: Dict[str, str]) -> None:
        """Create an SCD Type 2 dimension table."""
        col_defs = ["sk_id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, dtype in columns.items():
            col_defs.append(f"{col} {dtype}")
        col_defs.extend([
            "valid_from TEXT NOT NULL",
            "valid_to TEXT DEFAULT '9999-12-31'",
            "is_current INTEGER DEFAULT 1",
            "row_hash TEXT",
        ])
        sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({', '.join(col_defs)})"
        self.cursor.execute(sql)
        self.conn.commit()
        self._columns = list(columns.keys())

    def _compute_hash(self, row: Dict[str, Any]) -> str:
        data = "|".join(str(row.get(c, "")) for c in self._columns)
        return hashlib.md5(data.encode()).hexdigest()

    def upsert(self, natural_key_col: str, row: Dict[str, Any], effective_date: str = None) -> str:
        """Insert a new record or expire the old one and insert a new version.

        Returns:
            'inserted' if new, 'updated' if changed, 'unchanged' if identical.
        """
        if effective_date is None:
            effective_date = datetime.now().strftime("%Y-%m-%d")
        new_hash = self._compute_hash(row)

        self.cursor.execute(
            f"SELECT sk_id, row_hash FROM {self.table_name} "
            f"WHERE {natural_key_col} = ? AND is_current = 1",
            (row[natural_key_col],),
        )
        existing = self.cursor.fetchone()

        if existing is None:
            cols = ", ".join(list(row.keys()) + ["valid_from", "row_hash"])
            placeholders = ", ".join(["?"] * (len(row) + 2))
            values = list(row.values()) + [effective_date, new_hash]
            self.cursor.execute(
                f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})",
                values,
            )
            self.conn.commit()
            return "inserted"

        if existing[1] == new_hash:
            return "unchanged"

        # Expire old record
        self.cursor.execute(
            f"UPDATE {self.table_name} SET valid_to = ?, is_current = 0 WHERE sk_id = ?",
            (effective_date, existing[0]),
        )
        # Insert new version
        cols = ", ".join(list(row.keys()) + ["valid_from", "row_hash"])
        placeholders = ", ".join(["?"] * (len(row) + 2))
        values = list(row.values()) + [effective_date, new_hash]
        self.cursor.execute(
            f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})",
            values,
        )
        self.conn.commit()
        return "updated"

    def get_history(self, natural_key_col: str, key_value: Any) -> List[Dict]:
        """Return the full version history for a natural key."""
        self.cursor.execute(
            f"SELECT * FROM {self.table_name} WHERE {natural_key_col} = ? ORDER BY valid_from",
            (key_value,),
        )
        rows = self.cursor.fetchall()
        return [dict(zip([d[0] for d in self.cursor.description], r)) for r in rows]


class ETLPipeline:
    """Lightweight ETL pipeline orchestrated over SQLite."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        self._steps: List[Dict[str, Any]] = []
        self._log: List[Dict[str, Any]] = []

    def add_step(self, name: str, sql: str, params: tuple = ()) -> None:
        """Register an ETL step."""
        self._steps.append({"name": name, "sql": sql, "params": params})

    def run(self) -> List[Dict[str, Any]]:
        """Execute all registered steps sequentially."""
        results = []
        for step in self._steps:
            start = datetime.now()
            try:
                self.cursor.execute(step["sql"], step["params"])
                self.conn.commit()
                affected = self.cursor.rowcount
                status = "success"
                error = None
            except Exception as exc:
                affected = 0
                status = "failed"
                error = str(exc)
            elapsed = (datetime.now() - start).total_seconds()
            entry = {
                "step": step["name"],
                "status": status,
                "rows_affected": affected,
                "elapsed_seconds": elapsed,
                "error": error,
            }
            results.append(entry)
            self._log.append(entry)
        return results

    def get_log(self) -> List[Dict[str, Any]]:
        return list(self._log)


class OLAPCubeSimulator:
    """Simulates basic OLAP cube operations (roll-up, drill-down, slice, dice)."""

    def __init__(self, conn: sqlite3.Connection, fact_table: str):
        self.conn = conn
        self.cursor = conn.cursor()
        self.fact_table = fact_table

    def rollup(self, dimension_cols: List[str], measure: str, agg: str = "SUM") -> List[Dict]:
        """Aggregate a measure across dimension hierarchies."""
        group = ", ".join(dimension_cols)
        sql = f"SELECT {group}, {agg}({measure}) AS agg_value FROM {self.fact_table} GROUP BY {group} ORDER BY {group}"
        self.cursor.execute(sql)
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, r)) for r in self.cursor.fetchall()]

    def slice_cube(self, dimension_col: str, value: Any, measure: str, agg: str = "SUM") -> List[Dict]:
        """Fix one dimension to a specific value and aggregate."""
        sql = (
            f"SELECT {dimension_col}, {agg}({measure}) AS agg_value "
            f"FROM {self.fact_table} WHERE {dimension_col} = ? "
            f"GROUP BY {dimension_col}"
        )
        self.cursor.execute(sql, (value,))
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, r)) for r in self.cursor.fetchall()]

    def dice_cube(self, conditions: Dict[str, Any], measure: str, agg: str = "SUM") -> List[Dict]:
        """Apply multiple dimension filters and aggregate."""
        where_parts = [f"{col} = ?" for col in conditions]
        cols_select = ", ".join(conditions.keys())
        sql = (
            f"SELECT {cols_select}, {agg}({measure}) AS agg_value "
            f"FROM {self.fact_table} WHERE {' AND '.join(where_parts)} "
            f"GROUP BY {cols_select}"
        )
        self.cursor.execute(sql, list(conditions.values()))
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, r)) for r in self.cursor.fetchall()]

    def pivot(self, row_dim: str, col_dim: str, measure: str, agg: str = "SUM") -> Dict[str, Dict]:
        """Create a pivot table from two dimensions and a measure."""
        sql = (
            f"SELECT {row_dim}, {col_dim}, {agg}({measure}) AS agg_value "
            f"FROM {self.fact_table} GROUP BY {row_dim}, {col_dim}"
        )
        self.cursor.execute(sql)
        pivot_data: Dict[str, Dict] = {}
        for row in self.cursor.fetchall():
            r_val, c_val, m_val = row
            pivot_data.setdefault(str(r_val), {})[str(c_val)] = m_val
        return pivot_data


class QueryOptimizerAnalyzer:
    """Analyzes query execution plans and suggests optimizations."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def explain(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Return the EXPLAIN QUERY PLAN output."""
        self.cursor.execute(f"EXPLAIN QUERY PLAN {sql}", params)
        cols = [d[0] for d in self.cursor.description]
        return [dict(zip(cols, r)) for r in self.cursor.fetchall()]

    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Gather statistics about a table."""
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = self.cursor.fetchone()[0]
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [
            {"name": r[1], "type": r[2], "nullable": not r[3], "pk": bool(r[5])}
            for r in self.cursor.fetchall()
        ]
        self.cursor.execute(f"PRAGMA index_list({table_name})")
        indexes = [{"name": r[1], "unique": bool(r[2])} for r in self.cursor.fetchall()]
        return {
            "table": table_name,
            "row_count": row_count,
            "columns": columns,
            "indexes": indexes,
        }

    def suggest_indexes(self, table_name: str, query_columns: List[str]) -> List[str]:
        """Suggest indexes that could benefit the given query columns."""
        existing = {idx["name"] for idx in self.analyze_table(table_name)["indexes"]}
        suggestions = []
        for col in query_columns:
            idx_name = f"idx_{table_name}_{col}"
            if idx_name not in existing:
                suggestions.append(
                    f"CREATE INDEX {idx_name} ON {table_name}({col})"
                )
        return suggestions

    def create_index(self, table_name: str, column: str, unique: bool = False) -> str:
        """Create an index on a column."""
        unique_kw = "UNIQUE " if unique else ""
        idx_name = f"idx_{table_name}_{column}"
        sql = f"CREATE {unique_kw}INDEX IF NOT EXISTS {idx_name} ON {table_name}({column})"
        self.cursor.execute(sql)
        self.conn.commit()
        return idx_name


class DateDimensionGenerator:
    """Generates a date dimension table for a warehouse calendar."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()

    def create_date_dimension(
        self, table_name: str = "dim_date", start: str = "2020-01-01", end: str = "2030-12-31"
    ) -> int:
        """Populate a date dimension with calendar attributes."""
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date_key INTEGER PRIMARY KEY,
                full_date TEXT,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                month_name TEXT,
                week INTEGER,
                day_of_month INTEGER,
                day_of_week INTEGER,
                day_name TEXT,
                is_weekend INTEGER,
                is_month_start INTEGER,
                is_month_end INTEGER
            )
        """)
        start_dt = datetime.strptime(start, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end, "%Y-%m-%d").date()
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        month_names = [
            "", "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ]
        rows = []
        current = start_dt
        while current <= end_dt:
            next_day = current + timedelta(days=1)
            rows.append((
                int(current.strftime("%Y%m%d")),
                current.isoformat(),
                current.year,
                (current.month - 1) // 3 + 1,
                current.month,
                month_names[current.month],
                current.isocalendar()[1],
                current.day,
                current.weekday(),
                day_names[current.weekday()],
                1 if current.weekday() >= 5 else 0,
                1 if current.day == 1 else 0,
                1 if next_day.month != current.month else 0,
            ))
            current = next_day

        self.cursor.executemany(
            f"INSERT OR IGNORE INTO {table_name} VALUES ({','.join(['?']*13)})",
            rows,
        )
        self.conn.commit()
        return len(rows)
