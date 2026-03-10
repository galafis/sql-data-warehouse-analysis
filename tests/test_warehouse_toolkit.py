"""
Tests for SQL Data Warehouse Analysis Toolkit.
"""

import sqlite3
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from warehouse_toolkit import (
    StarSchemaBuilder,
    SCDType2Manager,
    ETLPipeline,
    OLAPCubeSimulator,
    QueryOptimizerAnalyzer,
    DateDimensionGenerator,
)


# ── Star Schema Tests ─────────────────────────────────────────────────

class TestStarSchemaBuilder:
    def setup_method(self):
        self.builder = StarSchemaBuilder(":memory:")

    def teardown_method(self):
        self.builder.close()

    def test_create_dimension_table(self):
        self.builder.create_dimension_table(
            "dim_product", {"product_name": "TEXT", "category": "TEXT"}
        )
        info = self.builder.get_schema_info()
        assert "dim_product" in info["dimensions"]
        assert info["dimension_count"] == 1

    def test_create_fact_table(self):
        self.builder.create_dimension_table("dim_customer", {"name": "TEXT"})
        self.builder.create_fact_table(
            "fact_sales",
            measures={"amount": "REAL", "quantity": "INTEGER"},
            foreign_keys={"customer_sk": "dim_customer"},
        )
        info = self.builder.get_schema_info()
        assert "fact_sales" in info["facts"]
        assert info["fact_count"] == 1

    def test_insert_dimension_row(self):
        self.builder.create_dimension_table("dim_store", {"store_name": "TEXT", "city": "TEXT"})
        sk = self.builder.insert_dimension_row("dim_store", {"store_name": "HQ", "city": "SP"})
        assert sk == 1

    def test_insert_fact_row(self):
        self.builder.create_dimension_table("dim_product", {"name": "TEXT"})
        pk = self.builder.insert_dimension_row("dim_product", {"name": "Widget"})
        self.builder.create_fact_table(
            "fact_orders",
            measures={"total": "REAL"},
            foreign_keys={"product_sk": "dim_product"},
        )
        fk = self.builder.insert_fact_row("fact_orders", {"product_sk": pk, "total": 99.5})
        assert fk == 1

    def test_schema_info_counts(self):
        self.builder.create_dimension_table("d1", {"a": "TEXT"})
        self.builder.create_dimension_table("d2", {"b": "TEXT"})
        self.builder.create_fact_table("f1", {"m": "REAL"}, {"d1_sk": "d1"})
        info = self.builder.get_schema_info()
        assert info["dimension_count"] == 2
        assert info["fact_count"] == 1


# ── SCD Type 2 Tests ──────────────────────────────────────────────────

class TestSCDType2Manager:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.scd = SCDType2Manager(self.conn, "dim_customer")
        self.scd.create_scd2_table({"customer_id": "TEXT", "name": "TEXT", "city": "TEXT"})

    def teardown_method(self):
        self.conn.close()

    def test_initial_insert(self):
        result = self.scd.upsert(
            "customer_id",
            {"customer_id": "C001", "name": "Alice", "city": "SP"},
            "2024-01-01",
        )
        assert result == "inserted"

    def test_unchanged_upsert(self):
        self.scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "SP"}, "2024-01-01")
        result = self.scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "SP"}, "2024-06-01")
        assert result == "unchanged"

    def test_updated_upsert_creates_history(self):
        self.scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "SP"}, "2024-01-01")
        self.scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "RJ"}, "2024-06-01")
        history = self.scd.get_history("customer_id", "C001")
        assert len(history) == 2
        assert history[0]["is_current"] == 0
        assert history[1]["is_current"] == 1

    def test_history_valid_dates(self):
        self.scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "SP"}, "2024-01-01")
        self.scd.upsert("customer_id", {"customer_id": "C001", "name": "Alice", "city": "RJ"}, "2024-06-01")
        history = self.scd.get_history("customer_id", "C001")
        assert history[0]["valid_to"] == "2024-06-01"
        assert history[1]["valid_from"] == "2024-06-01"


# ── ETL Pipeline Tests ────────────────────────────────────────────────

class TestETLPipeline:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE src (id INTEGER, val TEXT)")
        self.conn.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b')")
        self.conn.commit()
        self.pipeline = ETLPipeline(self.conn)

    def teardown_method(self):
        self.conn.close()

    def test_add_and_run_step(self):
        self.pipeline.add_step("create_dest", "CREATE TABLE dest AS SELECT * FROM src")
        results = self.pipeline.run()
        assert len(results) == 1
        assert results[0]["status"] == "success"

    def test_failed_step(self):
        self.pipeline.add_step("bad_sql", "SELECT * FROM nonexistent_table")
        results = self.pipeline.run()
        assert results[0]["status"] == "failed"
        assert results[0]["error"] is not None

    def test_log_preserved(self):
        self.pipeline.add_step("s1", "SELECT 1")
        self.pipeline.run()
        assert len(self.pipeline.get_log()) == 1


# ── OLAP Cube Tests ───────────────────────────────────────────────────

class TestOLAPCubeSimulator:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            "CREATE TABLE fact_sales (region TEXT, product TEXT, amount REAL)"
        )
        rows = [
            ("North", "A", 100), ("North", "B", 200),
            ("South", "A", 150), ("South", "B", 250),
        ]
        self.conn.executemany("INSERT INTO fact_sales VALUES (?, ?, ?)", rows)
        self.conn.commit()
        self.cube = OLAPCubeSimulator(self.conn, "fact_sales")

    def teardown_method(self):
        self.conn.close()

    def test_rollup(self):
        result = self.cube.rollup(["region"], "amount")
        assert len(result) == 2
        totals = {r["region"]: r["agg_value"] for r in result}
        assert totals["North"] == 300
        assert totals["South"] == 400

    def test_slice_cube(self):
        result = self.cube.slice_cube("region", "North", "amount")
        assert result[0]["agg_value"] == 300

    def test_dice_cube(self):
        result = self.cube.dice_cube({"region": "South", "product": "A"}, "amount")
        assert result[0]["agg_value"] == 150

    def test_pivot(self):
        pivot = self.cube.pivot("region", "product", "amount")
        assert pivot["North"]["A"] == 100
        assert pivot["South"]["B"] == 250


# ── Query Optimizer Tests ─────────────────────────────────────────────

class TestQueryOptimizerAnalyzer:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total REAL)")
        self.conn.execute("INSERT INTO orders VALUES (1, 'Alice', 100)")
        self.conn.commit()
        self.analyzer = QueryOptimizerAnalyzer(self.conn)

    def teardown_method(self):
        self.conn.close()

    def test_explain(self):
        plan = self.analyzer.explain("SELECT * FROM orders WHERE customer = ?", ("Alice",))
        assert isinstance(plan, list)
        assert len(plan) > 0

    def test_analyze_table(self):
        info = self.analyzer.analyze_table("orders")
        assert info["row_count"] == 1
        assert info["table"] == "orders"
        assert len(info["columns"]) == 3

    def test_suggest_indexes(self):
        suggestions = self.analyzer.suggest_indexes("orders", ["customer"])
        assert len(suggestions) == 1
        assert "customer" in suggestions[0]

    def test_create_index(self):
        idx = self.analyzer.create_index("orders", "customer")
        assert idx == "idx_orders_customer"
        info = self.analyzer.analyze_table("orders")
        idx_names = [i["name"] for i in info["indexes"]]
        assert "idx_orders_customer" in idx_names


# ── Date Dimension Tests ──────────────────────────────────────────────

class TestDateDimensionGenerator:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.gen = DateDimensionGenerator(self.conn)

    def teardown_method(self):
        self.conn.close()

    def test_generate_date_dimension(self):
        count = self.gen.create_date_dimension("dim_date", "2024-01-01", "2024-12-31")
        assert count == 366  # 2024 is a leap year

    def test_date_attributes(self):
        self.gen.create_date_dimension("dim_date", "2024-01-01", "2024-01-01")
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM dim_date WHERE date_key = 20240101")
        row = cur.fetchone()
        assert row is not None
        assert row[2] == 2024  # year
        assert row[4] == 1     # month
        assert row[5] == "January"  # month_name

    def test_weekend_flag(self):
        self.gen.create_date_dimension("dim_date", "2024-01-06", "2024-01-07")
        cur = self.conn.cursor()
        cur.execute("SELECT is_weekend FROM dim_date")
        rows = cur.fetchall()
        assert all(r[0] == 1 for r in rows)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
