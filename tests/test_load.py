import sqlite3

import pandas as pd
import pytest

from src.load.loader import (
    create_schema,
    export_processed_csvs,
    get_connection,
    load_dataframe,
    run_analytics_queries,
)

@pytest.fixture
def conn(tmp_path):
    
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()

@pytest.fixture
def sample_products_df():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "title": ["Laptop", "T-Shirt", "Ring"],
        "category": ["Electronics", "Clothing", "Jewelery"],
        "price": [999.99, 15.99, 350.00],
        "price_brl": [5049.95, 80.75, 1767.50],
        "price_range": ["luxo", "econômico", "premium"],
        "rating_rate": [4.5, 3.2, 4.8],
        "rating_count": [200, 50, 300],
        "rating_class": ["alto", "baixo", "alto"],
        "revenue_potential": [199998.0, 799.5, 105000.0],
        "transformed_at": ["2024-01-01"] * 3,
    })

@pytest.fixture
def sample_order_items_df():
    return pd.DataFrame({
        "cart_id": [1, 1, 2],
        "user_id": [10, 10, 20],
        "date": ["2023-06-15", "2023-06-15", "2023-06-16"],
        "product_id": [1, 2, 3],
        "quantity": [1, 3, 2],
        "price": [999.99, 15.99, 350.00],
        "price_brl": [5049.95, 80.75, 1767.50],
        "category": ["Electronics", "Clothing", "Jewelery"],
        "item_revenue_usd": [999.99, 47.97, 700.00],
        "item_revenue_brl": [5049.95, 242.25, 3535.00],
        "transformed_at": ["2024-01-01"] * 3,
    })

class TestCreateSchema:
    def test_creates_tables(self, conn):
        create_schema(conn)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert "products" in tables
        assert "order_items" in tables
        assert "category_summary" in tables

    def test_idempotent(self, conn):
        
        create_schema(conn)
        create_schema(conn)

class TestLoadDataframe:
    def test_loads_correctly(self, conn, sample_products_df):
        create_schema(conn)
        load_dataframe(sample_products_df, "products", conn)
        result = pd.read_sql("SELECT * FROM products", conn)
        assert len(result) == 3

    def test_replace_mode(self, conn, sample_products_df):
        create_schema(conn)
        load_dataframe(sample_products_df, "products", conn)
        load_dataframe(sample_products_df, "products", conn, if_exists="replace")
        result = pd.read_sql("SELECT * FROM products", conn)
        assert len(result) == 3

class TestExportCSVs:
    def test_creates_csv_files(self, tmp_path, sample_products_df):
        dataframes = {"products": sample_products_df}
        export_processed_csvs(dataframes, processed_dir=str(tmp_path))
        assert (tmp_path / "products.csv").exists()

    def test_csv_content_correct(self, tmp_path, sample_products_df):
        export_processed_csvs({"products": sample_products_df}, str(tmp_path))
        loaded = pd.read_csv(tmp_path / "products.csv")
        assert len(loaded) == 3
        assert "title" in loaded.columns

class TestAnalyticsQueries:
    def test_queries_return_dataframes(self, conn, sample_products_df, sample_order_items_df):
        create_schema(conn)
        load_dataframe(sample_products_df, "products", conn)
        load_dataframe(sample_order_items_df, "order_items", conn)

        results = run_analytics_queries(conn)

        assert isinstance(results, dict)
        assert "receita_por_categoria" in results
        assert "top5_produtos_receita" in results
        assert all(isinstance(df, pd.DataFrame) for df in results.values())

    def test_top5_has_max_5_rows(self, conn, sample_products_df, sample_order_items_df):
        create_schema(conn)
        load_dataframe(sample_products_df, "products", conn)
        load_dataframe(sample_order_items_df, "order_items", conn)

        results = run_analytics_queries(conn)
        assert len(results["top5_produtos_receita"]) <= 5
