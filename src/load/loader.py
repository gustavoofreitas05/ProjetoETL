import logging
import os
import sqlite3
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/ecommerce.db")

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS products (
    id                  INTEGER PRIMARY KEY,
    title               TEXT    NOT NULL,
    category            TEXT    NOT NULL,
    price               REAL    NOT NULL,
    price_brl           REAL,
    price_range         TEXT,
    rating_rate         REAL,
    rating_count        INTEGER,
    rating_class        TEXT,
    revenue_potential   REAL,
    transformed_at      TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    cart_id             INTEGER NOT NULL,
    user_id             INTEGER NOT NULL,
    date                TEXT,
    product_id          INTEGER NOT NULL,
    quantity            INTEGER NOT NULL,
    price               REAL,
    price_brl           REAL,
    category            TEXT,
    item_revenue_usd    REAL,
    item_revenue_brl    REAL,
    transformed_at      TEXT,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS category_summary (
    category                TEXT PRIMARY KEY,
    total_products          INTEGER,
    avg_price_usd           REAL,
    avg_price_brl           REAL,
    min_price               REAL,
    max_price               REAL,
    avg_rating              REAL,
    total_reviews           INTEGER,
    total_revenue_potential REAL,
    created_at              TEXT
);

CREATE TABLE IF NOT EXISTS top_products (
    id                  INTEGER PRIMARY KEY,
    title               TEXT,
    category            TEXT,
    price               REAL,
    rating_rate         REAL,
    rating_count        INTEGER,
    revenue_potential   REAL
);
"""


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    logger.info("[LOAD] Criando schema no SQLite...")
    conn.executescript(CREATE_TABLES_SQL)
    conn.commit()
    logger.info("[LOAD] Schema criado com sucesso.")


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection,
    if_exists: str = "replace",
) -> None:
    df = df.copy()
    for col in df.select_dtypes(include="category").columns:
        df[col] = df[col].astype(str)
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    logger.info(f"[LOAD] Tabela '{table_name}' carregada — {len(df)} registros.")


def export_processed_csvs(dataframes: dict, processed_dir: str = "data/processed") -> None:
    Path(processed_dir).mkdir(parents=True, exist_ok=True)
    for name, df in dataframes.items():
        df_copy = df.copy()
        for col in df_copy.select_dtypes(include="category").columns:
            df_copy[col] = df_copy[col].astype(str)
        path = os.path.join(processed_dir, f"{name}.csv")
        df_copy.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"[LOAD] CSV exportado: {path}")


def run_analytics_queries(conn: sqlite3.Connection) -> dict:
    logger.info("[LOAD] Executando queries analíticas...")

    queries = {
        "receita_por_categoria": """
            SELECT
                category,
                COUNT(*)                        AS total_products,
                ROUND(AVG(price), 2)            AS avg_price_usd,
                ROUND(SUM(revenue_potential), 2) AS total_revenue_potential,
                ROUND(AVG(rating_rate), 2)      AS avg_rating
            FROM products
            GROUP BY category
            ORDER BY total_revenue_potential DESC;
        """,
        "top5_produtos_receita": """
            SELECT
                id,
                title,
                category,
                price,
                rating_rate,
                rating_count,
                ROUND(revenue_potential, 2) AS revenue_potential
            FROM products
            ORDER BY revenue_potential DESC
            LIMIT 5;
        """,
        "distribuicao_faixa_preco": """
            SELECT
                price_range,
                COUNT(*)            AS total_products,
                ROUND(AVG(price), 2) AS avg_price,
                ROUND(MIN(price), 2) AS min_price,
                ROUND(MAX(price), 2) AS max_price
            FROM products
            GROUP BY price_range
            ORDER BY avg_price;
        """,
        "analise_avaliacoes": """
            SELECT
                rating_class,
                COUNT(*)                        AS total_products,
                ROUND(AVG(price), 2)            AS avg_price,
                ROUND(AVG(rating_rate), 2)      AS avg_rating,
                SUM(rating_count)               AS total_reviews
            FROM products
            GROUP BY rating_class
            ORDER BY avg_rating DESC;
        """,
        "pedidos_por_categoria": """
            SELECT
                oi.category,
                COUNT(DISTINCT oi.cart_id)      AS total_orders,
                SUM(oi.quantity)                AS total_items,
                ROUND(SUM(oi.item_revenue_usd), 2) AS total_revenue_usd,
                ROUND(SUM(oi.item_revenue_brl), 2) AS total_revenue_brl
            FROM order_items oi
            GROUP BY oi.category
            ORDER BY total_revenue_usd DESC;
        """,
    }

    results = {}
    for name, sql in queries.items():
        df = pd.read_sql_query(sql, conn)
        results[name] = df
        logger.info(f"[ANALYTICS] Query '{name}' → {len(df)} linhas")

    return results


def run_load(dataframes: dict) -> dict:
    conn = get_connection()

    try:
        create_schema(conn)

        for table_name, df in dataframes.items():
            load_dataframe(df, table_name, conn)

        export_processed_csvs(dataframes)

        analytics = run_analytics_queries(conn)
        export_processed_csvs(analytics, processed_dir="data/processed/analytics")

        conn.commit()
        logger.info(f"[LOAD] Pipeline de carga concluído. Banco: {DB_PATH}")

        return analytics

    except Exception as e:
        conn.rollback()
        logger.error(f"[LOAD] Erro durante a carga: {e}")
        raise

    finally:
        conn.close()
