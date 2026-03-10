import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

def transform_products(raw_products: list[dict]) -> pd.DataFrame:
    
    logger.info("[TRANSFORM] Iniciando transformação de produtos...")

    df = pd.DataFrame(raw_products)

    df = df.dropna(subset=["id", "price", "category"])
    df = df.drop_duplicates(subset=["id"])

    df["rating_rate"] = df["rating"].apply(lambda x: x.get("rate", 0) if isinstance(x, dict) else 0)
    df["rating_count"] = df["rating"].apply(lambda x: x.get("count", 0) if isinstance(x, dict) else 0)
    df = df.drop(columns=["rating"])

    df["category_clean"] = df["category"].str.strip().str.title()
    df["title_clean"] = df["title"].str.strip().str.slice(0, 60)

    USD_TO_BRL = 5.05
    df["price_brl"] = (df["price"] * USD_TO_BRL).round(2)

    df["rating_class"] = pd.cut(
        df["rating_rate"],
        bins=[0, 3.0, 4.0, 5.0],
        labels=["baixo", "médio", "alto"],
        right=True,
    )

    df["revenue_potential"] = (df["price"] * df["rating_count"]).round(2)

    df["price_range"] = pd.cut(
        df["price"],
        bins=[0, 25, 75, 200, float("inf")],
        labels=["econômico", "intermediário", "premium", "luxo"],
    )

    df["transformed_at"] = datetime.now().isoformat()

    cols = [
        "id", "title_clean", "category_clean", "price", "price_brl",
        "price_range", "rating_rate", "rating_count", "rating_class",
        "revenue_potential", "transformed_at",
    ]
    df = df[cols].rename(columns={"title_clean": "title", "category_clean": "category"})

    logger.info(f"[TRANSFORM] Produtos transformados: {len(df)} registros")
    return df

def transform_carts(raw_carts: list[dict], products_df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("[TRANSFORM] Iniciando transformação de carrinhos...")

    rows = []
    for cart in raw_carts:
        for product in cart.get("products", []):
            rows.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "date": cart["date"],
                "product_id": product["productId"],
                "quantity": product["quantity"],
            })

    df = pd.DataFrame(rows)

    price_lookup = products_df[["id", "price", "price_brl", "category"]].rename(
        columns={"id": "product_id"}
    )
    df = df.merge(price_lookup, on="product_id", how="left")

    df["item_revenue_usd"] = (df["price"] * df["quantity"]).round(2)
    df["item_revenue_brl"] = (df["price_brl"] * df["quantity"]).round(2)

    df["date"] = pd.to_datetime(df["date"]).dt.date

    df["transformed_at"] = datetime.now().isoformat()

    logger.info(f"[TRANSFORM] Itens de pedido transformados: {len(df)} registros")
    return df

def build_category_summary(products_df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("[TRANSFORM] Construindo sumário por categoria...")

    summary = (
        products_df.groupby("category")
        .agg(
            total_products=("id", "count"),
            avg_price_usd=("price", "mean"),
            avg_price_brl=("price_brl", "mean"),
            min_price=("price", "min"),
            max_price=("price", "max"),
            avg_rating=("rating_rate", "mean"),
            total_reviews=("rating_count", "sum"),
            total_revenue_potential=("revenue_potential", "sum"),
        )
        .reset_index()
    )

    summary["avg_price_usd"] = summary["avg_price_usd"].round(2)
    summary["avg_price_brl"] = summary["avg_price_brl"].round(2)
    summary["avg_rating"] = summary["avg_rating"].round(2)
    summary["total_revenue_potential"] = summary["total_revenue_potential"].round(2)
    summary["created_at"] = datetime.now().isoformat()

    logger.info(f"[TRANSFORM] Categorias sumarizadas: {len(summary)}")
    return summary

def build_top_products(products_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    
    return (
        products_df.nlargest(top_n, "revenue_potential")[
            ["id", "title", "category", "price", "rating_rate", "rating_count", "revenue_potential"]
        ]
        .reset_index(drop=True)
    )

def run_transform(raw_data: dict) -> dict:
    
    products_df = transform_products(raw_data["products"])
    carts_df = transform_carts(raw_data["carts"], products_df)
    category_summary_df = build_category_summary(products_df)
    top_products_df = build_top_products(products_df)

    return {
        "products": products_df,
        "order_items": carts_df,
        "category_summary": category_summary_df,
        "top_products": top_products_df,
    }
