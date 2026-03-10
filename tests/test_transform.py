import pandas as pd
import pytest

from src.transform.transformer import (
    build_category_summary,
    build_top_products,
    transform_carts,
    transform_products,
)

MOCK_PRODUCTS_RAW = [
    {"id": 1, "title": "Laptop Pro 15  ", "price": 999.99, "category": "electronics",
     "rating": {"rate": 4.5, "count": 200}, "description": "A great laptop"},
    {"id": 2, "title": "T-Shirt Basic", "price": 15.99, "category": "men's clothing",
     "rating": {"rate": 3.2, "count": 50}, "description": "Comfortable shirt"},
    {"id": 3, "title": "Gold Ring", "price": 350.00, "category": "jewelery",
     "rating": {"rate": 4.8, "count": 300}, "description": "Beautiful ring"},
    {"id": 1, "title": "Laptop Pro 15 (dup)", "price": 999.99, "category": "electronics",
     "rating": {"rate": 4.5, "count": 200}, "description": "Duplicate"},
]

MOCK_CARTS_RAW = [
    {"id": 1, "userId": 10, "date": "2023-06-15T00:00:00", "products": [
        {"productId": 1, "quantity": 1},
        {"productId": 2, "quantity": 3},
    ]},
    {"id": 2, "userId": 20, "date": "2023-06-16T00:00:00", "products": [
        {"productId": 3, "quantity": 2},
    ]},
]

@pytest.fixture
def products_df():
    return transform_products(MOCK_PRODUCTS_RAW)

class TestTransformProducts:
    def test_removes_duplicates(self, products_df):
        assert len(products_df) == 3

    def test_rating_flattened(self, products_df):
        assert "rating_rate" in products_df.columns
        assert "rating_count" in products_df.columns
        assert "rating" not in products_df.columns

    def test_category_normalized(self, products_df):
        categories = products_df["category"].tolist()
        assert "Electronics" in categories
        assert "Men'S Clothing" in categories

    def test_price_brl_calculated(self, products_df):
        usd = products_df[products_df["id"] == 1]["price"].values[0]
        brl = products_df[products_df["id"] == 1]["price_brl"].values[0]
        assert brl == round(usd * 5.05, 2)

    def test_rating_class_assigned(self, products_df):
        high_rating = products_df[products_df["id"] == 3]["rating_class"].values[0]
        assert str(high_rating) == "alto"

        low_rating = products_df[products_df["id"] == 2]["rating_class"].values[0]
        assert str(low_rating) == "baixo"

    def test_revenue_potential_calculated(self, products_df):
        row = products_df[products_df["id"] == 1].iloc[0]
        expected = round(row["price"] * row["rating_count"], 2)
        assert row["revenue_potential"] == expected

    def test_price_range_assigned(self, products_df):
        cheap = products_df[products_df["id"] == 2]["price_range"].values[0]
        assert str(cheap) == "econômico"

        expensive = products_df[products_df["id"] == 1]["price_range"].values[0]
        assert str(expensive) == "luxo"

class TestTransformCarts:
    def test_returns_dataframe(self, products_df):
        df = transform_carts(MOCK_CARTS_RAW, products_df)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, products_df):
        df = transform_carts(MOCK_CARTS_RAW, products_df)
        assert len(df) == 3

    def test_revenue_calculated(self, products_df):
        df = transform_carts(MOCK_CARTS_RAW, products_df)
        item = df[(df["cart_id"] == 2) & (df["product_id"] == 3)].iloc[0]
        assert item["quantity"] == 2
        assert item["item_revenue_usd"] > 0

    def test_join_with_products(self, products_df):
        df = transform_carts(MOCK_CARTS_RAW, products_df)
        assert "price" in df.columns
        assert "category" in df.columns

class TestCategorySummary:
    def test_correct_columns(self, products_df):
        summary = build_category_summary(products_df)
        expected_cols = [
            "category", "total_products", "avg_price_usd",
            "avg_rating", "total_revenue_potential",
        ]
        for col in expected_cols:
            assert col in summary.columns

    def test_correct_category_count(self, products_df):
        summary = build_category_summary(products_df)
        assert len(summary) == 3

class TestTopProducts:
    def test_returns_top_n(self, products_df):
        top = build_top_products(products_df, top_n=2)
        assert len(top) == 2

    def test_sorted_by_revenue(self, products_df):
        top = build_top_products(products_df, top_n=3)
        revenues = top["revenue_potential"].tolist()
        assert revenues == sorted(revenues, reverse=True)
