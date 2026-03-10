import json
from unittest.mock import MagicMock, patch

import pytest

from src.extract.api_client import FakeStoreAPIClient, save_raw_data

MOCK_PRODUCTS = [
    {"id": 1, "title": "Product A", "price": 29.99, "category": "electronics",
     "rating": {"rate": 4.2, "count": 150}},
    {"id": 2, "title": "Product B", "price": 9.99, "category": "clothing",
     "rating": {"rate": 3.8, "count": 80}},
]
MOCK_CATEGORIES = ["electronics", "clothing", "jewelery", "men's clothing"]
MOCK_CARTS = [
    {"id": 1, "userId": 1, "date": "2023-01-01", "products": [{"productId": 1, "quantity": 2}]},
]
MOCK_USERS = [{"id": 1, "email": "user@test.com", "username": "testuser"}]

@pytest.fixture
def client():
    return FakeStoreAPIClient()

class TestFakeStoreAPIClient:
    def test_fetch_products_returns_list(self, client):
        with patch.object(client, "_get", return_value=MOCK_PRODUCTS):
            result = client.fetch_products()
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_fetch_categories_returns_list(self, client):
        with patch.object(client, "_get", return_value=MOCK_CATEGORIES):
            result = client.fetch_categories()
        assert isinstance(result, list)
        assert "electronics" in result

    def test_fetch_all_returns_complete_dict(self, client):
        with patch.object(client, "fetch_products", return_value=MOCK_PRODUCTS), \
             patch.object(client, "fetch_categories", return_value=MOCK_CATEGORIES), \
             patch.object(client, "fetch_carts", return_value=MOCK_CARTS), \
             patch.object(client, "fetch_users", return_value=MOCK_USERS):
            result = client.fetch_all()

        assert "products" in result
        assert "categories" in result
        assert "carts" in result
        assert "users" in result
        assert "extracted_at" in result

    def test_retry_on_connection_error(self, client):
        
        import requests
        with patch.object(client.session, "get",
                          side_effect=requests.exceptions.ConnectionError):
            with pytest.raises(requests.exceptions.ConnectionError):
                client._get("/products")

    def test_http_error_raises(self, client):
        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        with patch.object(client.session, "get", return_value=mock_response):
            with pytest.raises(requests.exceptions.HTTPError):
                client._get("/nonexistent")

class TestSaveRawData:
    def test_saves_json_file(self, tmp_path):
        data = {"products": MOCK_PRODUCTS, "extracted_at": "2024-01-01T00:00:00"}
        filepath = save_raw_data(data, raw_dir=str(tmp_path))
        assert filepath.endswith(".json")
        with open(filepath) as f:
            loaded = json.load(f)
        assert loaded["products"][0]["id"] == 1

    def test_creates_directory_if_not_exists(self, tmp_path):
        new_dir = str(tmp_path / "new" / "raw")
        data = {"products": []}
        save_raw_data(data, raw_dir=new_dir)
        import os
        assert os.path.isdir(new_dir)
