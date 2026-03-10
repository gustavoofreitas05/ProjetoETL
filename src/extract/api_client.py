import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class FakeStoreAPIClient:
    
    def __init__(self):
        self.base_url = os.getenv("API_BASE_URL", "https://fakestoreapi.com")
        self.timeout = int(os.getenv("API_TIMEOUT", 30))
        self.retries = int(os.getenv("API_RETRIES", 3))
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, endpoint: str) -> dict | list:
        
        url = f"{self.base_url}{endpoint}"
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(f"[EXTRACT] GET {url} (tentativa {attempt}/{self.retries})")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                logger.info(f"[EXTRACT] Sucesso — status {response.status_code}")
                return response.json()
            except requests.exceptions.HTTPError as e:
                logger.error(f"[EXTRACT] HTTP error: {e}")
                raise
            except requests.exceptions.ConnectionError:
                logger.warning(f"[EXTRACT] Conexão falhou. Tentativa {attempt}/{self.retries}")
                if attempt < self.retries:
                    time.sleep(2 ** attempt)
                else:
                    raise
            except requests.exceptions.Timeout:
                logger.warning(f"[EXTRACT] Timeout. Tentativa {attempt}/{self.retries}")
                if attempt < self.retries:
                    time.sleep(2)
                else:
                    raise

    def fetch_products(self) -> list[dict]:
        
        return self._get("/products")

    def fetch_categories(self) -> list[str]:
        
        return self._get("/products/categories")

    def fetch_carts(self) -> list[dict]:
        
        return self._get("/carts")

    def fetch_users(self) -> list[dict]:
        
        return self._get("/users")

    def fetch_all(self) -> dict:
        
        logger.info("[EXTRACT] Iniciando extração completa da API...")
        data = {
            "products": self.fetch_products(),
            "categories": self.fetch_categories(),
            "carts": self.fetch_carts(),
            "users": self.fetch_users(),
            "extracted_at": datetime.now().isoformat(),
        }
        logger.info(
            f"[EXTRACT] Extração concluída — "
            f"{len(data['products'])} produtos | "
            f"{len(data['carts'])} carrinhos | "
            f"{len(data['users'])} usuários"
        )
        return data

def save_raw_data(data: dict, raw_dir: str = "data/raw") -> str:
    
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(raw_dir, f"raw_data_{timestamp}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"[EXTRACT] Dados brutos salvos em: {filepath}")
    return filepath

def run_extract() -> dict:
    
    client = FakeStoreAPIClient()
    data = client.fetch_all()
    save_raw_data(data)
    return data
