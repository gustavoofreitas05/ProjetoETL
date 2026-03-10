import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

Path("logs").mkdir(exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/pipeline.log")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("etl_pipeline")

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract.api_client import run_extract
from src.transform.transformer import run_transform
from src.load.loader import run_load

BANNER = """
╔══════════════════════════════════════════════════╗
║      🛒  Pipeline ETL — E-commerce Analytics     ║
║         Extract → Transform → Load               ║
╚══════════════════════════════════════════════════╝
"""

def run_pipeline() -> None:
    print(BANNER)
    start_time = time.time()
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"{'='*55}")
    logger.info(f"PIPELINE INICIADO EM {run_at}")
    logger.info(f"{'='*55}")

    try:

        logger.info(">>> FASE 1/3: EXTRACT")
        t0 = time.time()
        raw_data = run_extract()
        logger.info(f"✔ EXTRACT concluído em {time.time() - t0:.2f}s\n")

        logger.info(">>> FASE 2/3: TRANSFORM")
        t0 = time.time()
        dataframes = run_transform(raw_data)
        logger.info(f"✔ TRANSFORM concluído em {time.time() - t0:.2f}s\n")

        logger.info(">>> FASE 3/3: LOAD")
        t0 = time.time()
        analytics = run_load(dataframes)
        logger.info(f"✔ LOAD concluído em {time.time() - t0:.2f}s\n")

        elapsed = time.time() - start_time
        logger.info(f"{'='*55}")
        logger.info(f"✅ PIPELINE CONCLUÍDO COM SUCESSO em {elapsed:.2f}s")
        logger.info(f"{'='*55}")

        _print_summary(dataframes, analytics, elapsed)

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"❌ PIPELINE FALHOU após {elapsed:.2f}s: {e}", exc_info=True)
        sys.exit(1)

def _print_summary(dataframes: dict, analytics: dict, elapsed: float) -> None:
    print("\n" + "─" * 55)
    print("📦  RESUMO DA EXECUÇÃO")
    print("─" * 55)

    print("\n🗃️  Tabelas carregadas:")
    for name, df in dataframes.items():
        print(f"   • {name:<25} {len(df):>4} registros")

    print("\n📊  KPIs gerados:")
    for name, df in analytics.items():
        print(f"   • {name:<35} {len(df):>2} linhas")

    print(f"\n⏱️  Tempo total: {elapsed:.2f}s")
    print(f"💾  Banco de dados: {os.getenv('DB_PATH', 'data/ecommerce.db')}")
    print(f"📁  CSVs em:       data/processed/")
    print(f"📝  Logs em:       logs/pipeline.log")
    print("─" * 55 + "\n")

    if "receita_por_categoria" in analytics:
        print("🏆  Receita Potencial por Categoria:")
        df = analytics["receita_por_categoria"]
        for _, row in df.iterrows():
            bar = "█" * int(row["total_revenue_potential"] / 5000)
            print(f"   {row['category']:<30} ${row['total_revenue_potential']:>10,.2f}  {bar}")
    print()

if __name__ == "__main__":
    run_pipeline()
