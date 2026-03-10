# 🛒 Pipeline ETL — E-commerce Sales Analytics

> Pipeline ETL completo que consome dados de uma API REST de e-commerce, realiza transformações com Python + Pandas e persiste os resultados em SQLite com SQL analítico.

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![SQLite](https://img.shields.io/badge/SQLite-3-lightblue?logo=sqlite)
![Pandas](https://img.shields.io/badge/Pandas-2.0-purple?logo=pandas)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📐 Arquitetura

```
API REST (FakeStore API)
        │
        ▼
  [EXTRACT] ──► Raw JSON salvo em data/raw/
        │
        ▼
  [TRANSFORM] ──► Limpeza, enriquecimento e agregações com Pandas
        │
        ▼
   [LOAD] ──► SQLite (data/ecommerce.db)
        │
        ▼
  [ANALYTICS] ──► Queries SQL para KPIs e relatórios
```

---

## 🗂️ Estrutura do Projeto

```
etl_ecommerce/
├── src/
│   ├── extract/
│   │   └── api_client.py        # Extração via API REST
│   ├── transform/
│   │   └── transformer.py       # Limpeza e transformações
│   ├── load/
│   │   └── loader.py            # Carga no SQLite
│   └── pipeline.py              # Orquestrador principal
├── sql/
│   ├── create_tables.sql        # DDL das tabelas
│   └── analytics_queries.sql    # Queries analíticas / KPIs
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/
│   ├── raw/                     # JSON bruto da API
│   └── processed/               # CSVs transformados
├── logs/                        # Logs de execução
├── notebooks/
│   └── exploratory_analysis.ipynb
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🚀 Como executar

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/etl-ecommerce.git
cd etl-ecommerce
```

### 2. Crie o ambiente virtual
```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows
```

### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

### 4. Configure as variáveis de ambiente
```bash
cp .env.example .env
# Edite o .env se necessário
```

### 5. Execute o pipeline completo
```bash
python src/pipeline.py
```

---

## 📊 KPIs gerados pelo pipeline

| Métrica | Descrição |
|---|---|
| `total_revenue` | Receita total por categoria |
| `avg_ticket` | Ticket médio por categoria |
| `top_products` | Top 5 produtos por receita |
| `rating_analysis` | Análise de avaliações vs vendas |
| `price_distribution` | Distribuição de preços por categoria |

---

## 🔄 Fases do ETL

### Extract
- Consome a [FakeStore API](https://fakestoreapi.com/) (produtos, usuários e pedidos)
- Salva o JSON bruto em `data/raw/` com timestamp
- Implementa retry automático e logging de erros

### Transform
- Remove duplicatas e valores nulos
- Normaliza campos de texto (categorias, títulos)
- Calcula colunas derivadas: `revenue`, `discount_price`, `rating_class`
- Agrega dados por categoria e por faixa de preço

### Load
- Cria schema no SQLite via DDL em `sql/create_tables.sql`
- Carrega as tabelas `products`, `categories_summary` e `price_analysis`
- Executa queries analíticas e exporta CSVs em `data/processed/`

---

## 🧪 Testes

```bash
pytest tests/ -v
```

---

## 🛠️ Tecnologias

- **Python 3.10+**
- **Pandas** — transformações e agregações
- **Requests** — consumo da API REST
- **SQLite3** — banco de dados relacional leve
- **python-dotenv** — variáveis de ambiente
- **pytest** — testes unitários
- **logging** — rastreamento de execução

---

## 📄 Licença

MIT © Seu Nome
