# LLM-Powered Booking Analytics & QA System

This project is a FastAPI-based backend for querying hotel booking analytics, powered by:
- PostgreSQL for structured data storage
- Weaviate for semantic search capabilities
- Ollama + local LLMs for vectorization and generation
- FastAPI for building and serving multiple endpoints

---

## 📁 Project Structure

```
hotel-booking-api/
│
├── data/                        # Raw or processed data files
├── db_setup/                   # Scripts to initialize and populate the database
│   └── insert.py
│   └── queries.sql
├── samples/                    # Sample queries and cached responses for reference
|   └── sample_caching_responses/
│   └── samples_queries.json    # Sample queries for reference
├── app.py                      # FastAPI app entry point
├── analytics_cache.py          # Caching analytics results
├── db_utils.py                 # Utility functions for DB operations
├── docker-compose.yaml         # Services: Weaviate + Postgres
├── requirements.txt            # Python dependencies
└── .gitattributes              # Git config
```

---

## 🚀 API Overview

This project exposes multiple endpoints for hotel booking data analysis. Here’s a brief overview:

### `POST /analytics/revenue`

### `POST /analytics/cancellations`

### `POST /analytics/geo`

### `POST /analytics/lead_time`

### `POST /analytics/others`

### `POST /ask`
> 🧪 Full API documentation is auto-generated and available at:  
`http://localhost:8000/docs` or `http://localhost:8000/redoc`

---

## ⚙️ Setup Instructions

Follow these steps to get the project running on your local machine:

### 1. Install Docker + Ollama
Ensure both [Docker](https://docs.docker.com/get-started/get-docker/) and [Ollama](https://ollama.com/download) are installed and accessible from your terminal.

### 2. Pull required LLM models
Download the required models using Ollama:

```bash
ollama pull nomic-embed-text
ollama pull llama3.2
```

### 3. Setup Virtual Environment (Python 3.10+ recommended)

```bash
python -m venv venv
source venv/bin/activate   # For Windows: venv\Scripts\activate
python -m pip install -r requirements.txt
```

### 4. Start Docker Services

```bash
docker-compose up -d
```

This will start:
- **PostgreSQL** on port `5432`
- **Weaviate** on port `8080`

### 5. Initialize the Database

1. Open **pgAdmin** , and run the SQL schema setup from:

```sql
db_setup/queries.sql
```

2. Insert sample data using:

```bash
python db_setup/inserter.py
```
### 6. Add Open AI Key to line 22 in app.py

```app.py
os.environ["OPENAI_API_KEY"] ="set_openai_api_key_for_evaluation"
```
### 7. Start the API Server

```bash
uvicorn app:app --reload
```

The FastAPI app will now be running at `http://localhost:8000`.

### 8. Test the APIs

Import the provided [**Postman collection**](https://github.com/vikassrini/Buyogo_Assesment/blob/main/Buyogo.postman_collection.json) and test the endpoints:

- Make sure to set the base URL to `http://localhost:8000`.
- You can also use the Swagger UI at `/docs`.

---

## 🧰 Tech Stack

- **Python** + **FastAPI**
- **PostgreSQL** for relational data
- **Weaviate** for vector search
- **Ollama** with local LLMs (`nomic-embed-text`, `llama3.2`)
- **Docker Compose** for orchestration

---

## NOTE:
- [Sample test queries](https://github.com/vikassrini/Buyogo_Assesment/blob/main/samples/samples_queries.json) and their expected results are available in the samples directory.

- Within this directory, the [sample_caching_responses](https://github.com/vikassrini/Buyogo_Assesment/tree/main/samples/sample_caching_responses) subfolder contains cached responses from each API, demonstrating how the implemented caching mechanism functions.

- A brief report outlining the implementation decisions and challenges faced can be found here: [Implementation Report](https://github.com/vikassrini/Buyogo_Assesment/blob/main/Implementation%20Report.pdf).
