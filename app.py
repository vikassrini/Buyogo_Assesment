from fastapi import FastAPI, Query
import pandas as pd
from db_utils import engine, get_db_last_updated_timestamp
from analytics_cache import AnalyticsCache
from sqlalchemy import text
from typing import List
import weaviate
from weaviate.classes.config import Configure
from PyPDF2 import PdfReader
import os
import torch
from ragas.metrics import faithfulness
from ragas import evaluate
from datasets import Dataset
import pandas as pd
from ragas.llms import LangchainLLMWrapper
from time import time
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
import os
os.environ["OPENAI_API_KEY"] ="set_openai_api_key_for_evaluation" 
evaluator_llm = LangchainLLMWrapper(ChatOpenAI(model="gpt-4o"))
evaluator_embeddings = LangchainEmbeddingsWrapper(OpenAIEmbeddings())


app = FastAPI()

def execute_cached_analytics(queries: dict, cache: AnalyticsCache):
    db_last_updated = get_db_last_updated_timestamp()

    if not cache.is_stale(db_last_updated):
        return cache.get_cache()

    results = {}
    with engine.connect() as connection:
        for key, query in queries.items():
            try:
                result_proxy = connection.execute(text(query))
                columns = result_proxy.keys()
                rows = result_proxy.fetchall()
                results[key] = [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                print(f"❌ SQL error for '{key}': {e}")
                results[key] = {"error": str(e)}

    cache.update_cache(results, db_last_updated)
    return results

# Create individual caches for each analytics endpoint
revenue_cache = AnalyticsCache()
cancellation_cache = AnalyticsCache()
geo_cache = AnalyticsCache()
lead_time_cache = AnalyticsCache()
other_cache = AnalyticsCache()






@app.post("/analytics/revenue")
def get_revenue_analysis():
    queries = {
        "monthly_revenue": """
            SELECT DATE_TRUNC('month', arrival_date) AS month, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY month
            ORDER BY month;
        """,
        "yearly_revenue": """
            SELECT DATE_PART('year', arrival_date) AS year, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY year
            ORDER BY year;
        """,
        "highest_revenue_month": """
            SELECT TO_CHAR(arrival_date, 'Month YYYY') AS month_year,
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY month_year
            ORDER BY total_revenue DESC
            LIMIT 1;
        """,
        "last_6_months_revenue": """
            SELECT DATE_TRUNC('month', arrival_date) AS month,
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            WHERE arrival_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY month
            ORDER BY month;
        """,
        "revenue_by_hotel_type": """
            SELECT hotel, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY hotel
            ORDER BY total_revenue DESC;
        """,
        "revenue_by_market_segment": """
            SELECT market_segment, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY market_segment
            ORDER BY total_revenue DESC;
        """,
        "revenue_by_country": """
            SELECT country, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY country
            ORDER BY total_revenue DESC;
        """,
        "revenue_by_cancellation_status": """
            SELECT is_canceled, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY is_canceled;
        """,
        "revenue_by_room_type": """
            SELECT reserved_room_type, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY reserved_room_type
            ORDER BY total_revenue DESC;
        """,
        "revenue_by_special_requests": """
            SELECT total_of_special_requests, 
                   SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY total_of_special_requests
            ORDER BY total_of_special_requests DESC;
        """
    }
    return execute_cached_analytics(queries, revenue_cache)


@app.post("/analytics/cancellations")
def get_cancellations_analysis():
     # Queries for Cancellation Rate Analysis
    queries = {
        # 📌 1️⃣ Overall Cancellation Rate
        "overall_cancellation_rate": """
            SELECT 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings;
        """,

        # 📌 2️⃣ Cancellation Rate by Hotel Type
        "cancellation_by_hotel_type": """
            SELECT 
                hotel, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY hotel
            ORDER BY cancellation_rate DESC;
        """,

        # 📌 3️⃣ Cancellation Rate Over Time (Monthly)
        "cancellation_rate_by_month": """
            SELECT 
                DATE_TRUNC('month', arrival_date) AS month, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY month
            ORDER BY month;
        """,

        # 📌 4️⃣ Cancellation Rate by Market Segment
        "cancellation_by_market_segment": """
            SELECT 
                market_segment, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY market_segment
            ORDER BY cancellation_rate DESC;
        """,

        # 📌 5️⃣ Cancellation Rate by Country
        "cancellation_by_country": """
            SELECT 
                country, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY country
            ORDER BY cancellation_rate DESC
            LIMIT 10;
        """,

        # 📌 6️⃣ Cancellation Rate by Room Type
        "cancellation_by_room_type": """
            SELECT 
                reserved_room_type, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY reserved_room_type
            ORDER BY cancellation_rate DESC;
        """,

        # 📌 7️⃣ Impact of Lead Time on Cancellations
        "cancellation_by_lead_time": """
            SELECT 
                CASE 
                    WHEN lead_time < 7 THEN 'Last-minute'
                    WHEN lead_time BETWEEN 7 AND 30 THEN 'Short-term'
                    ELSE 'Long-term'
                END AS booking_type,
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY booking_type
            ORDER BY cancellation_rate DESC;
        """,

        # 📌 8️⃣ Impact of Special Requests on Cancellations
        "cancellation_by_special_requests": """
            SELECT 
                total_of_special_requests, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY total_of_special_requests
            ORDER BY cancellation_rate DESC;
        """,

        # 📌 9️⃣ Cancellation Rate by Deposit Type
        "cancellation_by_deposit_type": """
            SELECT 
                deposit_type, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY deposit_type
            ORDER BY cancellation_rate DESC;
        """
    }
    return execute_cached_analytics(queries, cancellation_cache)

@app.post("/analytics/geo")
def get_geo_analytics():
    # Queries for Geographical Analytics
    queries = {
        # 📌 1️⃣ Number of Bookings by Country
        "bookings_by_country": """
            SELECT 
                country, 
                COUNT(*) AS total_bookings
            FROM hotel_bookings
            GROUP BY country
            ORDER BY total_bookings DESC;
        """,

        # 📌 2️⃣ Percentage of Bookings by Country
        "booking_percentage_by_country": """
            SELECT 
                country, 
                COUNT(*) AS total_bookings,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS booking_percentage
            FROM hotel_bookings
            GROUP BY country
            ORDER BY total_bookings DESC;
        """,

        # 📌 3️⃣ Revenue by Country
        "revenue_by_country": """
            SELECT 
                country, 
                SUM(adr * (stays_in_week_nights + stays_in_weekend_nights)) AS total_revenue
            FROM hotel_bookings
            GROUP BY country
            ORDER BY total_revenue DESC;
        """,

        # 📌 4️⃣ Cancellation Rate by Country
        "cancellation_by_country": """
            SELECT 
                country, 
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY country
            ORDER BY cancellation_rate DESC;
        """,

        # 📌 5️⃣ Top 10 Countries with Highest Bookings
        "top_10_bookings_by_country": """
            SELECT 
                country, 
                COUNT(*) AS total_bookings
            FROM hotel_bookings
            GROUP BY country
            ORDER BY total_bookings DESC
            LIMIT 10;
        """,

        # 📌 6️⃣ Geographical Distribution Over Time
        "geo_distribution_over_time": """
            SELECT 
                DATE_TRUNC('month', arrival_date) AS month, 
                country, 
                COUNT(*) AS total_bookings
            FROM hotel_bookings
            GROUP BY month, country
            ORDER BY month, total_bookings DESC;
        """
    }
    return execute_cached_analytics(queries, geo_cache)

@app.post("/analytics/lead_time")
def get_lead_time_analytics():
    # Queries for Lead Time Analytics
    queries = {
        # 📌 1️⃣ Average Lead Time
        "average_lead_time": """
            SELECT 
                AVG(lead_time) AS average_lead_time
            FROM hotel_bookings;
        """,

        # 📌 2️⃣ Lead Time Distribution
        "lead_time_distribution": """
            SELECT 
                CASE 
                    WHEN lead_time < 7 THEN 'Less than a week'
                    WHEN lead_time BETWEEN 7 AND 30 THEN '1 week to 1 month'
                    WHEN lead_time BETWEEN 31 AND 90 THEN '1 to 3 months'
                    WHEN lead_time BETWEEN 91 AND 180 THEN '3 to 6 months'
                    ELSE 'More than 6 months'
                END AS lead_time_category,
                COUNT(*) AS total_bookings
            FROM hotel_bookings
            GROUP BY lead_time_category
            ORDER BY total_bookings DESC;
        """,

        # 📌 3️⃣ Percentage of Short-Term (Last-Minute) Bookings
        "percentage_last_minute_bookings": """
            SELECT 
                ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM hotel_bookings), 2) AS percentage_last_minute_bookings
            FROM hotel_bookings
            WHERE lead_time < 7;
        """,

        # 📌 4️⃣ Lead Time by Hotel Type
        "lead_time_by_hotel_type": """
            SELECT 
                hotel, 
                AVG(lead_time) AS average_lead_time
            FROM hotel_bookings
            GROUP BY hotel
            ORDER BY average_lead_time DESC;
        """,

        # 📌 5️⃣ Lead Time by Market Segment
        "lead_time_by_market_segment": """
            SELECT 
                market_segment, 
                AVG(lead_time) AS average_lead_time
            FROM hotel_bookings
            GROUP BY market_segment
            ORDER BY average_lead_time DESC;
        """,

        # 📌 6️⃣ Impact of Lead Time on Cancellation Rate
        "lead_time_cancellation_impact": """
            SELECT 
                CASE 
                    WHEN lead_time < 7 THEN 'Last-minute'
                    WHEN lead_time BETWEEN 7 AND 30 THEN '1 week to 1 month'
                    ELSE 'Long-term'
                END AS booking_category,
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) AS canceled_bookings,
                ROUND(100.0 * SUM(CASE WHEN is_canceled = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY booking_category
            ORDER BY cancellation_rate DESC;
        """
    }
    return execute_cached_analytics(queries, lead_time_cache)

@app.post("/analytics/others")
def get_other_analytics():
    queries = {
        # 📌 1️⃣ Cancellation Rate by Special Requests (Fixed)
        "special_requests_vs_cancellations": """
            SELECT total_of_special_requests, 
                   SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY total_of_special_requests;
        """,

        # 📌 2️⃣ Cancellation Rate by Deposit Type (Fixed)
        "deposit_type_vs_cancellation": """
            SELECT deposit_type, 
                   SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY deposit_type;
        """,

        # 📌 3️⃣ Cancellation Rate by Lead Time (Fixed)
        "lead_time_cancellation_impact": """
            SELECT 
                CASE 
                    WHEN lead_time < 7 THEN 'Last-minute'
                    WHEN lead_time BETWEEN 7 AND 30 THEN 'Short-term'
                    ELSE 'Long-term'
                END AS booking_type,
                COUNT(*) AS total_bookings,
                SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS cancellation_rate
            FROM hotel_bookings
            GROUP BY booking_type
            ORDER BY cancellation_rate DESC;
        """
    }
    return execute_cached_analytics(queries, other_cache)

PDF_FILES = {
    "Revenue": "data/revenue_answers.pdf",
    "Geography": "data/geo_answers.pdf",
    "Cancellations": "data/cancellations_answers.pdf",
    "Lead Time": "data/lead_time_answers.pdf",
    "Other Analytics": "data/others_analytics_answers.pdf",
}

chat_history={}
# === Utility to extract answers ===
def extract_answers_from_pdfs():
    data = []
    for section, filename in PDF_FILES.items():
        reader = PdfReader(f"{filename}")
        text = "\n".join([p.extract_text() for p in reader.pages if p.extract_text()])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        answers = [line for line in lines if any(char.isdigit() for char in line[:3])]
        for i, answer in enumerate(answers, 1):
            data.append({
                "section": section,
                "index": i,
                "text": answer
            })
    return data





# === Connect to Weaviate ===
def connect_weaviate():
    return weaviate.connect_to_local()

# === Single Endpoint: Load PDFs & Run RAG ===
@app.post("/ask")
def ask(
    query: str = Query(..., description="Ask a question about the hotel analytics reports"),
    ground_truth: str = Query(None, description="Optional expected answer for similarity check")
):
    start = time()
    with engine.connect() as conn:
        check_query = text("""
            SELECT generated_response, faithfulness_score
            FROM query_history
            WHERE user_query = :query
        """)
        result = conn.execute(check_query, {"query": query}).fetchone()

    if result:
        # Query found in cache, return cached response
        print("✅ Returning cached result")
        duration = time() - start
        return {
            "question": query,
            "generated_answer": result.generated_response,
            "faithfullness": result.faithfulness_score,
            "response_time": f"{duration:.3f} seconds",
            "cached": True
        }
    
    
    data = extract_answers_from_pdfs()
    client = connect_weaviate()
    if not client.collections.exists("HotelAnalytics"):
        client.collections.create(
            name="HotelAnalytics",
            vectorizer_config=Configure.Vectorizer.text2vec_ollama(
                model="nomic-embed-text",
                api_endpoint="http://host.docker.internal:11434"
            ),
            generative_config=Configure.Generative.ollama(
                model="llama3.2",
                api_endpoint="http://host.docker.internal:11434"
            )
        )

    col = client.collections.get("HotelAnalytics")
    aggregation = col.aggregate.over_all(total_count=True)
    existing_count = aggregation.total_count
    if existing_count == 0:
        with col.batch.dynamic() as batch:
            for item in data:
                batch.add_object({
                    "section": item["section"],
                    "index": item["index"],
                    "text": item["text"]
                })


    response = col.generate.near_text(
        query=query,
        limit=4,
        grouped_task="Answer the question in a paragraph using the following context."
    )

    client.close()
    answer = str(response.generated)  # Ensures string
   
    evaluation_data = {
        "question": [query],
        "answer": [answer],
        "contexts":[[item["text"] for item in data]],
        "ground_truth": [str(ground_truth)] 
    }

  
    dataset = Dataset.from_dict(evaluation_data)

    # Evaluate faithfulness if ground_truth is provided
    results = {}
    if ground_truth:
        results = evaluate(dataset, metrics=[faithfulness],llm=evaluator_llm,embeddings=evaluator_embeddings)
    
    duration = time() - start
    with engine.begin() as conn:

        insert_query = text("""
            INSERT INTO query_history (user_query, generated_response, faithfulness_score)
            VALUES (:query, :answer, :score)
        """)
        conn.execute(insert_query, {
            "query": query,
            "answer": answer,
            "score": results['faithfulness']
        })

    return {
        "question": query,
        "generated_answer": answer,
        "faithfullness": results['faithfulness'],
        "response_time":f"{duration} seconds"
    }

 