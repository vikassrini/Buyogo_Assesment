
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/hotel_data"
engine = create_engine(DATABASE_URL)

def get_db_last_updated_timestamp() -> float:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT MAX(last_updated) FROM hotel_bookings")).scalar()
        return result.timestamp() if result else 0.0
