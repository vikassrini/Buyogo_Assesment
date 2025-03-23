import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --- Step 1: Load CSV ---
csv_file_path = "db_setup\hotel_data_no_true_duplicates.csv"
df = pd.read_csv(csv_file_path)

# Convert int columns to proper types if needed
df['is_canceled'] = df['is_canceled'].astype(bool)
df['is_repeated_guest'] = df['is_repeated_guest'].astype(bool)
df['reservation_status_date'] = pd.to_datetime(df['reservation_status_date'])
df['arrival_date'] = pd.to_datetime(df['arrival_date'])

# --- Step 2: Database Connection ---
conn = psycopg2.connect(
    host="localhost",
    database="testers",
    user="postgres",
    password="vikas"
)
cursor = conn.cursor()

# --- Step 3: Prepare Insert ---
columns = [
    'hotel', 'lead_time', 'arrival_date_week_number',
    'stays_in_weekend_nights', 'stays_in_week_nights',
    'adults', 'children', 'babies', 'meal', 'country',
    'market_segment', 'distribution_channel',
    'previous_cancellations', 'previous_bookings_not_canceled',
    'reserved_room_type', 'assigned_room_type', 'booking_changes',
    'deposit_type', 'agent', 'company', 'days_in_waiting_list',
    'customer_type', 'adr', 'required_car_parking_spaces',
    'total_of_special_requests', 'reservation_status',
    'reservation_status_date', 'arrival_date',
    'is_canceled', 'is_repeated_guest'
]

# Turn DataFrame into list of tuples
data = [tuple(x) for x in df[columns].to_numpy()]

# --- Step 4: Insert Query ---
query = f"""
    INSERT INTO hotel_bookings ({', '.join(columns)})
    VALUES %s
"""

execute_values(cursor, query, data)
conn.commit()

# --- Step 5: Clean Up ---
cursor.close()
conn.close()

print("✅ Data inserted successfully!")
