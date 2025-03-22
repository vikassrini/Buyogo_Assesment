-- 1. Create the table
CREATE TABLE hotel_bookings (
    hotel TEXT,
    lead_time BIGINT,
    arrival_date_week_number BIGINT,
    stays_in_weekend_nights BIGINT,
    stays_in_week_nights BIGINT,
    adults BIGINT,
    children BIGINT,
    babies BIGINT,
    meal TEXT,
    country TEXT,
    market_segment TEXT,
    distribution_channel TEXT,
    previous_cancellations BIGINT,
    previous_bookings_not_canceled BIGINT,
    reserved_room_type TEXT,
    assigned_room_type TEXT,
    booking_changes BIGINT,
    deposit_type TEXT,
    agent BIGINT,
    company BIGINT,
    days_in_waiting_list BIGINT,
    customer_type TEXT,
    adr DOUBLE PRECISION,
    required_car_parking_spaces BIGINT,
    total_of_special_requests BIGINT,
    reservation_status TEXT,
    reservation_status_date DATE,
    arrival_date DATE,
    is_canceled BOOLEAN,
    is_repeated_guest BOOLEAN,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- 2. Create trigger function to auto-update last_updated
CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_updated = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Create trigger on the hotel_bookings table
CREATE TRIGGER update_last_updated_trigger
BEFORE INSERT OR UPDATE ON hotel_bookings
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_column();



CREATE TABLE query_history (
    id SERIAL PRIMARY KEY,
    user_query TEXT NOT NULL,
    generated_response TEXT NOT NULL,
    faithfulness_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);