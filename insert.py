import pandas as pd
from sqlalchemy.orm import Session
from models import SessionLocal, Hotel, Customer, MarketSegment, MealPlan, Booking, Room, BookingDetails

# Load cleaned dataset
file_path = "hotel_data_no_true_duplicates.csv"
df = pd.read_csv(file_path)

# Convert necessary columns
df['arrival_date'] = pd.to_datetime(df['arrival_date'])
df['reservation_status_date'] = pd.to_datetime(df['reservation_status_date'])
df['revenue'] = df['adr'] * (df['stays_in_weekend_nights'] + df['stays_in_week_nights'])

# Convert is_repeated_guest to Boolean to fix the error
df['is_repeated_guest'] = df['is_repeated_guest'].astype(bool)

# Start SQLAlchemy session
session = SessionLocal()

# **1️⃣ Insert Rooms (Avoid Duplicates)**
for _, row in df[['reserved_room_type', 'assigned_room_type']].drop_duplicates().iterrows():
    existing_room = session.query(Room).filter_by(
        reserved_room_type=row['reserved_room_type'], 
        assigned_room_type=row['assigned_room_type']
    ).first()
    
    if not existing_room:
        room = Room(
            reserved_room_type=row['reserved_room_type'],
            assigned_room_type=row['assigned_room_type']
        )
        session.add(room)

session.commit()  # Commit rooms before proceeding

# **2️⃣ Insert Market Segments (Avoid Duplicates)**
for _, row in df[['market_segment', 'distribution_channel']].drop_duplicates().iterrows():
    existing_segment = session.query(MarketSegment).filter_by(
        market_segment=row['market_segment']
    ).first()
    
    if not existing_segment:
        segment = MarketSegment(
            market_segment=row['market_segment'], 
            distribution_channel=row['distribution_channel']
        )
        session.add(segment)

session.commit()  # Commit market segments before proceeding

# **3️⃣ Insert Bookings**
for _, row in df.iterrows():
    # Fetch Foreign Keys
    hotel_id = session.query(Hotel.hotel_id).filter(Hotel.hotel_name == row['hotel']).scalar()
    customer_id = session.query(Customer.customer_id).filter(
        (Customer.country == row['country']) & 
        (Customer.is_repeated_guest == row['is_repeated_guest'])  # ✅ Fix boolean comparison issue
    ).scalar()

    # Check if the booking already exists to prevent duplicates
    existing_booking = session.query(Booking).filter_by(
        hotel_id=hotel_id,
        customer_id=customer_id,
        arrival_date=row['arrival_date']
    ).first()

    if not existing_booking:
        booking = Booking(
            hotel_id=hotel_id,
            customer_id=customer_id,
            lead_time=row['lead_time'],
            arrival_date=row['arrival_date'],
            arrival_date_week_number=row['arrival_date_week_number'],
            stays_in_weekend_nights=row['stays_in_weekend_nights'],
            stays_in_week_nights=row['stays_in_week_nights'],
            adults=row['adults'],
            children=row['children'],
            babies=row['babies'],
            revenue=row['revenue'],
            is_canceled=row['is_canceled'],
            reservation_status=row['reservation_status'],
            reservation_status_date=row['reservation_status_date']
        )
        session.add(booking)

session.commit()  # Commit bookings before proceeding

# **4️⃣ Insert Booking Details (Avoid Duplicate `booking_id`)**
with session.no_autoflush:
    for _, row in df.iterrows():
        # Fetch Foreign Keys
        booking_id = session.query(Booking.booking_id).filter(
            Booking.arrival_date == row['arrival_date']
        ).scalar()

        room_id = session.query(Room.room_id).filter(
            (Room.reserved_room_type == row['reserved_room_type']) &
            (Room.assigned_room_type == row['assigned_room_type'])
        ).scalar()

        segment_id = session.query(MarketSegment.segment_id).filter(
            MarketSegment.market_segment == row['market_segment']
        ).scalar()

        meal_id = session.query(MealPlan.meal_id).filter(
            MealPlan.meal_type == row['meal']
        ).scalar()

        # Check if booking details already exist to prevent duplicate booking_id insertions
        existing_booking_details = session.query(BookingDetails).filter_by(
            booking_id=booking_id,
            room_id=room_id,
            segment_id=segment_id,
            meal_id=meal_id
        ).first()

        if not existing_booking_details:
            booking_detail = BookingDetails(
                booking_id=booking_id,
                room_id=room_id,
                segment_id=segment_id,
                meal_id=meal_id,
                deposit_type=row['deposit_type'],
                agent=row['agent'],
                company=row['company'],
                days_in_waiting_list=row['days_in_waiting_list'],
                customer_type=row['customer_type'],
                adr=row['adr'],
                required_car_parking_spaces=row['required_car_parking_spaces'],
                total_of_special_requests=row['total_of_special_requests']
            )
            session.add(booking_detail)

session.commit()
session.close()

print("✅ Bookings, Rooms, and Booking Details Inserted Successfully!")
