from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, ForeignKey, Date
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Define Database Connection
DATABASE_URL = "postgresql://postgres:vikas@localhost:5432/hotel_data"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Hotel Table
class Hotel(Base):
    __tablename__ = "hotels"
    hotel_id = Column(Integer, primary_key=True, autoincrement=True)
    hotel_name = Column(String, unique=True, nullable=False)

# Customer Table
class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(String, nullable=False)
    is_repeated_guest = Column(Boolean, nullable=False)

# Booking Table
class Booking(Base):
    __tablename__ = "bookings"
    booking_id = Column(Integer, primary_key=True, autoincrement=True)
    hotel_id = Column(Integer, ForeignKey("hotels.hotel_id"))
    customer_id = Column(Integer, ForeignKey("customers.customer_id"))
    lead_time = Column(Integer)
    arrival_date = Column(Date, nullable=False)
    arrival_date_week_number = Column(Integer)
    stays_in_weekend_nights = Column(Integer)
    stays_in_week_nights = Column(Integer)
    adults = Column(Integer)
    children = Column(Integer)
    babies = Column(Integer)
    revenue = Column(Float)
    is_canceled = Column(Boolean)
    reservation_status = Column(String)
    reservation_status_date = Column(Date)

# Room Table
class Room(Base):
    __tablename__ = "rooms"
    room_id = Column(Integer, primary_key=True, autoincrement=True)
    reserved_room_type = Column(String, nullable=False)
    assigned_room_type = Column(String, nullable=False)

# Market Segment Table
class MarketSegment(Base):
    __tablename__ = "market_segments"
    segment_id = Column(Integer, primary_key=True, autoincrement=True)
    market_segment = Column(String, unique=True, nullable=False)
    distribution_channel = Column(String)

# Meal Plan Table
class MealPlan(Base):
    __tablename__ = "meal_plans"
    meal_id = Column(Integer, primary_key=True, autoincrement=True)
    meal_type = Column(String, unique=True, nullable=False)

# Booking Details Table
class BookingDetails(Base):
    __tablename__ = "booking_details"
    booking_id = Column(Integer, ForeignKey("bookings.booking_id"), primary_key=True)
    room_id = Column(Integer, ForeignKey("rooms.room_id"))
    segment_id = Column(Integer, ForeignKey("market_segments.segment_id"))
    meal_id = Column(Integer, ForeignKey("meal_plans.meal_id"))
    deposit_type = Column(String)
    agent = Column(Integer)
    company = Column(Integer)
    days_in_waiting_list = Column(Integer)
    customer_type = Column(String)
    adr = Column(Float)
    required_car_parking_spaces = Column(Integer)
    total_of_special_requests = Column(Integer)
