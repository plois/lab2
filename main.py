from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, validator
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from typing import List, Set
import os
import json

# Configuration for POSTGRES
from config import *

# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)

metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Pydantic models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float

class GpsData(BaseModel):
    latitude: float
    longitude: float

class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @validator('timestamp', pre=True)
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError("Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")

class ProcessedAgentDataCreate(BaseModel):
    road_state: str
    agent_data: AgentData

class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()

@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)

# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data))

# CRUD operations
@app.post("/processed_agent_data/", response_model=ProcessedAgentDataInDB)
async def create_processed_agent_data(data: ProcessedAgentDataCreate, db: Session = Depends(get_db)):
    db_data = processed_agent_data.insert().values(
        road_state=data.road_state,
        x=data.agent_data.accelerometer.x,
        y=data.agent_data.accelerometer.y,
        z=data.agent_data.accelerometer.z,
        latitude=data.agent_data.gps.latitude,
        longitude=data.agent_data.gps.longitude,
        timestamp=data.agent_data.timestamp
    )
    db.execute(db_data)
    db.commit()
    created_data = db.execute(processed_agent_data.select().order_by(processed_agent_data.c.id.desc()).limit(1)).first()
    await send_data_to_subscribers(created_data)
    return created_data

@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    db_data = db.execute(processed_agent_data.select().where(processed_agent_data.c.id == processed_agent_data_id)).first()
    if db_data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    return db_data

@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return db.execute(processed_agent_data.select().offset(skip).limit(limit)).fetchall()

@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentDataCreate, db: Session = Depends(get_db)):
    db_data = db.execute(processed_agent_data.select().where(processed_agent_data.c.id == processed_agent_data_id)).first()
    if db_data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    update_data = processed_agent_data.update().where(processed_agent_data.c.id == processed_agent_data_id).values(
        road_state=data.road_state,
        x=data.agent_data.accelerometer.x,
        y=data.agent_data.accelerometer.y,
        z=data.agent_data.accelerometer.z,
        latitude=data.agent_data.gps.latitude,
        longitude=data.agent_data.gps.longitude,
        timestamp=data.agent_data.timestamp
    )
    db.execute(update_data)
    db.commit()
    updated_data = db.execute(processed_agent_data.select().where(processed_agent_data.c.id == processed_agent_data_id)).first()
    return updated_data

@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    db_data = db.execute(processed_agent_data.select().where(processed_agent_data.c.id == processed_agent_data_id)).first()
    if db_data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    db.execute(processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id))
    db.commit()
    return db_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
