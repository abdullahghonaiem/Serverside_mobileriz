from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import asyncpg
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
import logging
import os
from asyncpg import create_pool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Database connection settings
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:pp00pp00@host.docker.internal:5432/mobileriz_db")
KAFKA_TOPIC = "products"

# Kafka Producer
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")  # Change to 'kafka' for Docker networking
producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')

# Connection pool for the database
DATABASE_POOL = None

# Database setup function
async def create_tables():
    async with DATABASE_POOL.acquire() as conn:
        await conn.execute('''CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS vendors (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            api_url TEXT NOT NULL,
            image_url TEXT NOT NULL  -- Added image_url field for vendors
        )''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            price REAL NOT NULL,
            image_url TEXT NOT NULL,
            vendor_id INTEGER REFERENCES vendors(id),
            category_id INTEGER REFERENCES categories(id)
        )''')

@app.on_event("startup")
async def startup_event():
    global DATABASE_POOL
    DATABASE_POOL = await create_pool(DATABASE_URL)
    await create_tables()
    await producer.start()  # Start the Kafka producer
    asyncio.create_task(consume_kafka_messages())  # Start Kafka consumer

@app.on_event("shutdown")
async def shutdown_event():
    await DATABASE_POOL.close()  # Close the connection pool
    await producer.stop()  # Stop the Kafka producer

# Pydantic models
class Category(BaseModel):
    id: int
    name: str

class Vendor(BaseModel):
    id: int
    name: str
    api_url: str
    image_url: str  # Added image_url field

class Product(BaseModel):
    id: int
    name: str
    description: str
    price: float
    image_url: str
    vendor_id: int  # Link to vendor
    category_id: int  # Link to category

# Start a Kafka consumer
async def consume_kafka_messages():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers='kafka:9092',
        group_id='product_group',
        auto_offset_reset='earliest',
    )
    await consumer.start()
    try:
        async for message in consumer:
            product_data = message.value.decode('utf-8')
            logger.info(f"Received Kafka message: {product_data}")  # Log the message
            await update_product_in_db(product_data)  # Update database with incoming product data
    except Exception as e:
        logger.error(f"Error consuming message: {e}")  # Log any errors during consumption
    finally:
        await consumer.stop()

async def update_product_in_db(product_data: str):
    try:
        product = json.loads(product_data)  # Attempt to decode JSON
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")  # Log the error if JSON is invalid
        return  # Skip processing if the message is not valid JSON

    async with DATABASE_POOL.acquire() as conn:
        await conn.execute('''INSERT INTO products (id, name, description, price, image_url, vendor_id, category_id) 
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT(id) DO UPDATE SET 
            name = excluded.name,
            description = excluded.description,
            price = excluded.price,
            image_url = excluded.image_url,
            vendor_id = excluded.vendor_id,
            category_id = excluded.category_id;
        ''', product['id'], product['name'], product['description'], product['price'], product['image_url'], product['vendor_id'], product['category_id'])

# Endpoint to create categories
@app.post("/categories/")
async def create_categories(categories: List[Category]):
    async with DATABASE_POOL.acquire() as conn:
        try:
            for category in categories:
                await conn.execute('INSERT INTO categories (id, name) VALUES ($1, $2)', 
                                   category.id, category.name)
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=400, detail="Category ID already exists.")
    
    return JSONResponse(content={"message": "Categories added successfully!"})

# Endpoint to create vendors
@app.post("/vendors/")
async def create_vendors(vendors: List[Vendor]):
    async with DATABASE_POOL.acquire() as conn:
        try:
            for vendor in vendors:
                await conn.execute('INSERT INTO vendors (id, name, api_url, image_url) VALUES ($1, $2, $3, $4)', 
                                   vendor.id, vendor.name, vendor.api_url, vendor.image_url)
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=400, detail="Vendor ID already exists.")
    
    return JSONResponse(content={"message": "Vendors added successfully!"})

# Endpoint to get all products, with optional category filter
@app.get("/products/")
async def get_products(category_id: Optional[int] = None):
    async with DATABASE_POOL.acquire() as conn:
        if category_id is not None:
            products = await conn.fetch('''
                SELECT p.*, v.id AS vendor_id, v.name AS vendor_name, v.image_url AS vendor_image_url
                FROM products p
                JOIN vendors v ON p.vendor_id = v.id
                WHERE p.category_id = $1
            ''', category_id)
        else:
            products = await conn.fetch('''
                SELECT p.*, v.id AS vendor_id, v.name AS vendor_name, v.image_url AS vendor_image_url
                FROM products p
                JOIN vendors v ON p.vendor_id = v.id
            ''')
    
    return JSONResponse(content=[{
            "id": product['id'],
            "name": product['name'],
            "description": product['description'],
            "price": product['price'],
            "image_url": product['image_url'],
            "vendor_id": product['vendor_id'],
            "category_id": product['category_id'],
            "vendor_info": {
                "id": product['vendor_id'],
                "name": product['vendor_name'],
                "image_url": product['vendor_image_url']
            }
        } for product in products
    ])

# Updated endpoint to get a product by ID
@app.get("/products/{product_id}")
async def get_product_by_id(product_id: int):
    async with DATABASE_POOL.acquire() as conn:
        product = await conn.fetchrow('SELECT * FROM products WHERE id = $1', product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found!")

        # Fetch the vendor details
        vendor = await conn.fetchrow('SELECT id, name, image_url FROM vendors WHERE id = $1', product['vendor_id'])
        if vendor is None:
            raise HTTPException(status_code=404, detail="Vendor not found!")

    return JSONResponse(content={
        "id": product['id'],
        "name": product['name'],
        "description": product['description'],
        "price": product['price'],
        "image_url": product['image_url'],
        "vendor_id": product['vendor_id'],
        "category_id": product['category_id'],
        "vendor_info": {
            "id": vendor['id'],
            "name": vendor['name'],
            "image_url": vendor['image_url']
        }
    })

@app.delete("/products/{product_id}")
async def delete_product(product_id: int):
    async with DATABASE_POOL.acquire() as conn:
        result = await conn.execute('DELETE FROM products WHERE id = $1', product_id)
    
    if result == 'DELETE 0':
        raise HTTPException(status_code=404, detail="Product not found!")
    
    return JSONResponse(content={"message": f"Product with ID {product_id} deleted successfully!"})

# Endpoint to load existing products from PostgreSQL to Kafka
@app.post("/load_products_to_kafka/")
async def load_products_to_kafka():
    async with DATABASE_POOL.acquire() as conn:
        products = await conn.fetch('SELECT * FROM products')

    for product in products:
        product_data = {
            "id": product['id'],
            "name": product['name'],
            "description": product['description'],
            "price": product['price'],
            "image_url": product['image_url'],
            "vendor_id": product['vendor_id'],
            "category_id": product['category_id'],
        }
        
        await producer.send_and_wait(KAFKA_TOPIC, json.dumps(product_data, ensure_ascii=False).encode('utf-8'))
    
    return JSONResponse(content={"message": "All products loaded to Kafka!"})

# Run the app with: uvicorn product_catalog_backend:app --reload 
