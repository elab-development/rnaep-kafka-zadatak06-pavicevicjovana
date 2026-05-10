from fastapi import FastAPI
from typing import List
from models import Notification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_confirmed = AIOKafkaConsumer(
        "order-confirmed",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    consumer_not_found = AIOKafkaConsumer(
        "product_not_found_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    consumer_out_of_stock = AIOKafkaConsumer(
        "out_of_stock_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )

    await consumer_confirmed.start()
    await consumer_not_found.start()
    await consumer_out_of_stock.start()

    task_confirmed = asyncio.create_task(consume_confirmed(consumer_confirmed))
    task_not_found = asyncio.create_task(consume_error(consumer_not_found))
    task_out_of_stock = asyncio.create_task(consume_error(consumer_out_of_stock))

    yield

    task_confirmed.cancel()
    task_not_found.cancel()
    task_out_of_stock.cancel()
    await consumer_confirmed.stop()
    await consumer_not_found.stop()
    await consumer_out_of_stock.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []

async def consume_confirmed(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Order {data['order_id']} for product {data['product_id']} has been placed."
            )
            notifications_db.append(notification)
    except asyncio.CancelledError:
        pass

async def consume_error(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Narudžbina {data['order_id']} je odbijena: {data['error_reason']}"
            )
            notifications_db.append(notification)
    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db
