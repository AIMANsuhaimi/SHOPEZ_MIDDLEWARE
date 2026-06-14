import pika
import json
import time
import random
import os

# ── Connection ────────────────────────────────────────────────────────────────
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# Must declare the same exchange as the producer
channel.exchange_declare(exchange='shopez_orders', exchange_type='fanout')

# Each consumer gets its own exclusive queue bound to the exchange
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='shopez_orders', queue=queue_name)

# ── Callback ──────────────────────────────────────────────────────────────────
def process_payment(ch, method, properties, body):
    order = json.loads(body)
    print(f"\n[Payment Service] 📨 Received Order: {order['order_id']}")
    print(f"[Payment Service]    Customer : {order['customer']}")
    print(f"[Payment Service]    Amount   : RM {order['total']:.2f}")
    print(f"[Payment Service] ⏳ Processing payment...")

    time.sleep(0.3)  # Simulate processing time

    # Simulate 90% success rate
    success = random.random() < 0.9
    if success:
        print(f"[Payment Service] ✅ Payment APPROVED for Order {order['order_id']}")
    else:
        print(f"[Payment Service] ❌ Payment DECLINED for Order {order['order_id']} — retrying via queue")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# ── Start Listening ───────────────────────────────────────────────────────────
channel.basic_consume(queue=queue_name, on_message_callback=process_payment)

print("[Payment Service] 🟢 Waiting for orders from RabbitMQ... (Ctrl+C to stop)\n")
channel.start_consuming()
