

import pika
import json
import time
import os

# ── Connection ────────────────────────────────────────────────────────────────
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

channel.exchange_declare(exchange='shopez_orders', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='shopez_orders', queue=queue_name)

# ── Callback ──────────────────────────────────────────────────────────────────
def process_order(ch, method, properties, body):
    order = json.loads(body)
    print(f"\n[Order Service] 📨 Received Order: {order['order_id']}")
    print(f"[Order Service] ⏳ Logging order to database...")

    time.sleep(0.15)  # Simulate DB write

    print(f"[Order Service]    Order ID  : {order['order_id']}")
    print(f"[Order Service]    Customer  : {order['customer']} ({order['email']})")
    print(f"[Order Service]    Items     : {len(order['items'])} item(s)")

    for item in order['items']:
        print(f"[Order Service]       - {item['name']} x{item['qty']} @ RM {item['price']:.2f}")

    print(f"[Order Service]    Total     : RM {order['total']:.2f}")
    print(f"[Order Service]    Placed at : {order['timestamp']}")
    print(f"[Order Service] ✅ Order {order['order_id']} confirmed and logged.")
    print(f"[Order Service] 📧 Confirmation email queued to {order['email']}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# ── Start Listening ───────────────────────────────────────────────────────────
channel.basic_consume(queue=queue_name, on_message_callback=process_order)

print("[Order Service] 🟢 Waiting for orders from RabbitMQ... (Ctrl+C to stop)\n")
channel.start_consuming()
