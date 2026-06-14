import pika
import json
import time
import os

# ── Simulated In-Memory Stock ─────────────────────────────────────────────────
stock = {
    "SKU-001": 50,
    "SKU-042": 200,
    "SKU-099": 5,
}

# ── Connection ────────────────────────────────────────────────────────────────
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

channel.exchange_declare(exchange='shopez_orders', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='shopez_orders', queue=queue_name)

# ── Callback ──────────────────────────────────────────────────────────────────
def update_inventory(ch, method, properties, body):
    order = json.loads(body)
    print(f"\n[Inventory Service] 📨 Received Order: {order['order_id']}")
    print(f"[Inventory Service] ⏳ Updating stock levels...")

    time.sleep(0.2)  # Simulate processing time

    all_ok = True
    for item in order['items']:
        sku = item['product_id']
        qty = item['qty']

        if sku in stock:
            if stock[sku] >= qty:
                stock[sku] -= qty
                print(f"[Inventory Service]    {item['name']} ({sku}): deducted {qty} unit(s) — {stock[sku]} remaining")
            else:
                print(f"[Inventory Service] ⚠️  {item['name']} ({sku}): insufficient stock!")
                all_ok = False
        else:
            print(f"[Inventory Service] ⚠️  SKU {sku} not found in inventory!")
            all_ok = False

    if all_ok:
        print(f"[Inventory Service] ✅ Inventory updated for Order {order['order_id']}")
    else:
        print(f"[Inventory Service] ⚠️  Partial inventory update for Order {order['order_id']}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=queue_name, on_message_callback=update_inventory)

print("[Inventory Service] 🟢 Waiting for orders from RabbitMQ... (Ctrl+C to stop)\n")
channel.start_consuming()
