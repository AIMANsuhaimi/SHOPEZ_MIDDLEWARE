from flask import Flask, request, jsonify
from flask_cors import CORS
import datetime
import random
import os
import time

import producer

app = Flask(__name__)
CORS(app)

CUSTOMERS = ['Ali bin Ahmad', 'Siti Noor', 'Raj Kumar', 'Mei Ling', 'Aziz Harun']
PRODUCTS = [
    {"sku": "SKU-001", "name": "Wireless Headphones", "price": 199.00},
    {"sku": "SKU-042", "name": "Phone Case",          "price": 15.50},
    {"sku": "SKU-099", "name": "Laptop Stand",        "price": 89.00},
]

order_seq = 10000


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})


@app.route('/order', methods=['POST'])
def place_orders():
    """
    Body: { "count": 5 }
    Publishes `count` random orders to RabbitMQ and returns their details
    so the dashboard can animate each one.
    """
    global order_seq

    data = request.get_json(force=True) or {}
    count = int(data.get("count", 1))
    count = max(1, min(count, 1000))  # keep it sane

    results = []
    for _ in range(count):
        order_seq += 1
        customer = random.choice(CUSTOMERS)
        item = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        total = round(item["price"] * qty, 2)

        order = {
            "order_id": f"ORD-{order_seq}",
            "customer": customer,
            "email": f"{customer.lower().replace(' ', '')}@example.com",
            "items": [
                {"product_id": item["sku"], "name": item["name"], "qty": qty, "price": item["price"]}
            ],
            "total": total,
            "timestamp": datetime.datetime.now().isoformat(),
        }

        try:
            producer.publish_order(order)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Could not connect to RabbitMQ: {e}"}), 503

        results.append({
            "order_id": order["order_id"],
            "customer": customer,
            "sku": item["sku"],
            "name": item["name"],
            "qty": qty,
            "total": total,
        })

    return jsonify({"status": "ok", "orders": results})

@app.route('/order_p2p', methods=['POST'])
def place_orders_p2p():
    """
    Simulates a Synchronous Point-to-Point Architecture.
    For each order, it sequentially waits for (simulated) Payment, Inventory, and Order services.
    """
    global order_seq

    data = request.get_json(force=True) or {}
    count = int(data.get("count", 1))
    count = max(1, min(count, 1000))

    results = []
    for _ in range(count):
        order_seq += 1
        customer = random.choice(CUSTOMERS)
        item = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        total = round(item["price"] * qty, 2)

        order_id = f"ORD-{order_seq}"

        # 1. Simulate Point-to-Point call to Payment Service
        time.sleep(0.3)
        payment_success = random.random() < 0.9

        if not payment_success:
            results.append({
                "order_id": order_id,
                "status": "failed_at_payment",
                "customer": customer,
            })
            continue # Abort this order
            
        # 2. Simulate Point-to-Point call to Inventory Service
        time.sleep(0.4)
        
        # 3. Simulate Point-to-Point call to Order Service
        time.sleep(0.2)

        results.append({
            "order_id": order_id,
            "status": "success",
            "customer": customer,
            "sku": item["sku"],
            "name": item["name"],
            "qty": qty,
            "total": total,
        })

    return jsonify({"status": "ok", "orders": results})



if __name__ == "__main__":
    flask_host = os.getenv('FLASK_HOST', 'localhost')
    print(f"🟢 Order API running on http://{flask_host}:5000")
    app.run(host=flask_host, port=5000)
