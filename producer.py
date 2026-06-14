import pika
import json
import datetime
import random
import os


def publish_order(order):
    """Publish a single order dict to the shopez_orders fanout exchange."""
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()

    # Declare a fanout exchange — broadcasts order to ALL bound queues
    channel.exchange_declare(exchange='shopez_orders', exchange_type='fanout')

    message = json.dumps(order)

    channel.basic_publish(
        exchange='shopez_orders',
        routing_key='',          # fanout ignores routing key
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,     # make message persistent
        )
    )

    print(f"[Producer] ✅ Order published to RabbitMQ exchange 'shopez_orders'")
    print(f"[Producer]    Order ID : {order['order_id']}")
    print(f"[Producer]    Customer : {order['customer']}")
    print(f"[Producer]    Total    : RM {order['total']:.2f}")
    print(f"[Producer]    Time     : {order['timestamp']}")

    connection.close()


if __name__ == "__main__":
    # Demo: publish one random order
    order = {
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "customer": "Ali bin Ahmad",
        "email": "ali@example.com",
        "items": [
            {"product_id": "SKU-001", "name": "Wireless Headphones", "qty": 1, "price": 199.00},
            {"product_id": "SKU-042", "name": "Phone Case",          "qty": 2, "price":  15.50},
        ],
        "total": 230.00,
        "timestamp": datetime.datetime.now().isoformat(),
    }
    publish_order(order)
