from confluent_kafka import Consumer
from .kafka_producer import send_order_update
from django.conf import settings
from django.db import transaction

# from .models import Order, OrderItem # Import the model where you want to save the data
import json

# Initialize Kafka Shipping Consumer
consumer_shipping = Consumer({
    'bootstrap.servers': settings.KAFKA_CONFIG['bootstrap.servers'],
    'group.id': settings.KAFKA_CONFIG['group.id'],
    'auto.offset.reset': settings.KAFKA_CONFIG['auto.offset.reset']
})

def consume_shipping_data():
    """Function to consume messages from Kafka."""
    consumer_shipping.subscribe([settings.KAFKA_TOPIC_ORDER_CREATED])
    
    try:
        while True:
            msg = consumer_shipping.poll(1.0)  # Timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            print(f"Consumer Shipping Received message: {msg.value().decode('utf-8')}")
            
            # Convert the Kafka message back to Python Dict
            shipping_data = json.loads(msg.value().decode('utf-8'))

            # Save the ORDER to the PostgreSQL database
            # save_order_to_db(order_data)

            # print(f"Record created for order: {shipping_data}")

            order_data_update = {
                'order_id':shipping_data['order_id'],
                'status':'SHIPPED'
            }

            send_order_update(settings.KAFKA_TOPIC_ORDER_UPDATE, json.dumps(order_data_update))

            print(f"Order update sent to kafka: {order_data_update}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer_shipping.close()
