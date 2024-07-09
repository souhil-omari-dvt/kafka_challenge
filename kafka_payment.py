from confluent_kafka import Consumer, Producer
import json
import time
from datetime import datetime

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def consume_payments_and_produce_orders(config, payment_topic, order_topic):
      # sets the consumer group ID and offset  
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"
    consumer = Consumer(config)
    producer = Producer(config)
    consumer.subscribe([payment_topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Process the message
            payment_message = json.loads(msg.value().decode('utf-8'))
            payment_id = payment_message.get("checkout_id")
            payment_status = payment_message.get("payment_status")

            if payment_status == "done":
                # If payment is done, produce a message to the order topic immediately
                produce_order_message(producer, order_topic, payment_id, "done")
            else:
                # Start a timer for 5 mins to wait for the payment status to be updated
                end_time = time.time() + 300 # 20 seconds for testing
                updated_payment_status = None

                while time.time() < end_time:
                    msg = consumer.poll(1.0)  # Poll for new messages
                    if msg is None:
                        continue
                    if msg.error():
                        print(f"Consumer error: {msg.error()}")
                        continue

                    # Process the new message
                    new_payment_message = json.loads(msg.value().decode('utf-8'))
                    new_payment_id = new_payment_message.get("checkout_id")
                    new_payment_status = new_payment_message.get("payment_status")

                    # Check if this message is for the same payment_id
                    if new_payment_id == payment_id:
                        updated_payment_status = new_payment_status
                        break  # Exit the loop if the relevant message is found

                # After waiting, check if the payment status was updated
                if updated_payment_status == "done":
                    produce_order_message(producer, order_topic, payment_id, "done")
                else:
                    # If no update or status is not done, assume failure
                    produce_order_message(producer, order_topic, payment_id, "failed")
    finally:
        consumer.close()

def produce_order_message(producer, order_topic, payment_id, status):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_message = json.dumps({
        "payment_id": payment_id,
        "status": status,
        "timestamp": current_datetime
    })
    producer.produce(order_topic, key=str(payment_id), value=order_message)
    producer.flush()
    print(f"Produced order message to topic {order_topic}: key = {payment_id} value = {order_message}")


payment_topic = "payment"
order_topic = "order"
config = read_config()
consume_payments_and_produce_orders(config, payment_topic, order_topic)