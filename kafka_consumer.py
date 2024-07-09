from confluent_kafka import Producer, Consumer
import random
import string
import json
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

def random_string(length=6):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))

def produce(topic, config):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  #key = random_string()
  key= "test3"
  value = json.dumps({
  "checkout_id": key, 
  "timestamp": current_datetime, 
  "status": "done",
  "user_id": "user_id",
  "items": [
    {
      "item_id": "item_identifier",
      "quantity": 1,
      "price": 12.99
    }
  ],
  "total_price": 12.99
})
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()
  return key

def produce_payment(topic, config, checkout_id):
    # creates a new producer instance
    producer = Producer(config)

    # prepares a payment message using the same checkout_id
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payment_status = "done"  # Example status (done or waiting), adjust as needed
    value = json.dumps({
    "checkout_id": checkout_id,
    "timestamp": current_datetime,
    "payment_status": payment_status,
    "total_price": 12.99
    })

    # produces the payment message
    producer.produce(topic, key=checkout_id, value=value)
    print(f"Produced payment message to topic {topic}: key = {checkout_id} value = {value}")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()

def consume_checkout(topic, config):
  # sets the consumer group ID and offset  
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"

  # creates a new consumer instance
  consumer = Consumer(config)

  # subscribes to the specified topic
  consumer.subscribe([topic])

  try:
    while True:
      # consumer polls the topic and prints any incoming messages
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        try:
            vjson = json.loads(value)  # Attempt to deserialize the message value
            checkout_id = vjson.get('checkout_id')  # Safely access 'ordertime' field
            print(f"Consumed message from topic {topic}: key = {key} checkout_id = {checkout_id}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON for message with key = {key} and value = {value}")
  except KeyboardInterrupt:
    pass
  finally:
    # closes the consumer connection
    consumer.close()

def main():
  config = read_config()
  topic_checkout = "checkout"

  checkout_id = produce(topic_checkout, config)
  topic_payment = "payment"
  produce_payment(topic_payment, config, checkout_id)
  consume_checkout(topic_checkout, config)


main()