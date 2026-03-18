import logging
import os
import sys
import pika
import time
import logging_json
import uuid

logHandler = logging.StreamHandler(sys.stdout)

formatter = logging_json.JSONFormatter(fields={"level": "levelname", 
                                    "loggerName": "name", 
                                    "processName": "processName",
                                    "processID": "process", 
                                    "threadName": "threadName", 
                                    "threadID": "thread",
                                    "lineNumber" : "lineno",
                                    "timestamp": "asctime"})
logHandler.setFormatter(formatter)

logger = logging.getLogger("consumer-logger")
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "passw123")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

result = channel.queue_declare(queue='logs', exclusive=False)
queue_name = result.method.queue

channel.queue_bind(exchange='logs', queue=queue_name)
channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    logger.info(
        "New Message consumed",
        extra={"messageReceveid":body}
    )
    print(" [x] %r" % body)
    time.sleep(4)
    channel.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=False)

channel.start_consuming()