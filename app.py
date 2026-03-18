import logging
import os
import sys
import pika
import logging_json
import uuid

from multiprocessing import Queue

from flask import Flask, request

app = Flask(__name__)

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

logger = logging.getLogger("app-logger")
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "passw123")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=credentials))
channel = connection.channel()

@app.get("/health")
def health_check():
    logger.info(
        "health executado",
        extra={"correlationId": uuid.uuid4()}
        )
    return {"message": "I'm alive"}, 200

@app.post("/message")
def send_message():
    request_data = request.get_json()
    correlationId = uuid.uuid4()
    new_message = {
        "name": request_data["name"],
        "messageNumber": request_data["messageNumber"],
        "correlationId": correlationId
    }
    
    logger.info(
        "New Message Received",
        extra={"correlationId": correlationId, "messageReceveid":new_message}
    )

    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    channel.basic_publish(exchange='logs', routing_key='', body=str(new_message))

    result = channel.queue_declare(queue='logs', exclusive=False)
    queue_name = result.method.queue

    channel.queue_bind(exchange='logs', queue=queue_name)

    logger.info(
        "Message published to logs exchange",
        extra={"correlationId": correlationId, "messageReceveid":new_message}
    )

    return new_message