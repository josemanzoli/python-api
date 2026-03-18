#!/usr/bin/env python
import time
import json
import sys
from src.logger import setup_logger
from src.rabbitmq import rabbitmq_service

logger = setup_logger("consumer-logger")

def message_callback(ch, method, properties, body):
    """Callback processador de mensagens assíncronas do RabbitMQ."""
    try:
        decoded_body = body.decode('utf-8')
        logger.info(
            "New Message consumed",
            extra={"messageReceived": decoded_body}
        )
        print(f" [x] Processing: {decoded_body}")
        
        # Simula processamento lento
        time.sleep(4)
        
        # Confirma o recebimento apenas após processado com sucesso
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}")
        # Em cenários reais, enviar para uma DLQ invés de apenas rejeitar
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

if __name__ == "__main__":
    try:
        rabbitmq_service.connect()
        rabbitmq_service.start_consuming(callback_func=message_callback)
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Closing connection...")
        rabbitmq_service.close()
        sys.exit(0)