#!/usr/bin/env python
import time
import json
import random
import sys
from src.logger import setup_logger
from src.rabbitmq import rabbitmq_service

logger = setup_logger("consumer-logger")

# Simulated Cache for Idempotency (in memory purely for lesson purposes)
# Em produção, usaria-se um Redis ou consulta no banco (Postgres/Mongo)
processed_messages = set()

def message_callback(ch, method, properties, body):
    """Callback processador de mensagens assíncronas do RabbitMQ."""
    try:
        decoded_body = body.decode('utf-8')
        message_data = json.loads(decoded_body)
        correlation_id = message_data.get("correlationId")
        
        # 1. Checagem de Idempotência
        if correlation_id in processed_messages:
            logger.warning(
                f"Idempotency Check: Message {correlation_id} already processed. Dropping duplicate silently.",
                extra={"correlationId": correlation_id}
            )
            # Confirma para tirar da fila, já que já existe na base
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
            
        logger.info(
            "New Message consumed",
            extra={"correlationId": correlation_id, "messageReceived": message_data}
        )
        print(f" [x] Processing: {message_data.get('name')} (ID: {correlation_id})")
        
        # 2. Simula processamento lento
        time.sleep(2)
        
        # 3. Simula falha esporádica arquitetural para aula (DLQ Trigger - 20% chance)
        if random.random() < 0.2:
             raise ValueError("Simulated random processing failure to demonstrate Dead Letter Queue!")

        # 4. Grava no pseudo-cache de idempotência e finaliza
        processed_messages.add(correlation_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}")
        # NACK envia a mensagem diretamente para a DLQ (pois requeue=False)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

if __name__ == "__main__":
    try:
        rabbitmq_service.connect()
        rabbitmq_service.start_consuming(callback_func=message_callback)
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Closing connection...")
        rabbitmq_service.close()
        sys.exit(0)