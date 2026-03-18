import os
import pika
from typing import Any
from .logger import setup_logger

logger = setup_logger("rabbitmq-service")

class RabbitMQService:
    def __init__(self):
        self.user = os.getenv("RABBITMQ_USER", "admin")
        self.password = os.getenv("RABBITMQ_PASSWORD", "passw123")
        self.host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.exchange = 'logs'
        self.queue_name = 'logs'
        self.connection = None
        self.channel = None

    def connect(self):
        """Estabelece conexão com o RabbitMQ e configura Exchange e Fila."""
        if not self.connection or self.connection.is_closed:
            try:
                credentials = pika.PlainCredentials(username=self.user, password=self.password)
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=5672, credentials=credentials)
                )
                self.channel = self.connection.channel()
                
                # Setup Topologia apenas uma vez
                self.channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
                result = self.channel.queue_declare(queue=self.queue_name, exclusive=False)
                self.queue_name = result.method.queue # Garante usar o queue correto
                self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name)
                
                logger.info("Successfully connected to RabbitMQ and setup topologies.")
            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
                raise

    def publish(self, message: Any) -> None:
        """Publica uma mensagem na Exchange."""
        if not self.channel or self.channel.is_closed:
            self.connect()
            
        self.channel.basic_publish(
            exchange=self.exchange, 
            routing_key='', 
            body=str(message)
        )

    def start_consuming(self, callback_func) -> None:
        """Configura o consumo contínuo associando uma função callback."""
        if not self.channel or self.channel.is_closed:
            self.connect()
            
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name, 
            on_message_callback=callback_func, 
            auto_ack=False
        )
        
        logger.info("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def close(self):
        """Encerra a conexão limpidamente."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

# Instância Singleton a ser reusada pelo app
rabbitmq_service = RabbitMQService()
