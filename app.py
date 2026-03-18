import uuid
from flask import Flask, request, jsonify, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter
from src.logger import setup_logger
from src.rabbitmq import rabbitmq_service

# Define as métricas
REQUEST_COUNT = Counter('http_requests_total', 'Total de requisicoes HTTP', ['method', 'endpoint'])
MESSAGE_PUBLISHED = Counter('messages_published_total', 'Total de mensagens publicadas com sucesso')

def create_app() -> Flask:
    app = Flask(__name__)
    logger = setup_logger("api-rest-logger")

    # Inicializar o rabbitmq apenas na inicialização do app web
    rabbitmq_service.connect()

    @app.get("/health")
    def health_check():
        REQUEST_COUNT.labels(method='GET', endpoint='/health').inc()
        logger.info("Healthcheck executed", extra={"correlationId": str(uuid.uuid4())})
        return jsonify({"message": "I'm alive"}), 200

    @app.post("/message")
    def send_message():
        REQUEST_COUNT.labels(method='POST', endpoint='/message').inc()
        request_data = request.get_json()
        correlation_id = str(uuid.uuid4())
        
        new_message = {
            "name": request_data.get("name", "Unknown"),
            "messageNumber": request_data.get("messageNumber", 0),
            "correlationId": correlation_id
        }
        
        logger.info(
            "New Message Received",
            extra={"correlationId": correlation_id, "messageReceived": new_message}
        )

        try:
            rabbitmq_service.publish(new_message)
            MESSAGE_PUBLISHED.inc()
            logger.info(
                "Message published to logs exchange",
                extra={"correlationId": correlation_id, "messageReceived": new_message}
            )
            return jsonify(new_message), 200
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            return jsonify({"error": "Failed to process message"}), 500

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    return app

if __name__ == "__main__":
    app_instance = create_app()
    app_instance.run(host="0.0.0.0", port=5000)