import uuid
from flask import Flask, request, jsonify
from src.logger import setup_logger
from src.rabbitmq import rabbitmq_service

def create_app() -> Flask:
    app = Flask(__name__)
    logger = setup_logger("api-rest-logger")

    # Inicializar o rabbitmq apenas na inicialização do app web
    rabbitmq_service.connect()

    @app.get("/health")
    def health_check():
        logger.info("Healthcheck executed", extra={"correlationId": str(uuid.uuid4())})
        return jsonify({"message": "I'm alive"}), 200

    @app.post("/message")
    def send_message():
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
            logger.info(
                "Message published to logs exchange",
                extra={"correlationId": correlation_id, "messageReceived": new_message}
            )
            return jsonify(new_message), 200
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            return jsonify({"error": "Failed to process message"}), 500

    return app

if __name__ == "__main__":
    app_instance = create_app()
    app_instance.run(host="0.0.0.0", port=5000)