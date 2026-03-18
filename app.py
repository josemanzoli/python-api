import uuid
import pybreaker
from flask import Flask, request, jsonify, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter
from src.logger import setup_logger
from src.rabbitmq import rabbitmq_service

# Define as métricas
REQUEST_COUNT = Counter('http_requests_total', 'Total de requisicoes HTTP', ['method', 'endpoint'])
MESSAGE_PUBLISHED = Counter('messages_published_total', 'Total de mensagens publicadas com sucesso')
TASK_PUBLISHED = Counter('tasks_published_total', 'Total de tarefas publicadas no Work Queue')

def create_app() -> Flask:
    app = Flask(__name__)
    logger = setup_logger("api-rest-logger")

    # Inicializar o rabbitmq apenas na inicialização do app web
    rabbitmq_service.connect()

    @app.get("/health/live")
    def liveness():
        REQUEST_COUNT.labels(method='GET', endpoint='/health/live').inc()
        return jsonify({"status": "UP"}), 200

    @app.get("/health/ready")
    def readiness():
        REQUEST_COUNT.labels(method='GET', endpoint='/health/ready').inc()
        cb_state = rabbitmq_service.circuit_state()
        if rabbitmq_service.is_connected():
            return jsonify({
                "status": "UP",
                "dependencies": {"rabbitmq": "UP"},
                "circuit_breaker": cb_state
            }), 200
        else:
            logger.error("Readiness check failed: RabbitMQ disconnected")
            return jsonify({
                "status": "DOWN",
                "dependencies": {"rabbitmq": "DOWN"},
                "circuit_breaker": cb_state
            }), 503

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
                "Message published to Pub/Sub exchange (fanout)",
                extra={"correlationId": correlation_id}
            )
            return jsonify(new_message), 200
        except pybreaker.CircuitBreakerError:
            logger.error("Circuit Breaker OPEN: RabbitMQ unavailable, rejecting fast.")
            return jsonify({"error": "Service unavailable (circuit breaker open)"}), 503
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            return jsonify({"error": "Failed to process message"}), 500

    @app.post("/task")
    def send_task():
        """Work Queue (direct exchange) — apenas 1 worker processa cada mensagem.
        
        Contraste com /message (Pub/Sub): aqui a mensagem vai para um único
        worker disponível em round-robin, ideal para tarefas pesadas.
        """
        REQUEST_COUNT.labels(method='POST', endpoint='/task').inc()
        request_data = request.get_json()
        correlation_id = str(uuid.uuid4())

        new_task = {
            "name": request_data.get("name", "Unknown"),
            "taskNumber": request_data.get("taskNumber", 0),
            "correlationId": correlation_id
        }

        logger.info(
            "New Task Received",
            extra={"correlationId": correlation_id, "taskReceived": new_task}
        )

        try:
            rabbitmq_service.publish_task(new_task)
            TASK_PUBLISHED.inc()
            logger.info(
                "Task published to Work Queue (direct exchange)",
                extra={"correlationId": correlation_id}
            )
            return jsonify(new_task), 200
        except pybreaker.CircuitBreakerError:
            logger.error("Circuit Breaker OPEN: RabbitMQ unavailable.")
            return jsonify({"error": "Service unavailable (circuit breaker open)"}), 503
        except Exception as e:
            logger.error(f"Failed to publish task: {str(e)}")
            return jsonify({"error": "Failed to process task"}), 500

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    return app

if __name__ == "__main__":
    app_instance = create_app()
    app_instance.run(host="0.0.0.0", port=5000)