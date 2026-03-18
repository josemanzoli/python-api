# Python API — RabbitMQ Producer & Consumer

API REST em **Flask** que demonstra comunicação síncrona e assíncrona com **RabbitMQ**, instrumentada com **Prometheus** para métricas. Desenvolvida para uso como serviço de exemplo em aulas de **Arquitetura de Microsserviços**.

> 📦 Este serviço faz parte do laboratório de observabilidade [josemanzoli/logs-with-loki](https://github.com/josemanzoli/logs-with-loki).

## Stack

| Tecnologia | Uso |
|---|---|
| **Flask 3.x** | Framework web (API REST) |
| **Pika** | Cliente Python para RabbitMQ |
| **prometheus_client** | Exportação de métricas HTTP |
| **logging_json** | Logs estruturados em JSON para stdout |

## Arquitetura

```
┌──────────────────────────────────────────────────────┐
│                    python-api                        │
│                                                      │
│  app.py (Flask)          consumer.py (Worker)        │
│  ├── /health/live        ├── Idempotência            │
│  ├── /health/ready       │   (via correlationId)     │
│  ├── /message ──────────►│   RabbitMQ consume        │
│  └── /metrics            └── DLQ em caso de erro     │
│       │                                               │
│  src/rabbitmq.py   ──── exchange: logs (fanout)      │
│  src/logger.py     ──── JSON stdout → Promtail/Loki  │
└──────────────────────────────────────────────────────┘
```

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health/live` | Liveness Probe — container está vivo? |
| `GET` | `/health/ready` | Readiness Probe — RabbitMQ conectado + estado do Circuit Breaker |
| `POST` | `/message` | Publica no **Pub/Sub** (fanout) — todos os consumers recebem |
| `POST` | `/task` | Publica no **Work Queue** (direct) — apenas 1 worker processa |
| `GET` | `/metrics` | Métricas Prometheus (scraped pelo Prometheus) |

### Exemplo de request

```sh
curl -X POST http://localhost:5000/message \
  -H "Content-Type: application/json" \
  -d '{"name": "minha-mensagem", "messageNumber": 1}'
```

### Exemplo de response

```json
{
  "correlationId": "b2a51119-7e50-4f49-88dc-fc5561e41561",
  "messageNumber": 1,
  "name": "minha-mensagem"
}
```

## Conceitos demonstrados

- **Comunicação síncrona** — chamada REST recebe resposta direta
- **Comunicação assíncrona** — mensagem enfileirada e processada pelo consumer de forma independente
- **Pub/Sub vs Work Queue** — `POST /message` (fanout, todos recebem) vs `POST /task` (direct, um processa)
- **Liveness vs Readiness** — health checks distintos para orquestradores (Kubernetes)
- **Dead Letter Queue (DLQ)** — mensagens com erro são desviadas para `logs_dlq` automaticamente
- **Retry com Backoff Exponencial** — consumer retenta 3x (1s → 2s → 4s) antes de enviar para DLQ
- **Circuit Breaker** — após 3 falhas no RabbitMQ, o circuito abre e retorna 503 imediatamente por 30s
- **Idempotência** — consumer verifica `correlationId` e descarta duplicatas silenciosamente
- **Escalabilidade horizontal** — aumente consumers com `docker compose up --scale consumer=N`
- **Distributed Tracing** — rastreie uma mensagem de ponta a ponta pelo `correlationId` no Grafana/Loki

## Exercício: Criando um Alerta no Grafana

O Grafana 8+ possui um **Alertmanager embutido** — sem precisar de container extra.

1. Acesse `http://localhost:3000` → menu lateral → **Alerting → Alert Rules**
2. Clique em **New alert rule**
3. Em **Define query**, selecione a fonte de dados **Prometheus**
4. Use a query: `rate(messages_published_total[1m]) > 2`
5. Em **Set alert condition** → defina o threshold e duracão
6. Em **Configure notifications** → crie um **Contact Point** (e-mail ou webhook)
7. Salve e dispare o alerta enviando mensagens via `POST /message`

> **Dica de aula:** Mostre o estado do alerta mudando de `Normal` → `Firing` em tempo real.

## Estrutura do projeto

```
python-api/
├── src/
│   ├── logger.py        # Configuração de logging JSON (reutilizável)
│   └── rabbitmq.py      # Serviço RabbitMQ (Singleton com DLQ topology)
├── app.py               # Application Factory Flask
├── consumer.py          # Worker assíncrono do RabbitMQ
├── Dockerfile
└── requirements.txt
```

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `RABBITMQ_USER` | `admin` | Usuário do RabbitMQ |
| `RABBITMQ_PASSWORD` | `passw123` | Senha do RabbitMQ |
| `RABBITMQ_HOST` | `rabbitmq` | Host do RabbitMQ |

## Como executar (standalone)

```sh
git clone https://github.com/josemanzoli/python-api
cd python-api
pip install -r requirements.txt

# Configurar variáveis
export RABBITMQ_HOST=localhost
export RABBITMQ_USER=admin
export RABBITMQ_PASSWORD=passw123

# Rodar a API
python app.py

# Em outro terminal, rodar o consumer
python consumer.py
```

> Para rodar com Docker completo (incluindo RabbitMQ, Loki, Grafana, etc.), veja o repositório [josemanzoli/logs-with-loki](https://github.com/josemanzoli/logs-with-loki).

## License

[MIT](LICENSE)
