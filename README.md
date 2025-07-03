# Real-Time Content Analysis & Knowledge Graph Q&A

This project is an end-to-end demonstration of an event-driven, AI-powered system that ingests live RSS feeds, analyzes the content to build a knowledge graph, and provides a natural language Q&A interface.

It showcases the integration of modern data streaming, caching, graph database, and LLM technologies in a single pipeline.

## Tech Stack & Architecture

- **Data Streaming:** Kafka
- **Graph Database:** Neo4j (with APOC plugin)
- **In-Memory Cache:** Redis
- **LLM Serving:** Ollama (running the Mistral model)
- **AI/LLM Orchestration:** LangChain
- **Web Interface:** Streamlit
- **Containerization:** Docker & Docker Compose

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/) and Docker Compose
- An NVIDIA GPU with the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html) installed (for GPU acceleration). The project can be modified to run on CPU.

### 1. Configure the Environment

Create a `.env` file in the project root by copying the example:

```bash
cp .env.example .env
```

Review the `.env` file and set your desired Neo4j credentials.

### 2. Configure RSS Feeds

Edit the `producer/feeds.json` file to add, remove, or change the RSS feeds you want to ingest. You can also configure the polling interval for each feed.

### 3. Build and Launch the Services

The first build will take some time as it needs to download the base Docker images and the Ollama model.

```bash
# Build all the custom service images
docker compose build

# Launch the entire stack in detached mode
docker compose up -d
```

### 4. Verify Services are Running

Check the status of all containers:

```bash
docker compose ps
```

You should see all services (`kafka`, `neo4j`, `producer`, `consumer`, `webapp`, etc.) in the `Up` state.

## Usage

1.  **Data Ingestion:** The `producer` and `consumer` services start automatically. You can monitor their activity by viewing their logs:
    ```bash
    # Watch the producer poll feeds
    docker logs -f producer

    # Watch the consumer process articles and build the graph
    docker logs -f consumer
    ```

2.  **Explore the Knowledge Graph:**
    -   Navigate to the Neo4j Browser at **`http://localhost:7474`**.
    -   Log in with the credentials from your `.env` file (default: `neo4j`/`password123`).
    -   Run Cypher queries to explore the graph, e.g., `MATCH (n) RETURN n LIMIT 25;`.

3.  **Ask Questions:**
    -   Navigate to the Streamlit Q&A web application at **`http://localhost:8501`**.
    -   Ask questions in natural language, such as:
        -   "Which people are mentioned in articles about Acme Corp?"
        -   "What topics are related to articles that mention SpaceX?"

## Shutting Down

To stop all the services and remove the containers, run:

```bash
docker compose down
```

To also remove the database volumes and the processed articles log (for a complete reset), use the `-v` flag:

```bash
docker compose down -v
```
