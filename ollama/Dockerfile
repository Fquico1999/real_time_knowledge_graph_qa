# Use the official Ollama image as the base
FROM ollama/ollama:latest

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

RUN set -e; \
    # 1. Start the server in the background.
    ollama serve & \
    # 2. Wait until the server is responsive.
    # We will use `curl` to ping the server's root endpoint until it responds with a 200 OK status.
    echo "Waiting for Ollama server to be ready..."; \
    while ! curl -s -f http://localhost:11434/ > /dev/null; do \
        sleep 1; \
    done; \
    echo "Ollama server is ready. Pre-pulling models..."; \
    # 3. Now that the server is confirmed to be ready, pull the models.
    ollama pull mistral; \
    ollama pull mixtral; \
    # 4. Clean up the temporary server process.
    echo "Finished pulling models. Killing temporary server..."; \
    kill $(pgrep ollama); \
    # Wait for the processes to be fully killed
    while pgrep ollama > /dev/null; do \
        sleep 1; \
    done; \
    echo "Ollama server stopped. Build step complete."