# Use the official Ollama image as the base
FROM ollama/ollama:latest

RUN set -e; \
    ollama serve & \
    ollama_pids=$(pgrep ollama); \
    while [ -z "$ollama_pids" ]; do \
        sleep 1; \
        ollama_pids=$(pgrep ollama); \
    done; \
    echo "Ollama server started. Pre-pulling models..."; \
    ollama pull mistral; \
    ollama pull mixtral; \
    echo "Finished pulling models. Killing temporary server..."; \
    kill $ollama_pids; \
    # Wait for the processes to be fully killed
    while pgrep ollama > /dev/null; do \
        sleep 1; \
    done; \
    echo "Ollama server stopped. Build step complete."