import streamlit as st
import os
from langchain_neo4j import Neo4jGraph
from langchain_ollama import ChatOllama
from langchain_neo4j import GraphCypherQAChain
from langchain.prompts.prompt import PromptTemplate
from langchain_core.globals import set_debug

from prompt import CYPHER_GENERATION_TEMPLATE

st.set_page_config(
    page_title="Graph Q&A",
    layout="wide"
)
st.title("Knowledge Graph Q&A")
st.write(
    "Ask questions about the articles processed by the system. "
    "The system will convert your question into a Cypher query,"
    " run it on the Neo4j graph, and use an LLM to generate a natural language answer."
)
st.write("Example: 'Who is the CEO of Acme Corp?' or 'Which companies work on AI?'")

set_debug(os.getenv("LANGCHAIN_DEBUG", "False").lower() == "true")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")

try:
    graph = Neo4jGraph(
        url=NEO4J_URI,
        username=NEO4J_USER,
        password=NEO4J_PASSWORD
    )
    graph.refresh_schema()

    llm = ChatOllama(
        model="mistral",
        base_url=OLLAMA_HOST,
        temperature=0
    )

    CYPHER_GENERATION_PROMPT = PromptTemplate(
        input_variables=["schema", "question"],
        template=CYPHER_GENERATION_TEMPLATE
    )

    chain = GraphCypherQAChain.from_llm(
        llm=llm,
        graph=graph,
        cypher_prompt=CYPHER_GENERATION_PROMPT,
        verbose=True,
        return_intermediate_steps=True,
        allow_dangerous_requests=True,
    )
    st.success("Connected to Neo4j and LLM successfully.")
except Exception as e:
    st.error(f"Error connecting to Neo4j or LLM: {e}")
    st.stop()

question = st.text_input("Your Question:", placeholder="Type your question and press Enter...")

if question:
    with st.spinner("Thinking..."):
        try:
            result = chain.invoke({"query": question})

            st.markdown("### Answer")
            st.write(result['result'])

            with st.expander("Show Generated Cypher Query", expanded=False):
                st.code(result['intermediate_steps'][0]['query'], language='cypher')

        except Exception as e:
            st.error(f"Error processing your question: {e}")

