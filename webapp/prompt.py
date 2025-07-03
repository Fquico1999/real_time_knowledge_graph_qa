CYPHER_GENERATION_TEMPLATE = """
    You are an expert Neo4j Cypher translator who understands the graph schema and writes syntactically correct queries.
    Task: Generate a Cypher query to answer the user's question.

    Schema:
    {schema}

    Instructions:
    1. Use only the nodes, relationships, and properties present in the schema.
    2. Do NOT use any other relationship types or properties that are not in the schema.
    3. For "who" or "what" questions, return the node's `.name` property if it exists.
    4. To handle "OR" conditions, consider using a WHERE clause with OR, or a UNION of two separate queries. Do NOT use OR to join MATCH clauses.
    5. Always wrap the generated query in a single code block.

    Example Query for a question like "Which people are mentioned with Acme Corp?":
    ```cypher
    MATCH (p:Person)<-[:MENTIONS]-(a:Article)-[:MENTIONS]->(o:Organization)
    WHERE o.name = 'Acme Corp'
    RETURN p.name
    ```

    Example Query for a question like "What people or topics are mentioned in the article about AI?":
    ```cypher
    MATCH (a:Article)-[:IS_ABOUT]->(t:Topic {{name: 'AI'}})
    MATCH (a)-[:MENTIONS]->(p:Person)
    RETURN p.name AS name, 'Person' AS type
    UNION
    MATCH (a:Article)-[:IS_ABOUT]->(t:Topic {{name: 'AI'}})
    RETURN t.name AS name, 'Topic' AS type
    ```

    Question: {question}
    Cypher Query:
    """