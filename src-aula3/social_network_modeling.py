import sqlite3
from neo4j import GraphDatabase
import os

# --- Configurações ---
# Para o Neo4j, configure com os dados do seu banco de dados local
NEO4J_URI = "bolt://localhost:7687"  # URI padrão do Neo4j
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"  # <- IMPORTANTE: Altere para a sua senha

# Nome do arquivo do banco de dados SQLite
SQLITE_DB_FILE = "social_network.db"

# --- Modelo de Dados ---
# Vamos criar uma pequena rede social com alguns usuários e suas relações de "seguir"
users_data = [
    (1, "alice", "Alice Wonder"),
    (2, "bob", "Bob Marley"),
    (3, "charlie", "Charlie Brown"),
    (4, "diana", "Diana Prince"),
    (5, "eva", "Eva Green"),
]

# Relações: (seguidor_id, seguido_id)
# Ex: (1, 2) -> Alice segue Bob
follows_data = [
    (1, 2),  # Alice segue Bob
    (1, 3),  # Alice segue Charlie
    (2, 1),  # Bob segue Alice (relação mútua)
    (2, 4),  # Bob segue Diana
    (3, 4),  # Charlie segue Diana
    (4, 5),  # Diana segue Eva
]


def modelagem_sql_com_sqlite():
    """
    Função para criar, popular e consultar a rede social usando SQLite.
    """
    print("--- INICIANDO MODELAGEM COM SQL (SQLite) ---")

    # Remove o banco de dados antigo, se existir, para começar do zero
    if os.path.exists(SQLITE_DB_FILE):
        os.remove(SQLITE_DB_FILE)

    # Conecta ao banco de dados (cria o arquivo se não existir)
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cursor = conn.cursor()

    # --- 1. Criação das Tabelas (Estrutura) ---
    print("\n[SQL] 1. Criando as tabelas 'usuarios' e 'seguidores'...")
    # Tabela para armazenar os usuários
    cursor.execute("""
                   CREATE TABLE usuarios
                   (
                       id            INTEGER PRIMARY KEY,
                       username      TEXT NOT NULL UNIQUE,
                       nome_completo TEXT
                   );
                   """)

    # Tabela de associação para representar a relação "segue" (muitos-para-muitos)
    cursor.execute("""
                   CREATE TABLE seguidores
                   (
                       seguidor_id INTEGER,
                       seguido_id  INTEGER,
                       PRIMARY KEY (seguidor_id, seguido_id),
                       FOREIGN KEY (seguidor_id) REFERENCES usuarios (id),
                       FOREIGN KEY (seguido_id) REFERENCES usuarios (id)
                   );
                   """)
    print("[SQL] Tabelas criadas com sucesso!")

    # --- 2. Inserção de Dados ---
    print("\n[SQL] 2. Inserindo dados de usuários e seguidores...")
    cursor.executemany("INSERT INTO usuarios VALUES (?, ?, ?)", users_data)
    cursor.executemany("INSERT INTO seguidores VALUES (?, ?)", follows_data)
    conn.commit()
    print(f"[SQL] {len(users_data)} usuários e {len(follows_data)} relações de 'seguir' inseridas.")

    # --- 3. Consultas (O Desafio do Relacional) ---
    print("\n[SQL] 3. Executando consultas...")

    # a) Quem Alice (id=1) segue?
    print("\n  a) Quem Alice (id=1) segue?")
    cursor.execute("""
                   SELECT u.nome_completo
                   FROM usuarios u
                            JOIN seguidores s ON u.id = s.seguido_id
                   WHERE s.seguidor_id = 1;
                   """)
    results = cursor.fetchall()
    print(f"     Resultado: {[row[0] for row in results]}")

    # b) Quem são os seguidores de Diana (id=4)?
    print("\n  b) Quem são os seguidores de Diana (id=4)?")
    cursor.execute("""
                   SELECT u.nome_completo
                   FROM usuarios u
                            JOIN seguidores s ON u.id = s.seguidor_id
                   WHERE s.seguido_id = 4;
                   """)
    results = cursor.fetchall()
    print(f"     Resultado: {[row[0] for row in results]}")

    # c) Quem são os "amigos dos amigos"? (Quem as pessoas que Alice segue, também seguem?)
    # Esta é a consulta que começa a mostrar a complexidade dos JOINs.
    print("\n  c) Quem as pessoas que Alice (id=1) segue, também seguem? (Sugestões de amizade)")
    cursor.execute("""
                   SELECT DISTINCT u_sugestao.nome_completo
                   FROM seguidores s1
                            JOIN seguidores s2 ON s1.seguido_id = s2.seguidor_id
                            JOIN usuarios u_sugestao ON u_sugestao.id = s2.seguido_id
                   WHERE s1.seguidor_id = 1 -- Partindo de Alice
                     AND s2.seguido_id != 1      -- Não sugerir a própria Alice
          AND s2.seguido_id NOT IN (  -- Não sugerir pessoas que Alice já segue
              SELECT seguido_id FROM seguidores WHERE seguidor_id = 1
          );
                   """)
    results = cursor.fetchall()
    print(f"     Resultado: {[row[0] for row in results]}")

    # Fechar a conexão
    conn.close()
    print("\n--- MODELAGEM SQL FINALIZADA ---")


class Neo4jModel:
    """
    Classe para gerenciar a conexão e as operações com o Neo4j.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def _execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def clean_database(self):
        print("\n[Neo4j] 0. Limpando o banco de dados para começar do zero...")
        self._execute_query("MATCH (n) DETACH DELETE n")
        print("[Neo4j] Banco de dados limpo.")

    def create_users_and_relationships(self):
        print("\n[Neo4j] 1. Criando nós de Usuários e relacionamentos 'SEGUE'...")
        # Usando UNWIND para criar todos os usuários de uma vez a partir de uma lista
        self._execute_query("""
            UNWIND $users as user
            CREATE (u:Usuario {id: user.id, username: user.username, nome: user.nome})
        """, parameters={'users': [
            {'id': u[0], 'username': u[1], 'nome': u[2]} for u in users_data
        ]})

        # Usando UNWIND para criar todos os relacionamentos de uma vez
        self._execute_query("""
            UNWIND $follows as follow
            MATCH (seguidor:Usuario {id: follow.seguidor_id})
            MATCH (seguido:Usuario {id: follow.seguido_id})
            CREATE (seguidor)-[:SEGUE]->(seguido)
        """, parameters={'follows': [
            {'seguidor_id': f[0], 'seguido_id': f[1]} for f in follows_data
        ]})
        print("[Neo4j] Nós e relacionamentos criados com sucesso!")

    def run_queries(self):
        print("\n[Neo4j] 2. Executando consultas (A Simplicidade do Grafo)...")

        # a) Quem Alice (id=1) segue?
        print("\n  a) Quem Alice (id=1) segue?")
        query_a = """
            MATCH (alice:Usuario {id: 1})-[:SEGUE]->(seguido)
            RETURN seguido.nome
        """
        results = self._execute_query(query_a)
        print(f"     Resultado: {[record['seguido.nome'] for record in results]}")

        # b) Quem são os seguidores de Diana (id=4)?
        print("\n  b) Quem são os seguidores de Diana (id=4)?")
        query_b = """
            MATCH (seguidor)-[:SEGUE]->(diana:Usuario {id: 4})
            RETURN seguidor.nome
        """
        results = self._execute_query(query_b)
        print(f"     Resultado: {[record['seguidor.nome'] for record in results]}")

        # c) Quem são os "amigos dos amigos"? (Quem as pessoas que Alice segue, também seguem?)
        # A consulta em Cypher é muito mais intuitiva e legível.
        print("\n  c) Quem as pessoas que Alice (id=1) segue, também seguem? (Sugestões de amizade)")
        query_c = """
            MATCH (alice:Usuario {id: 1})-[:SEGUE]->(seguido)-[:SEGUE]->(sugestao:Usuario)
            // Garantir que a sugestão não é a própria Alice
            WHERE sugestao.id <> alice.id
            // Garantir que Alice já não segue a sugestão
            AND NOT (alice)-[:SEGUE]->(sugestao)
            RETURN DISTINCT sugestao.nome
        """
        results = self._execute_query(query_c)
        print(f"     Resultado: {[record['sugestao.nome'] for record in results]}")


def modelagem_grafo_com_neo4j():
    """
    Função para criar, popular e consultar a rede social usando Neo4j.
    """
    print("\n\n--- INICIANDO MODELAGEM COM GRAFO (Neo4j) ---")
    try:
        neo4j_model = Neo4jModel(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        # Passos
        neo4j_model.clean_database()
        neo4j_model.create_users_and_relationships()
        neo4j_model.run_queries()

        neo4j_model.close()
        print("\n--- MODELAGEM NEO4J FINALIZADA ---")

    except Exception as e:
        print(
            f"\n[ERRO] Não foi possível conectar ao Neo4j. Verifique se o banco de dados está rodando e as credenciais estão corretas.")
        print(f"Detalhe do erro: {e}")


if __name__ == "__main__":
    # Executa a modelagem relacional
    modelagem_sql_com_sqlite()

    # Executa a modelagem de grafo
    modelagem_grafo_com_neo4j()