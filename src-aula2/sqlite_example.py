import sqlite3
import os

DB_FILE = "enquete.db"


def setup_database():
    """Cria e/ou zera o banco de dados e as tabelas."""
    # Apaga o arquivo do banco de dados se ele já existir, para começar do zero
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Criar tabelas
    cursor.execute('''
                   CREATE TABLE polls
                   (
                       id       INTEGER PRIMARY KEY,
                       question TEXT NOT NULL
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE options
                   (
                       id          INTEGER PRIMARY KEY,
                       poll_id     INTEGER NOT NULL,
                       option_text TEXT    NOT NULL,
                       FOREIGN KEY (poll_id) REFERENCES polls (id)
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE users
                   (
                       id   INTEGER PRIMARY KEY,
                       name TEXT NOT NULL UNIQUE
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE votes
                   (
                       id        INTEGER PRIMARY KEY,
                       user_id   INTEGER NOT NULL,
                       poll_id   INTEGER NOT NULL,
                       option_id INTEGER NOT NULL,
                       FOREIGN KEY (user_id) REFERENCES users (id),
                       FOREIGN KEY (poll_id) REFERENCES polls (id),
                       FOREIGN KEY (option_id) REFERENCES options (id),
                       -- A "mágica" do SQL para garantir voto único por usuário na enquete
                       UNIQUE (user_id, poll_id)
                   )
                   ''')
    print("Banco de dados e tabelas criados com sucesso.")
    conn.commit()
    return conn


def seed_data(conn):
    """Popula o banco com dados iniciais (enquete, opções, usuários)."""
    cursor = conn.cursor()
    # Criar a enquete
    cursor.execute("INSERT INTO polls (id, question) VALUES (?, ?)", (1, "Qual sua linguagem favorita?"))

    # Criar as opções
    opcoes = [(1, 1, "A"), (2, 1, "B"), (3, 1, "C")]
    cursor.executemany("INSERT INTO options (id, poll_id, option_text) VALUES (?, ?, ?)", opcoes)

    # Criar os usuários
    usuarios = [(101, "user:101"), (102, "user:102"), (103, "user:103"), (104, "user:104"), (105, "user:105")]
    cursor.executemany("INSERT INTO users (id, name) VALUES (?, ?)", usuarios)

    print("Dados iniciais (enquete, opções, usuários) inseridos.")
    conn.commit()


def votar_sql(conn, id_enquete, id_usuario, id_opcao):
    """Registra o voto de um usuário."""
    cursor = conn.cursor()
    try:
        # Tenta inserir o voto. O banco de dados irá rejeitar se o par (user_id, poll_id) já existir.
        cursor.execute(
            "INSERT INTO votes (user_id, poll_id, option_id) VALUES (?, ?, ?)",
            (id_usuario, id_enquete, id_opcao)
        )
        conn.commit()
        print(f"✅ Voto de 'user:{id_usuario}' para a opção de ID '{id_opcao}' registrado!")
        return True
    except sqlite3.IntegrityError:
        # Este erro acontece quando a restrição UNIQUE é violada
        print(f"⚠️  Usuário 'user:{id_usuario}' já votou nesta enquete.")
        return False


def obter_resultados_sql(conn, id_enquete):
    """Mostra os resultados da enquete usando JOIN e GROUP BY."""
    print("\n--- Resultados Parciais (SQL) ---")
    cursor = conn.cursor()
    query = '''
            SELECT o.option_text, \
                   COUNT(v.id) as vote_count
            FROM options o \
                     LEFT JOIN \
                 votes v ON o.id = v.option_id
            WHERE o.poll_id = ?
            GROUP BY o.option_text
            ORDER BY o.option_text; \
            '''
    cursor.execute(query, (id_enquete,))
    for row in cursor.fetchall():
        print(f"Opção {row[0]}: {row[1]} votos")


def mostrar_placar_sql(conn, id_enquete):
    """Mostra o placar ordenado do maior para o menor."""
    print("\n--- Placar em Tempo Real (Ranking SQL) ---")
    cursor = conn.cursor()
    query = '''
            SELECT o.option_text, \
                   COUNT(v.id) as vote_count
            FROM options o \
                     LEFT JOIN \
                 votes v ON o.id = v.option_id
            WHERE o.poll_id = ?
            GROUP BY o.option_text
            ORDER BY vote_count DESC; \
            '''
    cursor.execute(query, (id_enquete,))
    placar = cursor.fetchall()

    if not any(row[1] > 0 for row in placar):
        print("Nenhum voto registrado ainda.")
        return

    for i, row in enumerate(placar):
        print(f"{i + 1}º Lugar: Opção {row[0]} com {row[1]} votos")


# --- DEMONSTRANDO O PODER DO SQL (A LIMITAÇÃO DO REDIS) ---
def analisar_votantes_por_opcao_sql(conn, id_enquete, texto_opcao):
    """
    Responde à pergunta que era difícil no Redis:
    "Quais são os nomes de todos que votaram em uma opção específica?"
    """
    print(f"\n--- Análise: Quem votou na 'Opção {texto_opcao}'? ---")
    cursor = conn.cursor()
    query = '''
            SELECT u.name
            FROM users u \
                     JOIN \
                 votes v ON u.id = v.user_id \
                     JOIN \
                 options o ON v.option_id = o.id
            WHERE o.poll_id = ? \
              AND o.option_text = ?; \
            '''
    cursor.execute(query, (id_enquete, texto_opcao))
    votantes = cursor.fetchall()
    if not votantes:
        print(f"Ninguém votou na 'Opção {texto_opcao}'.")
        return

    for votante in votantes:
        print(f"- {votante[0]}")


# --- Simulação ---
if __name__ == "__main__":
    conn = setup_database()
    seed_data(conn)

    print("\n--- Realizando Votação (SQL) ---")
    # Opção A tem id=1, B id=2, C id=3

    from time import perf_counter_ns
    start = perf_counter_ns()
    votar_sql(conn, 1, 101, 1)  # User 101 vota na A
    votar_sql(conn, 1, 102, 2)  # User 102 vota na B
    votar_sql(conn, 1, 103, 1)  # User 103 vota na A
    votar_sql(conn, 1, 101, 3)  # User 101 tenta votar de novo (na C) -> FALHA
    votar_sql(conn, 1, 104, 3)  # User 104 vota na C
    votar_sql(conn, 1, 105, 1)  # User 105 vota na A
    # print time
    print(f"Tempo de execução dos votos: {(perf_counter_ns()-start)/1_000_000_000:03f} s")

    # Mostrando os resultados
    obter_resultados_sql(conn, 1)
    mostrar_placar_sql(conn, 1)

    # Mostrando a força do SQL onde o Redis era fraco
    analisar_votantes_por_opcao_sql(conn, 1, "A")
    analisar_votantes_por_opcao_sql(conn, 1, "B")

    conn.close()
    print("\nConexão com o banco de dados fechada.")
