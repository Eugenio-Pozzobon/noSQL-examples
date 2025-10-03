import sqlite3
import redis
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, ConnectionFailure
import time
import os

# --- Configuração do SQLite ---
DB_FILE = "enquete_benchmark.db"

def setup_sqlite():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'CREATE TABLE votes (id INTEGER PRIMARY KEY, user_id INTEGER, poll_id INTEGER, UNIQUE (user_id, poll_id))')
    conn.commit()
    return conn

def votar_sql(conn, id_enquete, id_usuario):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO votes (user_id, poll_id) VALUES (?, ?)",
            (id_usuario, id_enquete)
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

# --- Configuração do Redis ---
try:
    r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
    r.flushdb()  # Limpa o banco de dados do benchmark
    r.ping()
    print("Conexão com o Redis bem-sucedida!")
except redis.exceptions.ConnectionError:
    r = None # Define como None se a conexão falhar
    print("AVISO: Não foi possível conectar ao Redis. O benchmark para Redis será ignorado.")

# --- Configuração do MongoDB ---
try:
    client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    db = client['enquete_benchmark_db']
    votes_collection = db['votes']
    print("Conexão com o MongoDB bem-sucedida!")
except ConnectionFailure:
    client = None # Define como None se a conexão falhar
    print("AVISO: Não foi possível conectar ao MongoDB. O benchmark para MongoDB será ignorado.")


def setup_mongodb():
    if client:
        votes_collection.delete_many({})
        # Tenta remover índices antigos antes de criar o novo
        try:
            votes_collection.drop_indexes()
        except:
            pass # Ignora erro se não houver índices
        votes_collection.create_index([("poll_id", 1), ("user_id", 1)], unique=True)

# --- Funções de Votação para Benchmark ---

def votar_redis_normal(id_enquete, id_usuario, opcao):
    if r.sadd(f"enquete:{id_enquete}:votantes", id_usuario):
        r.incr(f"enquete:{id_enquete}:opcao:{opcao}")
        return True
    return False

def votar_redis_pipelined(id_enquete, id_usuario, opcao):
    pipe = r.pipeline()
    pipe.sadd(f"enquete:{id_enquete}:votantes", id_usuario)
    pipe.incr(f"enquete:{id_enquete}:opcao:{opcao}")
    resultados = pipe.execute()
    return resultados[0] == 1

def votar_mongo(id_enquete, id_usuario, opcao):
    try:
        votes_collection.insert_one({
            "poll_id": id_enquete,
            "user_id": id_usuario,
            "option_id": opcao
        })
        return True
    except DuplicateKeyError:
        return False

# --- O Benchmark ---

if __name__ == "__main__":
    NUM_VOTOS = 100000
    ID_ENQUETE = 1

    print(f"\n--- Realizando benchmark com {NUM_VOTOS} votos ---")

    # Benchmark SQLite
    conn_sqlite = setup_sqlite()
    start_time = time.perf_counter()
    for i in range(NUM_VOTOS):
        votar_sql(conn_sqlite, ID_ENQUETE, i)
    end_time = time.perf_counter()
    print(f"SQLite:              {end_time - start_time:.4f} segundos")
    conn_sqlite.close()

    if r:
        # Benchmark Redis Normal
        start_time = time.perf_counter()
        for i in range(NUM_VOTOS):
            votar_redis_normal(ID_ENQUETE, i, "A")
        end_time = time.perf_counter()
        print(f"Redis (Normal):      {end_time - start_time:.4f} segundos")
        r.flushdb()  # Limpa para o próximo teste

        # Benchmark Redis Pipelined
        start_time = time.perf_counter()
        for i in range(NUM_VOTOS):
            votar_redis_pipelined(ID_ENQUETE, i, "A")
        end_time = time.perf_counter()
        print(f"Redis (Pipelined):   {end_time - start_time:.4f} segundos")
        r.flushdb()

    # Benchmark do MongoDB
    if client:
        setup_mongodb()
        start_time = time.perf_counter()
        for i in range(NUM_VOTOS):
            votar_mongo(ID_ENQUETE, i, "A")
        end_time = time.perf_counter()
        print(f"MongoDB:             {end_time - start_time:.4f} segundos")
        client.close()
