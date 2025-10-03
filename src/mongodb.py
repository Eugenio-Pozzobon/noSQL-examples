from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, ConnectionFailure
import time

# --- Configuração do MongoDB ---
try:
    # Conecta ao servidor MongoDB (rodando via Docker em localhost)
    client = MongoClient('localhost', 27017)
    # Testa a conexão
    client.admin.command('ping')
    print("Conexão com o MongoDB bem-sucedida!")
except ConnectionFailure:
    print("ERRO: Não foi possível conectar ao MongoDB. O script não pode continuar.")
    print("Por favor, execute: docker run -d --name meu-mongo -p 27017:27017 mongo")
    exit()

# Seleciona o banco de dados e as coleções
db = client['enquete_db']
polls_collection = db['polls']
votes_collection = db['votes']


def setup_mongodb():
    """Limpa os dados antigos e configura o banco para o exemplo."""
    print("\nLimpando e configurando o banco de dados MongoDB...")
    # Limpa as coleções para começar do zero
    polls_collection.delete_many({})
    votes_collection.delete_many({})

    # PONTO-CHAVE: Criar um índice único para garantir a regra de negócio
    # Isso garante que a combinação de poll_id e user_id seja única em toda a coleção.
    votes_collection.create_index([("poll_id", 1), ("user_id", 1)], unique=True)
    print("Índice único criado em 'votes' para (poll_id, user_id).")


def seed_data_mongo():
    """Insere os dados iniciais da enquete."""
    polls_collection.insert_one({
        "_id": 1,
        "question": "Qual sua linguagem favorita?"
    })
    print("Dados iniciais (enquete) inseridos.")


def votar_mongo(id_enquete, id_usuario, opcao):
    """Insere um documento de voto. O índice único cuida da validação."""
    try:
        # Tenta inserir o documento do voto
        votes_collection.insert_one({
            "poll_id": id_enquete,
            "user_id": id_usuario,
            "option_id": opcao
        })
        print(f"✅ Voto de '{id_usuario}' para a 'Opção {opcao}' registrado!")
        return True
    except DuplicateKeyError:
        # Este erro é gerado pelo MongoDB se o índice único for violado
        print(f"⚠️  Usuário '{id_usuario}' já votou nesta enquete.")
        return False


def mostrar_placar_mongo(id_enquete):
    """
    Mostra o placar usando o poderoso Aggregation Framework do MongoDB.
    Isso é o equivalente ao GROUP BY do SQL.
    """
    print("\n--- Placar em Tempo Real (Ranking MongoDB) ---")

    # PONTO-CHAVE: Pipeline de Agregação
    pipeline = [
        # 1. Filtra os votos apenas para a enquete que queremos
        {"$match": {"poll_id": id_enquete}},

        # 2. Agrupa os documentos pela 'option_id' e conta quantos há em cada grupo
        {"$group": {
            "_id": "$option_id",
            "vote_count": {"$sum": 1}
        }},

        # 3. Ordena os resultados pela contagem de votos, do maior para o menor
        {"$sort": {"vote_count": -1}}
    ]

    resultados = list(votes_collection.aggregate(pipeline))

    if not resultados:
        print("Nenhum voto registrado ainda.")
        return

    for i, doc in enumerate(resultados):
        opcao = doc['_id']
        contagem = doc['vote_count']
        print(f"{i + 1}º Lugar: Opção {opcao} com {contagem} votos")


def analisar_votantes_por_opcao_mongo(id_enquete, opcao):
    """
    Responde à pergunta que era difícil no Redis:
    "Quais são os nomes de todos que votaram em uma opção específica?"
    """
    print(f"\n--- Análise: Quem votou na 'Opção {opcao}'? ---")

    # A consulta no MongoDB é muito expressiva e simples
    query = {
        "poll_id": id_enquete,
        "option_id": opcao
    }

    # O .find() retorna um cursor com todos os documentos que correspondem à consulta
    votantes = votes_collection.find(query)

    lista_votantes = [v['user_id'] for v in votantes]

    if not lista_votantes:
        print(f"Ninguém votou na 'Opção {opcao}'.")
        return

    for user_id in lista_votantes:
        print(f"- {user_id}")


# --- Simulação ---
if __name__ == "__main__":
    setup_mongodb()
    seed_data_mongo()

    print("\n--- Realizando Votação (MongoDB) ---")
    votar_mongo(1, "user:101", "A")
    votar_mongo(1, "user:102", "B")
    votar_mongo(1, "user:103", "A")
    votar_mongo(1, "user:101", "C")  # Tentativa de voto duplicado -> FALHA
    votar_mongo(1, "user:104", "C")
    votar_mongo(1, "user:105", "A")

    # Mostrando os resultados
    mostrar_placar_mongo(1)

    # Mostrando a força do MongoDB em consultas flexíveis
    analisar_votantes_por_opcao_mongo(1, "A")

    # Fecha a conexão
    client.close()
