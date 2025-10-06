from redis import Redis, exceptions
import time


def connect_redis():
    try:
        r = Redis(host='localhost', port=6379, db=0, decode_responses=True)
        r.ping()
        print("Conexão com o Redis bem-sucedida!")
    except exceptions.ConnectionError as e:
        print(f"Não foi possível conectar ao Redis: {e}")
        print("Por favor, verifique se o Redis está rodando.")
        exit()

    return r


def votar(r, id_enquete, id_usuario, opcao):
    """
    Registra o voto de um usuário em uma opção.
    Demonstra a atomicidade e o uso de SETs.
    """
    # 1. CAPACIDADE: Usar um SET para garantir que o usuário vote apenas uma vez.
    # O comando SADD retorna 1 se o item foi adicionado (primeiro voto)
    # e 0 se o item já existia (voto repetido).
    if r.sadd(f"enquete:{id_enquete}:votantes", id_usuario):
        # 2. CAPACIDADE: Usar INCR para um contador atômico e rápido.
        votos = r.incr(f"enquete:{id_enquete}:opcao:{opcao}")

        # 3. CAPACIDADE: Usar ZINCRBY para atualizar o placar em tempo real.
        r.zincrby(f"enquete:{id_enquete}:placar", 1, f"Opção {opcao}")

        print(f"✅ Voto de '{id_usuario}' para a 'Opção {opcao}' registrado! Total de votos para a opção: {votos}.")
        return True
    else:
        print(f"⚠️  Usuário '{id_usuario}' já votou nesta enquete.")
        return False

def obter_resultados(r, id_enquete, opcoes):
    """
    Mostra os resultados atuais da enquete.
    """
    print("\n--- Resultados Parciais ---")
    for opcao in opcoes:
        votos = r.get(f"enquete:{id_enquete}:opcao:{opcao}") or 0
        print(f"Opção {opcao}: {votos} votos")

def mostrar_todos_os_dados(r):
    """
    Lista todas as chaves do banco de dados, identifica o tipo de cada uma
    e exibe seu conteúdo de forma apropriada.
    """
    print("\n" + "="*40)
    print("🔎 INSPECIONANDO TODOS OS DADOS NO REDIS 🔎")
    print("="*40)

    # 1. Listar todas as chaves
    # ATENÇÃO: KEYS * é OK para debug, mas NUNCA em produção. Use SCAN.
    chaves = r.keys('enquete:1:*') # Vamos pegar apenas as chaves da nossa enquete

    if not chaves:
        print("Nenhuma chave encontrada no banco de dados com o padrão 'enquete:1:*'.")
        return

    print(f"Encontradas {len(chaves)} chaves.\n")

    # 2. Iterar sobre cada chave para obter seu tipo e valor
    for chave in sorted(chaves):
        # Obtém o tipo da chave (string, set, zset, list, hash)
        tipo = r.type(chave)
        print(f"🔑 Chave: '{chave}'")
        print(f"   🏷️ Tipo: {tipo}")

        valor = None
        # Usa o comando correto para buscar o valor com base no tipo
        if tipo == 'string':
            valor = r.get(chave)
        elif tipo == 'set':
            valor = r.smembers(chave)
        elif tipo == 'zset':
            # zrange com 0 e -1 pega todos os elementos do Sorted Set
            valor = r.zrange(chave, 0, -1, withscores=True)
        elif tipo == 'list':
            valor = r.lrange(chave, 0, -1)
        elif tipo == 'hash':
            valor = r.hgetall(chave)
        else:
            valor = "Tipo de dado não inspecionado neste script."

        print(f"   💾 Valor: {valor}\n")

def mostrar_placar(r, id_enquete):
    """
    Mostra o placar ordenado.
    """
    # ZREVRANGEBYSCORE busca no Sorted Set, ordenando do maior score para o menor.
    placar = r.zrevrange(f"enquete:{id_enquete}:placar", 0, -1, withscores=True)
    print("\n--- Placar em Tempo Real (Ranking) ---")
    if not placar:
        print("Nenhum voto registrado ainda.")
        return

    for i, (opcao, score) in enumerate(placar):
        print(f"{i+1}º Lugar: {opcao} com {int(score)} votos")


if __name__ == "__main__":
    r = connect_redis()
    # --- Simulação ---
    ID_ENQUETE = 1
    OPCOES = ["A", "B", "C"]

    # Limpando dados de uma execução anterior para começar do zero
    print("\n--- INICIANDO SIMULAÇÃO: Limpando dados antigos... ---")
    chaves_para_limpar = r.keys(f"enquete:{ID_ENQUETE}:*")
    if chaves_para_limpar:
        r.delete(*chaves_para_limpar)

    print("\n--- Realizando Votação ---")
    from time import perf_counter_ns
    start = perf_counter_ns()
    votar(r, ID_ENQUETE, "user:101", "A")
    votar(r, ID_ENQUETE, "user:102", "B")
    votar(r, ID_ENQUETE, "user:103", "A")
    votar(r, ID_ENQUETE, "user:101", "C") # Tentativa de voto duplicado
    votar(r, ID_ENQUETE, "user:104", "C")
    votar(r, ID_ENQUETE, "user:105", "A")
    print(f"Tempo de execução dos votos: {(perf_counter_ns() - start)/1_000_000_000:03f} s")



    # Mostrando os resultados
    obter_resultados(r, ID_ENQUETE, OPCOES)
    mostrar_placar(r, ID_ENQUETE)

    mostrar_todos_os_dados(r)