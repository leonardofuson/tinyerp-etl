import requests
import json
import datetime
import time 
import os   
import psycopg2 # Driver para PostgreSQL
from psycopg2 import sql # Para construir queries SQL de forma segura

# --- Configurações Lidas de Variáveis de Ambiente ---
API_V2_TOKEN = os.environ.get("TINY_API_V2_TOKEN")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", "5432") 

BASE_URL_V2 = "https://api.tiny.com.br/api2"

# Endpoints
ENDPOINT_CATEGORIAS = "/produtos.categorias.arvore.php"
ENDPOINT_PRODUTOS_PESQUISA = "/produtos.pesquisa.php"
ENDPOINT_PRODUTO_OBTER_ESTOQUE = "/produto.obter.estoque.php" 
ENDPOINT_PEDIDOS_PESQUISA = "/pedidos.pesquisa.php"
ENDPOINT_PEDIDO_OBTER = "/pedido.obter.php" 

# Nome dos processos para controle de última execução
PROCESSO_CATEGORIAS = "categorias" 
PROCESSO_PRODUTOS = "produtos"
PROCESSO_PEDIDOS = "pedidos"

DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL = "01/01/2000 00:00:00"

# --- Funções de Banco de Dados (PostgreSQL) ---

def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        print("Conexão com PostgreSQL estabelecida com sucesso.")
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
    return conn

def criar_tabelas_db(conn):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS categorias (
            id_categoria INTEGER PRIMARY KEY,
            descricao_categoria TEXT NOT NULL,
            id_categoria_pai INTEGER,
            FOREIGN KEY (id_categoria_pai) REFERENCES categorias (id_categoria)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produtos (
            id_produto INTEGER PRIMARY KEY, 
            nome_produto TEXT,
            codigo_produto TEXT UNIQUE, 
            preco_produto REAL,
            unidade_produto TEXT,
            situacao_produto TEXT,
            data_criacao_produto TEXT,
            gtin_produto TEXT,
            preco_promocional_produto REAL,
            preco_custo_produto REAL,
            tipo_variacao_produto TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produto_estoque_total (
            id_produto INTEGER PRIMARY KEY,
            nome_produto_estoque TEXT,
            saldo_total_api REAL,
            saldo_reservado_api REAL,
            FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produto_estoque_depositos (
            id_estoque_deposito SERIAL PRIMARY KEY,
            id_produto INTEGER NOT NULL,
            nome_produto_estoque TEXT,
            nome_deposito TEXT,
            saldo_deposito REAL,
            desconsiderar_deposito TEXT,
            empresa_deposito TEXT,
            FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE,
            UNIQUE (id_produto, nome_deposito, empresa_deposito)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS pedidos (
            id_pedido INTEGER PRIMARY KEY, 
            numero_pedido TEXT,
            numero_ecommerce TEXT,
            data_pedido TEXT,
            data_prevista TEXT,
            nome_cliente TEXT,
            valor_pedido REAL,
            id_vendedor INTEGER, -- Permite NULL
            nome_vendedor TEXT,
            situacao_pedido TEXT,
            codigo_rastreamento TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS pedido_itens (
            id_item_pedido SERIAL PRIMARY KEY,
            id_pedido INTEGER NOT NULL,          
            id_produto_tiny INTEGER,             
            codigo_produto_pedido TEXT,
            descricao_produto_pedido TEXT,
            quantidade REAL,
            unidade_pedido TEXT,
            valor_unitario_pedido REAL,
            id_grade_pedido TEXT, 
            FOREIGN KEY (id_pedido) REFERENCES pedidos (id_pedido) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS script_ultima_execucao (
            nome_processo TEXT PRIMARY KEY,
            timestamp_ultima_execucao TIMESTAMP WITH TIME ZONE 
        );
        """
    )
    try:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
            table_names = ["categorias", "produtos", "produto_estoque_total", "produto_estoque_depositos", "pedidos", "pedido_itens", "script_ultima_execucao"]
            # for table_name in table_names: # Log reduzido
            #     print(f"Tabela '{table_name}' verificada/criada.")
        conn.commit()
        print("Todas as tabelas foram verificadas/criadas.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao criar tabelas: {error}")
        conn.rollback()

def get_ultima_execucao(conn, nome_processo):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT timestamp_ultima_execucao FROM script_ultima_execucao WHERE nome_processo = %s", (nome_processo,))
            resultado = cur.fetchone()
            if resultado and resultado[0]:
                return (resultado[0] + datetime.timedelta(seconds=1)).strftime("%d/%m/%Y %H:%M:%S") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao buscar última execução para '{nome_processo}': {error}")
    return None

def set_ultima_execucao(conn, nome_processo, timestamp=None):
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc) 
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO script_ultima_execucao (nome_processo, timestamp_ultima_execucao)
                VALUES (%s, %s)
                ON CONFLICT (nome_processo) DO UPDATE SET
                    timestamp_ultima_execucao = EXCLUDED.timestamp_ultima_execucao;
            """, (nome_processo, timestamp))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao definir última execução para '{nome_processo}': {error}")
        conn.rollback()

def salvar_categoria_db(conn, categoria_dict, id_pai=None):
    cat_id_str = categoria_dict.get("id") 
    cat_descricao = categoria_dict.get("descricao")
    if cat_id_str and cat_descricao: 
        try:
            cat_id = int(cat_id_str) 
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO categorias (id_categoria, descricao_categoria, id_categoria_pai)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id_categoria) DO UPDATE SET
                        descricao_categoria = EXCLUDED.descricao_categoria,
                        id_categoria_pai = EXCLUDED.id_categoria_pai;
                """, (cat_id, cat_descricao, id_pai))
            if "nodes" in categoria_dict and isinstance(categoria_dict["nodes"], list):
                for sub_categoria_dict in categoria_dict["nodes"]:
                    salvar_categoria_db(conn, sub_categoria_dict, id_pai=cat_id) 
        except ValueError: print(f"ERRO ao converter ID da categoria '{cat_id_str}' para número.")
        except (Exception, psycopg2.DatabaseError) as error: 
            print(f"Erro PostgreSQL ao inserir/atualizar categoria '{cat_id_str}': {error}")
            conn.rollback()

def salvar_produto_db(conn, produto_lista_dict):
    try:
        id_produto = int(produto_lista_dict.get("id"))
        nome_produto = produto_lista_dict.get("nome")
        codigo_produto_api = produto_lista_dict.get("codigo")
        codigo_produto_db = codigo_produto_api if codigo_produto_api and codigo_produto_api.strip() != "" else None

        preco = float(produto_lista_dict.get("preco", 0)) if produto_lista_dict.get("preco") not in [None, ''] else 0.0
        preco_promocional = float(produto_lista_dict.get("preco_promocional", 0)) if produto_lista_dict.get("preco_promocional") not in [None, ''] else 0.0
        preco_custo = float(produto_lista_dict.get("preco_custo", 0)) if produto_lista_dict.get("preco_custo") not in [None, ''] else 0.0
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO produtos (
                    id_produto, nome_produto, codigo_produto, preco_produto, unidade_produto, 
                    situacao_produto, data_criacao_produto, gtin_produto, 
                    preco_promocional_produto, preco_custo_produto, tipo_variacao_produto
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (id_produto) DO UPDATE SET
                    nome_produto = EXCLUDED.nome_produto,
                    codigo_produto = EXCLUDED.codigo_produto,
                    preco_produto = EXCLUDED.preco_produto,
                    unidade_produto = EXCLUDED.unidade_produto,
                    situacao_produto = EXCLUDED.situacao_produto,
                    data_criacao_produto = EXCLUDED.data_criacao_produto,
                    gtin_produto = EXCLUDED.gtin_produto,
                    preco_promocional_produto = EXCLUDED.preco_promocional_produto,
                    preco_custo_produto = EXCLUDED.preco_custo_produto,
                    tipo_variacao_produto = EXCLUDED.tipo_variacao_produto;
            """, (
                id_produto, nome_produto, codigo_produto_db, 
                preco, produto_lista_dict.get("unidade"), produto_lista_dict.get("situacao"),
                produto_lista_dict.get("data_criacao"), produto_lista_dict.get("gtin"),
                preco_promocional, preco_custo, produto_lista_dict.get("tipoVariacao")
            ))
    except ValueError: print(f"Erro de valor ao processar dados do produto ID '{produto_lista_dict.get('id')}'.")
    except (Exception, psycopg2.DatabaseError) as error: 
        print(f"Erro PostgreSQL ao inserir/atualizar produto {produto_lista_dict.get('id')}: {error}")
        conn.rollback() 
        raise 

def salvar_produto_estoque_total_db(conn, id_produto_api, nome_produto_api, saldo_total_api_valor, saldo_reservado_api_valor):
    try:
        id_produto_int = int(id_produto_api)
        saldo_total_str = str(saldo_total_api_valor).strip() if saldo_total_api_valor is not None else ""
        saldo_reservado_str = str(saldo_reservado_api_valor).strip() if saldo_reservado_api_valor is not None else ""

        saldo_total = float(saldo_total_str) if saldo_total_str != "" else 0.0
        saldo_reservado = float(saldo_reservado_str) if saldo_reservado_str != "" else 0.0
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO produto_estoque_total (id_produto, nome_produto_estoque, saldo_total_api, saldo_reservado_api)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_produto) DO UPDATE SET
                    nome_produto_estoque = EXCLUDED.nome_produto_estoque,
                    saldo_total_api = EXCLUDED.saldo_total_api,
                    saldo_reservado_api = EXCLUDED.saldo_reservado_api;
            """, (id_produto_int, nome_produto_api, saldo_total, saldo_reservado))
    except ValueError:
        print(f"    ERRO de valor ao converter estoque para Produto ID {id_produto_api}. Saldo: '{saldo_total_api_valor}', Reservado: '{saldo_reservado_api_valor}'.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"    Erro PostgreSQL ao salvar estoque total para Produto ID {id_produto_api}: {error}")
        conn.rollback()
        raise

def salvar_estoque_por_deposito_db(conn, id_produto_api, nome_produto_api, lista_depositos_api):
    id_produto_int = int(id_produto_api)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_estoque_depositos WHERE id_produto = %s", (id_produto_int,))
        
            if not lista_depositos_api or not isinstance(lista_depositos_api, list):
                return
            
            for dep_wrapper in lista_depositos_api:
                dep_data = dep_wrapper.get("deposito")
                if dep_data and isinstance(dep_data, dict):
                    try:
                        nome_dep = dep_data.get("nome")
                        saldo_dep_str = dep_data.get("saldo")
                        saldo_dep = float(saldo_dep_str) if saldo_dep_str is not None and str(saldo_dep_str).strip() != '' else 0.0
                        desconsiderar = dep_data.get("desconsiderar")
                        empresa = dep_data.get("empresa")

                        cur.execute("""
                            INSERT INTO produto_estoque_depositos 
                            (id_produto, nome_produto_estoque, nome_deposito, saldo_deposito, desconsiderar_deposito, empresa_deposito)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id_produto, nome_deposito, empresa_deposito) DO UPDATE SET 
                                nome_produto_estoque = EXCLUDED.nome_produto_estoque,
                                saldo_deposito = EXCLUDED.saldo_deposito,
                                desconsiderar_deposito = EXCLUDED.desconsiderar_deposito;
                        """, (id_produto_int, nome_produto_api, nome_dep, saldo_dep, desconsiderar, empresa))
                    except ValueError:
                        print(f"        ERRO de valor ao processar dados do depósito '{nome_dep}' para produto ID {id_produto_int}. Saldo: '{saldo_dep_str}'.")
                    except (Exception, psycopg2.DatabaseError) as error_item:
                        print(f"        Erro PostgreSQL ao inserir estoque do depósito '{nome_dep}' para produto ID {id_produto_int}: {error_item}")
    except (Exception, psycopg2.DatabaseError) as error_main:
        print(f"    Erro PostgreSQL na operação de estoque por depósito para produto ID {id_produto_int}: {error_main}")
        conn.rollback()
        raise

def salvar_pedido_db(conn, pedido_dict):
    try:
        id_pedido = int(pedido_dict.get("id"))
        valor = float(pedido_dict.get("valor", 0)) if pedido_dict.get("valor") not in [None, ''] else 0.0
        
        # Tratamento para id_vendedor que pode ser string vazia
        id_vendedor_str = pedido_dict.get("id_vendedor")
        id_vendedor_db = int(id_vendedor_str) if id_vendedor_str and str(id_vendedor_str).strip() != "" else None

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pedidos (
                    id_pedido, numero_pedido, numero_ecommerce, data_pedido, data_prevista,
                    nome_cliente, valor_pedido, id_vendedor, nome_vendedor, 
                    situacao_pedido, codigo_rastreamento
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_pedido) DO UPDATE SET
                    numero_pedido = EXCLUDED.numero_pedido,
                    numero_ecommerce = EXCLUDED.numero_ecommerce,
                    data_pedido = EXCLUDED.data_pedido,
                    data_prevista = EXCLUDED.data_prevista,
                    nome_cliente = EXCLUDED.nome_cliente,
                    valor_pedido = EXCLUDED.valor_pedido,
                    id_vendedor = EXCLUDED.id_vendedor,
                    nome_vendedor = EXCLUDED.nome_vendedor,
                    situacao_pedido = EXCLUDED.situacao_pedido,
                    codigo_rastreamento = EXCLUDED.codigo_rastreamento;
            """, (
                id_pedido, pedido_dict.get("numero"), pedido_dict.get("numero_ecommerce"),
                pedido_dict.get("data_pedido"), pedido_dict.get("data_prevista"), pedido_dict.get("nome"),
                valor, id_vendedor_db, pedido_dict.get("nome_vendedor"), # Usando id_vendedor tratado
                pedido_dict.get("situacao"), pedido_dict.get("codigo_rastreamento")
            ))
    except ValueError as ve: 
        print(f"Erro de valor ao processar dados do pedido ID '{pedido_dict.get('id')}': {ve}")
    except (Exception, psycopg2.DatabaseError) as error: 
        print(f"Erro PostgreSQL ao inserir/atualizar pedido {pedido_dict.get('id')}: {error}")
        conn.rollback()
        raise

def salvar_pedido_itens_db(conn, id_pedido_api, itens_lista_api):
    id_pedido_int = int(id_pedido_api)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pedido_itens WHERE id_pedido = %s", (id_pedido_int,))
            if not itens_lista_api or not isinstance(itens_lista_api, list): return
            for item_dict_wrapper in itens_lista_api: 
                item_data = item_dict_wrapper.get("item")
                if not item_data or not isinstance(item_data, dict): continue
                try:
                    id_produto_tiny_str = item_data.get("id_produto")
                    id_produto_tiny = int(id_produto_tiny_str) if id_produto_tiny_str and str(id_produto_tiny_str).strip() != "" else None
                     
                    quantidade_str = item_data.get("quantidade")
                    quantidade = float(quantidade_str) if quantidade_str is not None and str(quantidade_str).strip() != "" else 0.0
                    
                    valor_unitario_str = item_data.get("valor_unitario")
                    valor_unitario = float(valor_unitario_str) if valor_unitario_str is not None and str(valor_unitario_str).strip() != "" else 0.0
                    
                    cur.execute("""
                        INSERT INTO pedido_itens (
                            id_pedido, id_produto_tiny, codigo_produto_pedido, descricao_produto_pedido,
                            quantidade, unidade_pedido, valor_unitario_pedido, id_grade_pedido
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        id_pedido_int, id_produto_tiny, item_data.get("codigo"), item_data.get("descricao"),
                        quantidade, item_data.get("unidade"), valor_unitario, item_data.get("id_grade")
                    ))
                except ValueError: print(f"      ERRO de valor (item pedido) {id_pedido_int}, produto '{item_data.get('id_produto')}'. Dados: {item_data}")
                except (Exception, psycopg2.DatabaseError) as error_item:
                    print(f"      Erro PostgreSQL (item pedido) {id_pedido_int}, produto '{item_data.get('id_produto')}': {error_item}")
    except (Exception, psycopg2.DatabaseError) as error_main:
        print(f"    Erro PostgreSQL (itens pedido) {id_pedido_int}: {error_main}")
        conn.rollback()
        raise

# --- Funções da API ---
def make_api_v2_request(endpoint_path, method="GET", extra_params=None):
    full_url = f"{BASE_URL_V2}{endpoint_path}"
    params = {"token": API_V2_TOKEN}
    if endpoint_path in [ENDPOINT_PRODUTOS_PESQUISA, ENDPOINT_PRODUTO_OBTER_ESTOQUE, ENDPOINT_PEDIDOS_PESQUISA, ENDPOINT_PEDIDO_OBTER]:
        params["formato"] = "json"
    if extra_params: params.update(extra_params)

    params_sem_token = params.copy(); 
    if "token" in params_sem_token: params_sem_token["token"] = "TOKEN_OCULTADO_NO_LOG"
    # print(f"\nFazendo requisição {method} para: {full_url}\nCom parâmetros: {params_sem_token}")

    response = None
    try:
        if method.upper() == "GET": response = requests.get(full_url, params=params, timeout=45)
        else: print(f"Método {method} não suportado."); return None, False
        
        # if endpoint_path == ENDPOINT_PRODUTO_OBTER_ESTOQUE or endpoint_path == ENDPOINT_PEDIDO_OBTER: # Log detalhado
        #     print(f"\n--- Resposta Bruta de {endpoint_path} (Status: {response.status_code}) ---")
        #     try:
        #         print(json.dumps(response.json(), indent=2, ensure_ascii=False)) 
        #     except json.JSONDecodeError:
        #         print(f"Corpo da resposta (não JSON): {response.text[:200]}...")
        #     print(f"--- Fim da Resposta Bruta de {endpoint_path} ---")

        response.raise_for_status()
        response_data = response.json() 
        retorno_geral = response_data.get("retorno")
        
        if not retorno_geral:
            print(f"ERRO: Chave 'retorno' ausente na resposta JSON de {endpoint_path}.")
            return None, False

        if endpoint_path == ENDPOINT_CATEGORIAS:
            if isinstance(retorno_geral, list): return retorno_geral, True 
            elif isinstance(retorno_geral, dict) and retorno_geral.get("status") != "OK":
                print(f"ERRO API Tiny (Categorias): Status '{retorno_geral.get('status')}'")
                return None, False
            else: 
                print(f"ERRO: Resposta inesperada para Categorias: {type(retorno_geral)}.")
                return None, False

        if not isinstance(retorno_geral, dict):
            print(f"ERRO: 'retorno' não é um dicionário para {endpoint_path}.")
            return None, False

        status_api = retorno_geral.get("status")
        status_processamento = str(retorno_geral.get("status_processamento", ""))
        
        if status_api != "OK":
            print(f"ERRO API Tiny: {status_api} (Endpoint: {endpoint_path})")
            return None, False 
        if status_processamento != "3": 
            print(f"AVISO: Status de processamento não é '3', mas '{status_processamento}' (Endpoint: {endpoint_path}).")
        return retorno_geral, True
    except requests.exceptions.HTTPError as e: 
        print(f"!!! Erro HTTP: {e} (Endpoint: {endpoint_path})")
        if response is not None: print(f"Corpo da Resposta: {response.text}")
    except Exception as e: print(f"!!! Ocorreu um erro inesperado em make_api_v2_request ({endpoint_path}): {e}")
    return None, False

def get_categorias_v2(conn):
    print("\n--- Buscando e Salvando Categorias (API v2) ---")
    lista_categorias, sucesso = make_api_v2_request(ENDPOINT_CATEGORIAS)
    if sucesso and isinstance(lista_categorias, list): 
        for categoria_raiz_dict in lista_categorias:
            salvar_categoria_db(conn, categoria_raiz_dict, id_pai=None)
        conn.commit()
        print(f"Categorias processadas. {len(lista_categorias)} categorias raiz afetadas/verificadas.")
        return True
    print("\nNão foi possível buscar ou salvar as categorias.")
    return False

def get_estoque_produto_v2(id_produto_tiny):
    params = {"id": id_produto_tiny}
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PRODUTO_OBTER_ESTOQUE, extra_params=params)
    if sucesso and retorno_obj:
        produto_estoque_data = retorno_obj.get("produto") 
        if produto_estoque_data:
            return produto_estoque_data 
    return None 

def search_produtos_v2(conn, data_alteracao_inicial=None, pagina=1):
    params = {"pagina": pagina}
    if data_alteracao_inicial:
        params["dataAlteracaoInicial"] = data_alteracao_inicial
        
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PRODUTOS_PESQUISA, extra_params=params)
    produtos_processados_nesta_pagina = 0
    if sucesso and retorno_obj:
        produtos_lista_api = retorno_obj.get("produtos")
        if produtos_lista_api and isinstance(produtos_lista_api, list):
            for item_produto in produtos_lista_api:
                produto_data_lista = item_produto.get("produto") 
                if produto_data_lista:
                    id_produto_atual_api_str = produto_data_lista.get("id")
                    try:
                        id_produto_atual_api = int(id_produto_atual_api_str)
                        salvar_produto_db(conn, produto_data_lista) # Salva dados básicos
                        
                        time.sleep(0.7) 
                        dados_estoque_produto_api = get_estoque_produto_v2(id_produto_atual_api)
                        
                        if dados_estoque_produto_api:
                            nome_prod_do_estoque = dados_estoque_produto_api.get("nome", produto_data_lista.get("nome"))
                            saldo_total = dados_estoque_produto_api.get("saldo")
                            saldo_reservado = dados_estoque_produto_api.get("saldoReservado")
                            salvar_produto_estoque_total_db(conn, id_produto_atual_api, nome_prod_do_estoque, saldo_total, saldo_reservado)
                            
                            lista_depositos = dados_estoque_produto_api.get("depositos")
                            if lista_depositos:
                                salvar_estoque_por_deposito_db(conn, id_produto_atual_api, nome_prod_do_estoque, lista_depositos)
                        else:
                            salvar_produto_estoque_total_db(conn, id_produto_atual_api, produto_data_lista.get("nome"), 0, 0)
                        produtos_processados_nesta_pagina += 1
                    except ValueError:
                        print(f"  ERRO: ID do produto '{id_produto_atual_api_str}' não é um número válido.")
                    except Exception as e_prod_loop:
                        print(f"  ERRO inesperado no loop de produto {id_produto_atual_api_str}: {e_prod_loop}")
                        conn.rollback() 
                        continue 
            conn.commit() 
            print(f"Produtos da página {pagina} ({produtos_processados_nesta_pagina} processados) e seus estoques salvos.")
            return produtos_lista_api, int(retorno_obj.get('numero_paginas', 0))
        else:
            print(f"  Nenhum produto retornado na chave 'produtos' ou não é uma lista (Página {pagina}).")
    return None, 0

def get_detalhes_pedido_v2(id_pedido_api):
    params = {"id": id_pedido_api} 
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PEDIDO_OBTER, extra_params=params)
    if sucesso and retorno_obj:
        pedido_detalhe = retorno_obj.get("pedido")
        if pedido_detalhe:
            return pedido_detalhe 
    return None

def search_pedidos_v2(conn, data_alteracao_ou_inicial=None, pagina=1):
    params = {"pagina": pagina} 
    if data_alteracao_ou_inicial:
        params["dataAlteracaoInicial"] = data_alteracao_ou_inicial
        
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PEDIDOS_PESQUISA, extra_params=params)
    pedidos_processados_nesta_pagina = 0
    if sucesso and retorno_obj:
        pedidos_lista_api = retorno_obj.get("pedidos")
        if pedidos_lista_api and isinstance(pedidos_lista_api, list):
            for item_pedido in pedidos_lista_api:
                pedido_data_api = item_pedido.get("pedido")
                if pedido_data_api:
                    id_pedido_atual_api_str = pedido_data_api.get("id")
                    try:
                        id_pedido_atual_api = int(id_pedido_atual_api_str)
                        salvar_pedido_db(conn, pedido_data_api) 
                        
                        time.sleep(0.6) 
                        detalhes_pedido = get_detalhes_pedido_v2(id_pedido_atual_api)
                        if detalhes_pedido and "itens" in detalhes_pedido:
                            lista_de_itens_bruta = detalhes_pedido["itens"]
                            salvar_pedido_itens_db(conn, id_pedido_atual_api, lista_de_itens_bruta)
                        pedidos_processados_nesta_pagina +=1
                    except ValueError:
                        print(f"  ERRO: ID do pedido '{id_pedido_atual_api_str}' não é um número válido.")
                    except Exception as e_ped_loop:
                        print(f"  ERRO inesperado no loop de pedido {id_pedido_atual_api_str}: {e_ped_loop}")
                        conn.rollback()
                        continue 
            conn.commit()
            print(f"Pedidos (e seus itens) da página {pagina} ({pedidos_processados_nesta_pagina} processados) salvos.")
            return pedidos_lista_api, int(retorno_obj.get('numero_paginas', 0))
        else:
            print(f"  Nenhum pedido retornado na chave 'pedidos' ou não é uma lista (Página {pagina}).")
    return None, 0

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    print("=== Iniciando Cliente para API v2 do Tiny ERP (PostgreSQL, Incremental, Estoque Detalhado) ===")
    
    start_time_total = time.time()

    if not all([API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("ERRO CRÍTICO: Variáveis de ambiente para API ou Banco de Dados não configuradas.")
        print("Configure: TINY_API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT.")
        exit()

    db_conn = get_db_connection()
    if db_conn is None:
        print("Não foi possível conectar ao banco de dados PostgreSQL. Encerrando.")
        exit()
    
    try:
        criar_tabelas_db(db_conn)

        # --- PASSO 1: Processar Categorias ---
        print("\nPASSO 1: Processando Categorias (Carga completa a cada execução)")
        get_categorias_v2(db_conn) 
        print("-" * 70)

        # --- PASSO 2: Processar Produtos com Estoque (Incremental) ---
        print("\nPASSO 2: Processando Produtos com Estoque")
        ultima_exec_produtos_str = get_ultima_execucao(db_conn, PROCESSO_PRODUTOS)
        data_filtro_produtos = ultima_exec_produtos_str if ultima_exec_produtos_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        produtos_total_processado_script = 0
        pagina_atual_prod = 1
        
        print(f"Iniciando busca de produtos (alterados desde: {data_filtro_produtos}).")
        while True: # Loop para todas as páginas de produtos
            print(f"Processando página {pagina_atual_prod} de produtos...")
            produtos_api_pagina, total_paginas_api = search_produtos_v2(db_conn, 
                                                                data_alteracao_inicial=data_filtro_produtos, 
                                                                pagina=pagina_atual_prod)
            if produtos_api_pagina:
                produtos_total_processado_script += len(produtos_api_pagina)
                if pagina_atual_prod >= total_paginas_api or total_paginas_api == 0:
                    print("Todas as páginas de produtos disponíveis foram processadas.")
                    break
                pagina_atual_prod += 1
            else: 
                print(f"Nenhum produto novo/alterado encontrado na página {pagina_atual_prod-1} (ou anterior) ou erro. Fim da busca de produtos.")
                break 
            
            if pagina_atual_prod <= total_paginas_api: 
                 print("Pausa de 1 segundo antes da próxima página de produtos...")
                 time.sleep(1) 
        
        set_ultima_execucao(db_conn, PROCESSO_PRODUTOS) # Atualiza o timestamp após o loop completo
        print(f"Total de {produtos_total_processado_script} produtos processados/atualizados nesta execução.")
        print("-" * 70)
        
        # --- PASSO 3: Processar Pedidos e seus Itens (Incremental) ---
        print("\nPASSO 3: Processando Pedidos e Seus Itens")
        ultima_exec_pedidos_str = get_ultima_execucao(db_conn, PROCESSO_PEDIDOS)
        data_filtro_pedidos = ultima_exec_pedidos_str if ultima_exec_pedidos_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        pedidos_total_processado_script = 0
        pagina_atual_ped = 1
        
        print(f"Iniciando busca de pedidos (alterados desde: {data_filtro_pedidos}).")
        while True: # Loop para todas as páginas de pedidos
            print(f"Processando página {pagina_atual_ped} de pedidos...")
            pedidos_api_pagina, total_paginas_api_ped = search_pedidos_v2(db_conn, 
                                                            data_alteracao_ou_inicial=data_filtro_pedidos, 
                                                            pagina=pagina_atual_ped)
            if pedidos_api_pagina:
                pedidos_total_processado_script += len(pedidos_api_pagina)
                if pagina_atual_ped >= total_paginas_api_ped or total_paginas_api_ped == 0:
                    print("Todas as páginas de pedidos disponíveis foram processadas.")
                    break
                pagina_atual_ped += 1
            else:
                print(f"Nenhum pedido novo/alterado encontrado na página {pagina_atual_ped-1} (ou anterior) ou erro. Fim da busca de pedidos.")
                break
            if pagina_atual_ped <= total_paginas_api_ped: 
                 print("Pausa de 1 segundo antes da próxima página de pedidos...")
                 time.sleep(1) 
        
        set_ultima_execucao(db_conn, PROCESSO_PEDIDOS) # Atualiza o timestamp após o loop completo
        print(f"Total de {pedidos_total_processado_script} pedidos processados/atualizados nesta execução.")
        print("-" * 70)

        # Contagem final das tabelas
        print("\nContagem final dos registros no banco de dados:")
        with db_conn.cursor() as cur:
            tabelas_para_contar = ["categorias", "produtos", "produto_estoque_total", "produto_estoque_depositos", "pedidos", "pedido_itens", "script_ultima_execucao"]
            for tabela in tabelas_para_contar:
                try:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tabela)))
                    count = cur.fetchone()[0]
                    print(f"  - Tabela '{tabela}': {count} registros.")
                except (Exception, psycopg2.DatabaseError) as e:
                    print(f"  Erro ao contar registros da tabela {tabela}: {e}")
        
    except (Exception, psycopg2.DatabaseError) as error_geral:
        print(f"ERRO GERAL NO PROCESSAMENTO: {error_geral}")
        if db_conn:
            try:
                db_conn.rollback()
                print("Rollback da transação geral efetuado devido a erro.")
            except Exception as e_rollback:
                print(f"Erro durante o rollback da transação geral: {e_rollback}")
    finally:
        if db_conn:
            db_conn.close()
            print("\nConexão com PostgreSQL fechada.")
    
    end_time_total = time.time()
    print(f"\n=== Processo Concluído em {end_time_total - start_time_total:.2f} segundos ===")