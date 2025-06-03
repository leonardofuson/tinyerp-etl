import requests
import json
import datetime
import time
import os
import psycopg2  # Driver para PostgreSQL
from psycopg2 import sql  # Para construir queries SQL de forma segura

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
            id_vendedor INTEGER, 
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
        except ValueError: 
            print(f"ERRO ao converter ID da categoria '{cat_id_str}' para número. Categoria: {categoria_dict}")
        except (Exception, psycopg2.DatabaseError) as error: 
            print(f"Erro PostgreSQL ao inserir/atualizar categoria ID '{cat_id_str}': {error}")

def salvar_produto_db(conn, produto_lista_dict):
    try:
        id_produto_str = str(produto_lista_dict.get("id","")).strip()
        if not id_produto_str or not id_produto_str.isdigit():
            print(f"ERRO CRÍTICO: ID do produto inválido ou ausente: '{id_produto_str}'. Produto não será salvo. Dados: {produto_lista_dict}")
            return 
        id_produto = int(id_produto_str)

        nome_produto = produto_lista_dict.get("nome")
        codigo_produto_api = produto_lista_dict.get("codigo")
        codigo_produto_db = str(codigo_produto_api).strip() if codigo_produto_api and str(codigo_produto_api).strip() != "" else None

        preco_str = str(produto_lista_dict.get("preco", "0")).strip()
        preco = float(preco_str) if preco_str and preco_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
        
        preco_promocional_str = str(produto_lista_dict.get("preco_promocional", "0")).strip()
        preco_promocional = float(preco_promocional_str) if preco_promocional_str and preco_promocional_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
        
        preco_custo_str = str(produto_lista_dict.get("preco_custo", "0")).strip()
        preco_custo = float(preco_custo_str) if preco_custo_str and preco_custo_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
            
        with conn.cursor() as cur:
            if codigo_produto_db is not None:
                cur.execute(
                    "SELECT id_produto FROM produtos WHERE codigo_produto = %s AND id_produto != %s",
                    (codigo_produto_db, id_produto)
                )
                conflicting_product = cur.fetchone()
                if conflicting_product:
                    print(f"AVISO: Código de produto (SKU) '{codigo_produto_db}' já está em uso pelo produto ID {conflicting_product[0]}. "
                          f"O produto ID {id_produto} terá seu código de produto definido como NULL para evitar duplicidade.")
                    codigo_produto_db = None

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
    except ValueError as ve:
        print(f"Erro de VALOR ao processar dados do produto ID '{produto_lista_dict.get('id')}': {ve}. Dados: {produto_lista_dict}")
        raise
    except (Exception, psycopg2.DatabaseError) as error: 
        print(f"Erro PostgreSQL ao inserir/atualizar produto {produto_lista_dict.get('id')}: {error}")
        raise 

def salvar_produto_estoque_total_db(conn, id_produto_api, nome_produto_api, saldo_total_api_valor, saldo_reservado_api_valor):
    try:
        id_produto_int = int(id_produto_api) 
        
        saldo_total_str = str(saldo_total_api_valor).strip() if saldo_total_api_valor is not None else ""
        saldo_reservado_str = str(saldo_reservado_api_valor).strip() if saldo_reservado_api_valor is not None else ""

        saldo_total = float(saldo_total_str) if saldo_total_str and saldo_total_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
        saldo_reservado = float(saldo_reservado_str) if saldo_reservado_str and saldo_reservado_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
        
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
        raise 
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"    Erro PostgreSQL ao salvar estoque total para Produto ID {id_produto_api}: {error}")
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
                        saldo_dep_str = str(dep_data.get("saldo", "0")).strip()
                        saldo_dep = float(saldo_dep_str) if saldo_dep_str and saldo_dep_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
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
                        print(f"        ERRO de valor ao processar dados do depósito '{nome_dep}' para produto ID {id_produto_int}. Saldo: '{dep_data.get('saldo')}'.")
                    except (Exception, psycopg2.DatabaseError) as error_item:
                        print(f"        Erro PostgreSQL ao inserir estoque do depósito '{nome_dep}' para produto ID {id_produto_int}: {error_item}")
    except (Exception, psycopg2.DatabaseError) as error_main:
        print(f"    Erro PostgreSQL na operação de estoque por depósito para produto ID {id_produto_int}: {error_main}")
        raise 

def salvar_pedido_db(conn, pedido_dict):
    try:
        id_pedido_str = str(pedido_dict.get("id", "")).strip()
        if not id_pedido_str or not id_pedido_str.isdigit():
            print(f"ERRO CRÍTICO: ID do pedido inválido ou ausente: '{id_pedido_str}'. Pedido não será salvo. Dados: {pedido_dict}")
            return 
        id_pedido = int(id_pedido_str)

        valor_str = str(pedido_dict.get("valor", "0")).strip()
        valor = float(valor_str) if valor_str and valor_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
        
        id_vendedor_str = str(pedido_dict.get("id_vendedor", "")).strip()
        id_vendedor_db = int(id_vendedor_str) if id_vendedor_str and id_vendedor_str.isdigit() else None

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
                valor, id_vendedor_db, pedido_dict.get("nome_vendedor"),
                pedido_dict.get("situacao"), pedido_dict.get("codigo_rastreamento")
            ))
    except ValueError as ve:
        print(f"Erro de VALOR ao processar dados do pedido ID '{pedido_dict.get('id', 'N/A')}': {ve}. Dados do pedido: {pedido_dict}")
        raise 
    except (Exception, psycopg2.DatabaseError) as error: 
        print(f"Erro PostgreSQL ao inserir/atualizar pedido {pedido_dict.get('id', 'N/A')}: {error}")
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
                    id_produto_tiny_str = str(item_data.get("id_produto", "")).strip()
                    id_produto_tiny = int(id_produto_tiny_str) if id_produto_tiny_str and id_produto_tiny_str.isdigit() else None
                        
                    quantidade_str = str(item_data.get("quantidade", "0")).strip()
                    quantidade = float(quantidade_str) if quantidade_str and quantidade_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
                    
                    valor_unitario_str = str(item_data.get("valor_unitario", "0")).strip()
                    valor_unitario = float(valor_unitario_str) if valor_unitario_str and valor_unitario_str.replace('.', '', 1).replace('-', '', 1).isdigit() else 0.0
                    
                    cur.execute("""
                        INSERT INTO pedido_itens (
                            id_pedido, id_produto_tiny, codigo_produto_pedido, descricao_produto_pedido,
                            quantidade, unidade_pedido, valor_unitario_pedido, id_grade_pedido
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        id_pedido_int, id_produto_tiny, item_data.get("codigo"), item_data.get("descricao"),
                        quantidade, item_data.get("unidade"), valor_unitario, item_data.get("id_grade")
                    ))
                except ValueError: 
                    print(f"        ERRO de valor (item pedido) {id_pedido_int}, produto ID Tiny '{item_data.get('id_produto')}'. Dados: {item_data}")
                except (Exception, psycopg2.DatabaseError) as error_item:
                    print(f"        Erro PostgreSQL (item pedido) {id_pedido_int}, produto ID Tiny '{item_data.get('id_produto')}': {error_item}")
    except (Exception, psycopg2.DatabaseError) as error_main:
        print(f"    Erro PostgreSQL (itens pedido geral) para Pedido ID {id_pedido_int}: {error_main}")
        raise 

# --- Funções da API ---
def make_api_v2_request(endpoint_path, method="GET", extra_params=None, max_retries=3, initial_retry_delay=2):
    full_url = f"{BASE_URL_V2}{endpoint_path}"
    params = {"token": API_V2_TOKEN}
    if endpoint_path in [ENDPOINT_PRODUTOS_PESQUISA, ENDPOINT_PRODUTO_OBTER_ESTOQUE, ENDPOINT_PEDIDOS_PESQUISA, ENDPOINT_PEDIDO_OBTER]:
        params["formato"] = "json"
    if extra_params: params.update(extra_params)

    params_sem_token = params.copy()
    if "token" in params_sem_token: params_sem_token["token"] = "TOKEN_OCULTADO_NO_LOG"
    
    retries_attempted = 0
    current_delay = initial_retry_delay
    
    while retries_attempted <= max_retries:
        response = None 
        try:
            if retries_attempted > 0: 
                print(f"Aguardando {current_delay}s antes da tentativa {retries_attempted + 1}/{max_retries + 1} para {endpoint_path}...")
                time.sleep(current_delay)
                current_delay = min(current_delay * 2, 30) 

            # print(f"\nTentativa {retries_attempted + 1}/{max_retries + 1} - Req: {method} {full_url} Params: {params_sem_token}")
            
            if method.upper() == "GET":
                response = requests.get(full_url, params=params, timeout=60)
            else:
                print(f"Método {method} não suportado."); 
                return None, False 

            response.raise_for_status() 
            
            response_data = response.json() 
            retorno_geral = response_data.get("retorno")
            
            if not retorno_geral:
                print(f"ERRO API: Chave 'retorno' ausente na resposta JSON de {endpoint_path}. Resposta: {str(response_data)[:500]}")
                return None, False 

            if endpoint_path == ENDPOINT_CATEGORIAS:
                if isinstance(retorno_geral, list): return retorno_geral, True 
                if isinstance(retorno_geral, dict):
                    if retorno_geral.get("status") == "OK" and "categorias" in retorno_geral and isinstance(retorno_geral["categorias"], list):
                        return retorno_geral["categorias"], True
                    if retorno_geral.get("status") != "OK":
                        erros = retorno_geral.get("erros", [])
                        msg_erro_api = f"ERRO API Tiny (Categorias): Status '{retorno_geral.get('status')}'"
                        if erros: msg_erro_api += f" Detalhes: {erros}"
                        print(msg_erro_api)
                        return None, False 
                print(f"ERRO API: Resposta inesperada para Categorias: {type(retorno_geral)}. Conteúdo: {str(retorno_geral)[:300]}")
                return None, False 

            if not isinstance(retorno_geral, dict):
                print(f"ERRO API: 'retorno' não é um dicionário para {endpoint_path}. Conteúdo: {str(retorno_geral)[:300]}")
                return None, False 

            status_api = retorno_geral.get("status")
            status_processamento = str(retorno_geral.get("status_processamento", ""))
            
            if status_api != "OK":
                erros_api = retorno_geral.get("erros") 
                codigo_erro_interno = ""
                msg_erro_interno = ""
                if isinstance(erros_api, list) and len(erros_api) > 0 and isinstance(erros_api[0], dict):
                     erro_obj = erros_api[0].get("erro")
                     if isinstance(erro_obj, dict): 
                         codigo_erro_interno = erro_obj.get("codigo", "")
                         msg_erro_interno = erro_obj.get("erro", "")
                     elif isinstance(erro_obj, str): 
                         msg_erro_interno = erro_obj
                
                print(f"ERRO API Tiny: Status '{status_api}' (Endpoint: {endpoint_path}). Código Interno: {codigo_erro_interno}. Mensagem: {msg_erro_interno}. Resposta: {str(retorno_geral)[:500]}")
                if codigo_erro_interno == "2": 
                    print("ERRO CRÍTICO: Token da API Tiny inválido ou expirado. Verifique a variável de ambiente TINY_API_V2_TOKEN.")
                return None, False 

            if status_processamento not in ["3", "10"]: 
                msg_erro_proc = ""
                erros_retorno = retorno_geral.get("erros")
                if isinstance(erros_retorno, list) and erros_retorno:
                    if isinstance(erros_retorno[0], dict) and "erro" in erros_retorno[0]:
                        msg_erro_proc = str(erros_retorno[0]["erro"])
                    else:
                        msg_erro_proc = str(erros_retorno[0])
                
                if "Nenhum registro encontrado" in msg_erro_proc:
                    return retorno_geral, True 

                print(f"AVISO/ERRO API: Status de processamento é '{status_processamento}' (Endpoint: {endpoint_path}). Mensagem: '{msg_erro_proc}'. Resposta: {str(retorno_geral)[:300]}")
                if status_processamento == "2": 
                     return None, False 

            return retorno_geral, True 

        except requests.exceptions.HTTPError as e_http:
            print(f"!!! Erro HTTP (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {e_http}")
            if response is not None: print(f"Corpo da Resposta HTTP: {response.text[:500]}")
            if response is not None and 400 <= response.status_code < 500 and response.status_code != 429:
                return None, False 

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e_net:
            print(f"!!! Erro de Rede/Timeout (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_net).__name__} - {e_net}")

        except requests.exceptions.RequestException as e_req: 
            print(f"!!! Erro de Requisição (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_req).__name__} - {e_req}")
        
        except json.JSONDecodeError as e_json: 
            print(f"!!! Erro ao decodificar JSON da resposta de {endpoint_path} (Tentativa {retries_attempted + 1}/{max_retries + 1}): {e_json}")
            if response is not None: print(f"Corpo da Resposta (não JSON): {response.text[:500]}")
            return None, False 

        except Exception as e_geral: 
            print(f"!!! Ocorreu um erro INESPERADO (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_geral).__name__} - {e_geral}")
            if response is not None: print(f"Corpo da Resposta: {response.text[:500]}")
            return None, False 

        retries_attempted += 1
        if retries_attempted > max_retries:
            print(f"Máximo de {max_retries + 1} tentativas foi atingido para {endpoint_path}. Desistindo.")
            return None, False

    return None, False

def get_categorias_v2(conn):
    print("\n--- Buscando e Salvando Categorias (API v2) ---")
    lista_categorias, sucesso = make_api_v2_request(ENDPOINT_CATEGORias)
    if sucesso and isinstance(lista_categorias, list): 
        if not lista_categorias:
            print("Nenhuma categoria retornada pela API.")
            return True 
        num_categorias_raiz = 0
        for categoria_raiz_dict in lista_categorias:
            if isinstance(categoria_raiz_dict, dict): 
                 salvar_categoria_db(conn, categoria_raiz_dict, id_pai=None)
                 num_categorias_raiz +=1
            else:
                 print(f"AVISO: Item inesperado na lista de categorias raiz não é um dicionário: {categoria_raiz_dict}")
        conn.commit() 
        print(f"Categorias processadas. {num_categorias_raiz} categorias raiz afetadas/verificadas.")
        set_ultima_execucao(conn, PROCESSO_CATEGORIAS) 
        return True
    print(f"\nNão foi possível buscar ou salvar as categorias. Sucesso API: {sucesso}")
    if lista_categorias: print(f"Dados recebidos (parcial): {str(lista_categorias)[:300]}")
    return False

def get_estoque_produto_v2(id_produto_tiny):
    params = {"id": id_produto_tiny}
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PRODUTO_OBTER_ESTOQUE, extra_params=params)
    if sucesso and retorno_obj:
        produto_estoque_data = retorno_obj.get("produto") 
        if produto_estoque_data:
            return produto_estoque_data 
        else:
            return None 
    return None 

def search_produtos_v2(conn, data_alteracao_inicial=None, pagina=1):
    params = {"pagina": pagina}
    if data_alteracao_inicial:
        params["dataAlteracaoInicial"] = data_alteracao_inicial
        
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PRODUTOS_PESQUISA, extra_params=params)
    produtos_processados_nesta_pagina = 0
    produtos_efetivamente_salvos = 0 # Contador para produtos realmente salvos/atualizados com sucesso
    if sucesso and retorno_obj:
        produtos_lista_api = retorno_obj.get("produtos")
        if produtos_lista_api and isinstance(produtos_lista_api, list):
            for item_produto in produtos_lista_api:
                produto_data_lista = item_produto.get("produto") 
                if produto_data_lista:
                    id_produto_atual_api_str = str(produto_data_lista.get("id","")).strip()
                    try:
                        if not id_produto_atual_api_str or not id_produto_atual_api_str.isdigit():
                            print(f"  ERRO (Loop Produto): ID do produto inválido '{id_produto_atual_api_str}'. Pulando. Dados: {produto_data_lista}")
                            continue
                        id_produto_atual_api = int(id_produto_atual_api_str)
                        
                        salvar_produto_db(conn, produto_data_lista) 
                        
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
                            print(f"  INFO (Loop Produto): Não foi possível obter dados de estoque para o produto ID {id_produto_atual_api}. Registrando estoque total como 0.")
                            salvar_produto_estoque_total_db(conn, id_produto_atual_api, produto_data_lista.get("nome"), 0, 0)
                            salvar_estoque_por_deposito_db(conn, id_produto_atual_api, produto_data_lista.get("nome"), []) # Limpa depósitos

                        produtos_efetivamente_salvos += 1 # Incrementa apenas se chegou até aqui sem exceção grave
                    except (ValueError, psycopg2.DatabaseError) as e_db_prod: 
                        print(f"  ERRO DB no processamento do produto ID {id_produto_atual_api_str}: {e_db_prod}. Rollback para este item.")
                        try: conn.rollback()
                        except Exception as e_roll: print(f"  Erro no rollback parcial do produto: {e_roll}")
                        continue 
                    except Exception as e_prod_loop:
                        print(f"  ERRO inesperado no loop de produto ID {id_produto_atual_api_str}: {e_prod_loop}")
                        try: conn.rollback()
                        except Exception as e_roll: print(f"  Erro no rollback parcial do produto: {e_roll}")
                        continue 
            
            produtos_processados_nesta_pagina = len(produtos_lista_api) # Total de produtos na resposta da API para a página

            if produtos_efetivamente_salvos > 0: # Se algum produto da página foi salvo
                 conn.commit() 
                 print(f"Produtos da página {pagina} ({produtos_efetivamente_salvos} de {produtos_processados_nesta_pagina} listados foram processados e commitados).")
            elif produtos_processados_nesta_pagina > 0 : 
                 print(f"Produtos da página {pagina} listados pela API ({produtos_processados_nesta_pagina}), mas nenhum pôde ser processado com sucesso (rollback pode ter ocorrido).")
            
            return produtos_lista_api, int(retorno_obj.get('numero_paginas', 0))
        else:
            pass 
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
    pedidos_efetivamente_salvos = 0
    if sucesso and retorno_obj:
        pedidos_lista_api = retorno_obj.get("pedidos")
        if pedidos_lista_api and isinstance(pedidos_lista_api, list):
            for item_pedido in pedidos_lista_api:
                pedido_data_api = item_pedido.get("pedido")
                if pedido_data_api:
                    id_pedido_atual_api_str = str(pedido_data_api.get("id","")).strip()
                    try:
                        if not id_pedido_atual_api_str or not id_pedido_atual_api_str.isdigit():
                            print(f"  ERRO (Loop Pedido): ID do pedido inválido '{id_pedido_atual_api_str}'. Pulando. Dados: {pedido_data_api}")
                            continue
                        id_pedido_atual_api = int(id_pedido_atual_api_str)
                        
                        salvar_pedido_db(conn, pedido_data_api) 
                        
                        time.sleep(0.6) 
                        detalhes_pedido = get_detalhes_pedido_v2(id_pedido_atual_api)
                        if detalhes_pedido and "itens" in detalhes_pedido:
                            lista_de_itens_bruta = detalhes_pedido["itens"]
                            salvar_pedido_itens_db(conn, id_pedido_atual_api, lista_de_itens_bruta)
                        
                        pedidos_efetivamente_salvos +=1
                    except (ValueError, psycopg2.DatabaseError) as e_db_ped: 
                        print(f"  ERRO DB no processamento do pedido ID {id_pedido_atual_api_str}: {e_db_ped}. Rollback para este item.")
                        try: conn.rollback()
                        except Exception as e_roll: print(f"  Erro no rollback parcial do pedido: {e_roll}")
                        continue 
                    except Exception as e_ped_loop:
                        print(f"  ERRO inesperado no loop de pedido ID {id_pedido_atual_api_str}: {e_ped_loop}")
                        try: conn.rollback()
                        except Exception as e_roll: print(f"  Erro no rollback parcial do pedido: {e_roll}")
                        continue 
            
            pedidos_processados_nesta_pagina = len(pedidos_lista_api)

            if pedidos_efetivamente_salvos > 0:
                conn.commit() 
                print(f"Pedidos (e seus itens) da página {pagina} ({pedidos_efetivamente_salvos} de {pedidos_processados_nesta_pagina} listados foram processados e commitados).")
            elif pedidos_processados_nesta_pagina > 0:
                 print(f"Pedidos da página {pagina} listados pela API ({pedidos_processados_nesta_pagina}), mas nenhum pôde ser processado com sucesso.")

            return pedidos_lista_api, int(retorno_obj.get('numero_paginas', 0))
        else:
            pass
    return None, 0

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    print("=== Iniciando Cliente para API v2 do Tiny ERP (PostgreSQL, Incremental, Estoque Detalhado) ===")
    
    start_time_total = time.time()

    if not all([API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("ERRO CRÍTICO: Variáveis de ambiente para API ou Banco de Dados não configuradas.")
        print("Configure: TINY_API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT (opcional, default 5432).")
        exit()

    db_conn = get_db_connection()
    if db_conn is None:
        print("Não foi possível conectar ao banco de dados PostgreSQL. Encerrando.")
        exit()
    
    try:
        criar_tabelas_db(db_conn) 

        print("\nPASSO 1: Processando Categorias (Carga completa a cada execução)")
        if get_categorias_v2(db_conn):
            print("Categorias processadas com sucesso.")
        else:
            print("Falha ao processar categorias.")
        print("-" * 70)

        print("\nPASSO 2: Processando Produtos com Estoque")
        ultima_exec_produtos_str = get_ultima_execucao(db_conn, PROCESSO_PRODUTOS)
        data_filtro_produtos = ultima_exec_produtos_str if ultima_exec_produtos_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        produtos_total_listados_api = 0
        pagina_atual_prod = 1
        timestamp_inicio_processo_produtos = datetime.datetime.now(datetime.timezone.utc) 
        
        print(f"Iniciando busca de produtos (alterados desde: {data_filtro_produtos}).")
        while True: 
            print(f"Processando página {pagina_atual_prod} de produtos...")
            produtos_api_pagina, total_paginas_api = search_produtos_v2(db_conn, 
                                                                    data_alteracao_inicial=data_filtro_produtos, 
                                                                    pagina=pagina_atual_prod)
            if produtos_api_pagina: 
                produtos_total_listados_api += len(produtos_api_pagina) 
                if pagina_atual_prod >= total_paginas_api or total_paginas_api == 0:
                    print("Todas as páginas de produtos disponíveis foram processadas (ou API indicou 0 páginas).")
                    break
                pagina_atual_prod += 1
            else: 
                print(f"Nenhuma lista de produtos retornada para a página {pagina_atual_prod} (pode ser o fim ou erro na API). Fim da busca de produtos.")
                break 
            
            if pagina_atual_prod <= total_paginas_api: 
                print("Pausa de 1 segundo antes da próxima página de produtos...")
                time.sleep(1) 
        
        set_ultima_execucao(db_conn, PROCESSO_PRODUTOS, timestamp_inicio_processo_produtos) 
        print(f"Total de {produtos_total_listados_api} produtos listados pela API nesta execução (ver logs para detalhes de salvamento).")
        print("-" * 70)
        
        print("\nPASSO 3: Processando Pedidos e Seus Itens")
        ultima_exec_pedidos_str = get_ultima_execucao(db_conn, PROCESSO_PEDIDOS)
        data_filtro_pedidos = ultima_exec_pedidos_str if ultima_exec_pedidos_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        pedidos_total_listados_api = 0
        pagina_atual_ped = 1
        timestamp_inicio_processo_pedidos = datetime.datetime.now(datetime.timezone.utc)
        
        print(f"Iniciando busca de pedidos (alterados desde: {data_filtro_pedidos}).")
        while True: 
            print(f"Processando página {pagina_atual_ped} de pedidos...")
            pedidos_api_pagina, total_paginas_api_ped = search_pedidos_v2(db_conn, 
                                                                      data_alteracao_ou_inicial=data_filtro_pedidos, 
                                                                      pagina=pagina_atual_ped)
            if pedidos_api_pagina:
                pedidos_total_listados_api += len(pedidos_api_pagina)
                if pagina_atual_ped >= total_paginas_api_ped or total_paginas_api_ped == 0:
                    print("Todas as páginas de pedidos disponíveis foram processadas (ou API indicou 0 páginas).")
                    break
                pagina_atual_ped += 1
            else:
                print(f"Nenhuma lista de pedidos retornada para a página {pagina_atual_ped} (pode ser o fim ou erro na API). Fim da busca de pedidos.")
                break
            if pagina_atual_ped <= total_paginas_api_ped: 
                print("Pausa de 1 segundo antes da próxima página de pedidos...")
                time.sleep(1) 
        
        set_ultima_execucao(db_conn, PROCESSO_PEDIDOS, timestamp_inicio_processo_pedidos)
        print(f"Total de {pedidos_total_listados_api} pedidos listados pela API nesta execução (ver logs para detalhes de salvamento).")
        print("-" * 70)

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
        print(f"ERRO GERAL NO PROCESSAMENTO PRINCIPAL: {error_geral}")
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