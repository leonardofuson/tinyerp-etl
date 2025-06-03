import requests
import json
import datetime
import time
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values # Para inserção em lote
import logging

# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    handlers=[
        logging.FileHandler("tiny_sync.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TinySync")

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
ENDPOINT_PRODUTO_OBTER = "/produto.obter.php"
ENDPOINT_LISTA_ATUALIZACOES_ESTOQUE = "/lista.atualizacoes.estoque.php"
ENDPOINT_PEDIDOS_PESQUISA = "/pedidos.pesquisa.php"
ENDPOINT_PEDIDO_OBTER = "/pedido.obter.php"

# Nome dos processos para controle de última execução
PROCESSO_CATEGORIAS = "categorias"
PROCESSO_PRODUTOS = "produtos_cadastrais"
PROCESSO_ESTOQUES = "estoques"
PROCESSO_PEDIDOS = "pedidos"

DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL = "01/01/2000 00:00:00"
DEFAULT_API_TIMEOUT = 60 # Timeout padrão para requisições à API
RETRY_DELAY_429 = 30 # Delay específico em segundos para erro 429

# --- Função Auxiliar para Conversão ---
def safe_float_convert(value_str, default=0.0):
    """Converte uma string para float de forma segura, tratando None, ',', e strings vazias."""
    if value_str is None:
        return default
    value_str = str(value_str).strip().replace(',', '.')
    if not value_str:
        return default
    try:
        return float(value_str)
    except ValueError:
        logger.debug(f"Não foi possível converter '{value_str}' para float, usando {default}.")
        return default

# --- Funções de Banco de Dados (PostgreSQL) ---

def get_db_connection():
    """Estabelece e retorna uma conexão com o banco de dados PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            connect_timeout=10 # Timeout para estabelecer conexão
        )
        logger.info("Conexão com PostgreSQL estabelecida com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao conectar ao PostgreSQL: {e}", exc_info=True)
    return conn

def criar_tabelas_db(conn):
    """Cria as tabelas no banco de dados se elas não existirem."""
    # ... (definição dos comandos DDL como na versão anterior) ...
    commands = (
        """
        CREATE TABLE IF NOT EXISTS categorias (
            id_categoria INTEGER PRIMARY KEY,
            descricao_categoria TEXT NOT NULL,
            id_categoria_pai INTEGER,
            FOREIGN KEY (id_categoria_pai) REFERENCES categorias (id_categoria) ON DELETE SET NULL
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
            preco_custo_medio_produto REAL,
            tipo_variacao_produto TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produto_categorias (
            id_produto INTEGER NOT NULL,
            id_categoria INTEGER NOT NULL,
            PRIMARY KEY (id_produto, id_categoria),
            FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE,
            FOREIGN KEY (id_categoria) REFERENCES categorias (id_categoria) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produto_estoque_total (
            id_produto INTEGER PRIMARY KEY,
            nome_produto_estoque TEXT,
            saldo_total_api REAL,
            saldo_reservado_api REAL,
            data_ultima_atualizacao_api TIMESTAMP WITH TIME ZONE,
            FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produto_estoque_depositos (
            id_estoque_deposito SERIAL PRIMARY KEY,
            id_produto INTEGER NOT NULL,
            nome_deposito TEXT,
            saldo_deposito REAL,
            desconsiderar_deposito TEXT, 
            empresa_deposito TEXT,
            FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE,
            UNIQUE (id_produto, nome_deposito) 
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
                logger.debug(f"Executando comando DDL: {command[:100]}...")
                cur.execute(command)
        if conn and not conn.closed:
            conn.commit()
        logger.info("Todas as tabelas foram verificadas/criadas.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Erro ao criar tabelas: {error}", exc_info=True)
        if conn and not conn.closed:
            conn.rollback()
        raise

def get_ultima_execucao(conn, nome_processo):
    """Obtém o timestamp da última execução bem-sucedida de um processo."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT timestamp_ultima_execucao FROM script_ultima_execucao WHERE nome_processo = %s", (nome_processo,))
            resultado = cur.fetchone()
            if resultado and resultado[0]:
                return (resultado[0] + datetime.timedelta(seconds=1)).strftime("%d/%m/%Y %H:%M:%S")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Erro ao buscar última execução para '{nome_processo}': {error}", exc_info=True)
    return None

def set_ultima_execucao(conn, nome_processo, timestamp=None):
    """Define o timestamp da última execução de um processo."""
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
        if conn and not conn.closed:
            conn.commit()
        logger.info(f"Timestamp da última execução para '{nome_processo}' definido como {timestamp.strftime('%d/%m/%Y %H:%M:%S %Z') if isinstance(timestamp, datetime.datetime) else timestamp}.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Erro ao definir última execução para '{nome_processo}': {error}", exc_info=True)
        if conn and not conn.closed:
            conn.rollback()

# --- Funções de Salvamento no Banco de Dados ---

def salvar_categoria_db(conn, categoria_dict, id_pai=None):
    """Salva uma categoria e suas subcategorias recursivamente no banco de dados."""
    # ... (código como na versão anterior) ...
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
            logger.warning(f"ID da categoria inválido '{cat_id_str}'. Categoria: {categoria_dict}", exc_info=False)
        except (Exception, psycopg2.DatabaseError) as error: 
            logger.error(f"Erro PostgreSQL ao inserir/atualizar categoria ID '{cat_id_str}': {error}", exc_info=True)


def salvar_produto_db(conn, produto_api_data):
    """Salva os dados cadastrais de um produto no banco."""
    # ... (código como na versão anterior, usando safe_float_convert) ...
    id_produto_str = str(produto_api_data.get("id","")).strip()
    if not id_produto_str or not id_produto_str.isdigit():
        logger.error(f"ID do produto inválido ou ausente: '{id_produto_str}'. Dados: {produto_api_data}")
        raise ValueError(f"ID do produto inválido: {id_produto_str}")
    id_produto = int(id_produto_str)

    try:
        nome_produto = produto_api_data.get("nome")
        codigo_produto_api = produto_api_data.get("codigo")
        codigo_produto_db = str(codigo_produto_api).strip() if codigo_produto_api and str(codigo_produto_api).strip() != "" else None

        preco = safe_float_convert(produto_api_data.get("preco"))
        preco_promocional = safe_float_convert(produto_api_data.get("preco_promocional"))
        preco_custo = safe_float_convert(produto_api_data.get("preco_custo"))
        preco_custo_medio = safe_float_convert(produto_api_data.get("preco_custo_medio"))
            
        with conn.cursor() as cur:
            if codigo_produto_db is not None:
                cur.execute(
                    "SELECT id_produto FROM produtos WHERE codigo_produto = %s AND id_produto != %s",
                    (codigo_produto_db, id_produto)
                )
                conflicting_product = cur.fetchone()
                if conflicting_product:
                    logger.warning(f"Código de produto (SKU) '{codigo_produto_db}' já está em uso pelo produto ID {conflicting_product[0]}. "
                                   f"O produto ID {id_produto} terá seu código de produto definido como NULL.")
                    codigo_produto_db = None

            cur.execute("""
                INSERT INTO produtos (
                    id_produto, nome_produto, codigo_produto, preco_produto, unidade_produto, 
                    situacao_produto, data_criacao_produto, gtin_produto, 
                    preco_promocional_produto, preco_custo_produto, preco_custo_medio_produto, tipo_variacao_produto
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
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
                    preco_custo_medio_produto = EXCLUDED.preco_custo_medio_produto,
                    tipo_variacao_produto = EXCLUDED.tipo_variacao_produto;
            """, (
                id_produto, nome_produto, codigo_produto_db, 
                preco, produto_api_data.get("unidade"), produto_api_data.get("situacao"),
                produto_api_data.get("data_criacao"), produto_api_data.get("gtin"),
                preco_promocional, preco_custo, preco_custo_medio, produto_api_data.get("tipoVariacao")
            ))
        logger.debug(f"Produto ID {id_produto} salvo/atualizado.")
    except (ValueError, psycopg2.DatabaseError, Exception) as error: 
        logger.error(f"Erro ao processar/salvar produto ID '{id_produto_str}': {error}", exc_info=True)
        raise

def salvar_produto_categorias_db(conn, id_produto, categorias_lista_api):
    """Salva o relacionamento entre um produto e suas categorias usando execute_values."""
    # ... (código como na versão anterior, usando execute_values) ...
    if not isinstance(id_produto, int):
        logger.error(f"ID do produto inválido para salvar categorias: {id_produto}")
        raise ValueError("ID do produto inválido para salvar categorias.")

    try:
        with conn.cursor() as cur:
            logger.debug(f"Limpando categorias existentes para o produto ID {id_produto}.")
            cur.execute("DELETE FROM produto_categorias WHERE id_produto = %s", (id_produto,))
            
            if categorias_lista_api and isinstance(categorias_lista_api, list):
                dados_para_inserir = []
                for cat_item_api in categorias_lista_api:
                    if isinstance(cat_item_api, dict):
                        cat_id_str = str(cat_item_api.get("id", "")).strip()
                        if cat_id_str and cat_id_str.isdigit():
                            dados_para_inserir.append((id_produto, int(cat_id_str)))
                        else:
                            logger.warning(f"ID de categoria inválido '{cat_id_str}' para produto {id_produto}. Item da API: {cat_item_api}")
                
                if dados_para_inserir:
                    query_insert = "INSERT INTO produto_categorias (id_produto, id_categoria) VALUES %s ON CONFLICT DO NOTHING"
                    execute_values(cur, query_insert, dados_para_inserir)
                    logger.debug(f"{len(dados_para_inserir)} relacionamentos de categoria salvos para o produto ID {id_produto}.")
            else:
                logger.debug(f"Nenhuma lista de categorias fornecida ou lista vazia para o produto ID {id_produto}.")
    except (ValueError, psycopg2.DatabaseError, Exception) as error:
        logger.error(f"Erro ao salvar categorias para o produto ID {id_produto}: {error}", exc_info=True)
        raise

def salvar_produto_estoque_total_db(conn, id_produto, nome_produto, saldo_total_api, saldo_reservado_api, data_ultima_atualizacao_estoque=None):
    """Salva o estoque total de um produto."""
    # ... (código como na versão anterior, com conversão de data_ultima_atualizacao_estoque) ...
    id_produto_int = int(id_produto)
    try:
        saldo_total = safe_float_convert(saldo_total_api)
        saldo_reservado = safe_float_convert(saldo_reservado_api)
        
        data_ultima_att_db = None
        if data_ultima_atualizacao_estoque is None:
            data_ultima_att_db = datetime.datetime.now(datetime.timezone.utc)
        elif isinstance(data_ultima_atualizacao_estoque, str): 
            try:
                dt_obj = datetime.datetime.strptime(data_ultima_atualizacao_estoque, "%d/%m/%Y %H:%M:%S")
                data_ultima_att_db = dt_obj.replace(tzinfo=datetime.timezone.utc) # Adiciona UTC
            except ValueError:
                logger.warning(f"Formato de data_ultima_atualizacao_estoque ('{data_ultima_atualizacao_estoque}') inválido para produto {id_produto_int}. Usando data/hora atual.")
                data_ultima_att_db = datetime.datetime.now(datetime.timezone.utc)
        elif isinstance(data_ultima_atualizacao_estoque, datetime.datetime):
            data_ultima_att_db = data_ultima_atualizacao_estoque
            if data_ultima_att_db.tzinfo is None: 
                 data_ultima_att_db = data_ultima_att_db.replace(tzinfo=datetime.timezone.utc)
        else:
            logger.warning(f"Tipo de data_ultima_atualizacao_estoque inesperado ('{type(data_ultima_atualizacao_estoque)}'). Usando data/hora atual.")
            data_ultima_att_db = datetime.datetime.now(datetime.timezone.utc)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO produto_estoque_total (id_produto, nome_produto_estoque, saldo_total_api, saldo_reservado_api, data_ultima_atualizacao_api)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id_produto) DO UPDATE SET
                    nome_produto_estoque = EXCLUDED.nome_produto_estoque,
                    saldo_total_api = EXCLUDED.saldo_total_api,
                    saldo_reservado_api = EXCLUDED.saldo_reservado_api,
                    data_ultima_atualizacao_api = EXCLUDED.data_ultima_atualizacao_api;
            """, (id_produto_int, nome_produto, saldo_total, saldo_reservado, data_ultima_att_db))
        logger.debug(f"Estoque total para produto ID {id_produto_int} salvo.")
    except (ValueError, psycopg2.DatabaseError, Exception) as error:
        logger.error(f"Erro ao salvar estoque total para Produto ID {id_produto}: {error}", exc_info=True)
        raise

def salvar_estoque_por_deposito_db(conn, id_produto, nome_produto, lista_depositos_api):
    """Salva o estoque por depósito de um produto."""
    # ... (código como na versão anterior) ...
    id_produto_int = int(id_produto)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_estoque_depositos WHERE id_produto = %s", (id_produto_int,))
        
            if not lista_depositos_api or not isinstance(lista_depositos_api, list):
                logger.debug(f"Nenhuma lista de depósitos para produto ID {id_produto_int}.")
                return 
            
            num_depositos_salvos = 0
            for dep_wrapper in lista_depositos_api:
                dep_data = dep_wrapper.get("deposito") 
                if not dep_data and isinstance(dep_wrapper, dict): 
                    dep_data = dep_wrapper

                if dep_data and isinstance(dep_data, dict):
                    nome_dep = dep_data.get("nome")
                    saldo_dep = safe_float_convert(dep_data.get("saldo"))
                    desconsiderar = dep_data.get("desconsiderar")
                    empresa_dep = dep_data.get("empresa")

                    with conn.cursor() as cur_dep:
                        cur_dep.execute("""
                            INSERT INTO produto_estoque_depositos 
                            (id_produto, nome_deposito, saldo_deposito, desconsiderar_deposito, empresa_deposito) 
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id_produto, nome_deposito) DO UPDATE SET 
                                saldo_deposito = EXCLUDED.saldo_deposito,
                                desconsiderar_deposito = EXCLUDED.desconsiderar_deposito,
                                empresa_deposito = EXCLUDED.empresa_deposito;
                        """, (id_produto_int, nome_dep, saldo_dep, desconsiderar, empresa_dep))
                    num_depositos_salvos +=1
                else:
                    logger.warning(f"Dados de depósito malformados para produto ID {id_produto_int}: {dep_wrapper}")
            logger.debug(f"{num_depositos_salvos} depósitos salvos para produto ID {id_produto_int}.")

    except (ValueError, psycopg2.DatabaseError, Exception) as error:
        logger.error(f"Erro na operação de estoque por depósito para produto ID {id_produto}: {error}", exc_info=True)
        raise

def salvar_pedido_db(conn, pedido_api_data):
    """Salva os dados de um pedido no banco."""
    # ... (código como na versão anterior) ...
    id_pedido_str = str(pedido_api_data.get("id", "")).strip()
    if not id_pedido_str or not id_pedido_str.isdigit():
        logger.error(f"ID do pedido inválido ou ausente: '{id_pedido_str}'. Dados: {pedido_api_data}")
        raise ValueError(f"ID do pedido inválido: {id_pedido_str}")
    id_pedido = int(id_pedido_str)

    try:
        valor = safe_float_convert(pedido_api_data.get("valor"))
        id_vendedor_str = str(pedido_api_data.get("id_vendedor", "")).strip()
        id_vendedor_db = int(id_vendedor_str) if id_vendedor_str and id_vendedor_str.isdigit() else None
        nome_cliente = pedido_api_data.get("nome")

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
                id_pedido, pedido_api_data.get("numero"), pedido_api_data.get("numero_ecommerce"),
                pedido_api_data.get("data_pedido"), pedido_api_data.get("data_prevista"), nome_cliente,
                valor, id_vendedor_db, pedido_api_data.get("nome_vendedor"),
                pedido_api_data.get("situacao"), pedido_api_data.get("codigo_rastreamento")
            ))
        logger.debug(f"Pedido ID {id_pedido} salvo/atualizado.")
    except (ValueError, psycopg2.DatabaseError, Exception) as error: 
        logger.error(f"Erro ao processar/salvar pedido ID '{id_pedido_str}': {error}", exc_info=True)
        raise

def salvar_pedido_itens_db(conn, id_pedido_api, itens_lista_api):
    """Salva os itens de um pedido no banco usando execute_values."""
    # ... (código como na versão anterior, usando execute_values) ...
    id_pedido_int = int(id_pedido_api)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pedido_itens WHERE id_pedido = %s", (id_pedido_int,))
            if not itens_lista_api or not isinstance(itens_lista_api, list):
                logger.debug(f"Nenhuma lista de itens para pedido ID {id_pedido_int}.")
                return
            
            dados_para_inserir_itens = []
            for item_dict_wrapper in itens_lista_api: 
                item_data = item_dict_wrapper.get("item")
                if not item_data or not isinstance(item_data, dict):
                    logger.warning(f"Item de pedido malformado para pedido ID {id_pedido_int}: {item_dict_wrapper}")
                    continue
                
                id_produto_tiny_str = str(item_data.get("id_produto", "")).strip()
                id_produto_tiny = int(id_produto_tiny_str) if id_produto_tiny_str and id_produto_tiny_str.isdigit() else None
                quantidade = safe_float_convert(item_data.get("quantidade"))
                valor_unitario = safe_float_convert(item_data.get("valor_unitario"))
                
                dados_para_inserir_itens.append((
                    id_pedido_int, id_produto_tiny, item_data.get("codigo"), item_data.get("descricao"),
                    quantidade, item_data.get("unidade"), valor_unitario, item_data.get("id_grade")
                ))
            
            if dados_para_inserir_itens:
                query_insert_itens = """
                    INSERT INTO pedido_itens (
                        id_pedido, id_produto_tiny, codigo_produto_pedido, descricao_produto_pedido,
                        quantidade, unidade_pedido, valor_unitario_pedido, id_grade_pedido
                    ) VALUES %s
                """ # ON CONFLICT não é necessário aqui pois estamos deletando antes.
                execute_values(cur, query_insert_itens, dados_para_inserir_itens)
                logger.debug(f"{len(dados_para_inserir_itens)} itens salvos para pedido ID {id_pedido_int}.")

    except (ValueError, psycopg2.DatabaseError, Exception) as error:
        logger.error(f"Erro na operação de itens do pedido para Pedido ID {id_pedido_api}: {error}", exc_info=True)
        raise 

# --- Funções da API ---
def make_api_v2_request(endpoint_path, method="GET", payload_dict=None, 
                        max_retries=3, initial_retry_delay=2, timeout_seconds=DEFAULT_API_TIMEOUT): # Adicionado timeout_seconds
    """
    Realiza uma requisição para a API v2 do Tiny ERP.
    """
    # ... (código como na versão anterior, mas usando timeout_seconds e tratando HTTP 429 com delay maior) ...
    full_url = f"{BASE_URL_V2}{endpoint_path}"
    base_params = {"token": API_V2_TOKEN, "formato": "json"}
    all_params = base_params.copy()
    if payload_dict: 
        all_params.update(payload_dict)

    params_log = {k: (v[:5] + '...' if k == 'token' and isinstance(v, str) and v else v) for k, v in all_params.items()}
    
    retries_attempted = 0
    current_delay = initial_retry_delay
    
    while retries_attempted <= max_retries:
        response = None 
        try:
            if retries_attempted > 0: 
                actual_delay_this_retry = current_delay
                # Tratamento específico para 429 pode ter ajustado current_delay para ser maior já na primeira retry
                # (removido ajuste de current_delay para 429 daqui, será tratado no except HTTPError)
                logger.info(f"Aguardando {actual_delay_this_retry}s antes da tentativa {retries_attempted + 1}/{max_retries + 1} para {endpoint_path}...")
                time.sleep(actual_delay_this_retry)
                current_delay = min(current_delay * 2, 30) # Aumenta para a *próxima* tentativa, se houver

            logger.debug(f"Tentativa {retries_attempted + 1}/{max_retries + 1} - {method} {full_url} - Params: {params_log}")
            
            if method.upper() == "GET":
                response = requests.get(full_url, params=all_params, timeout=timeout_seconds)
            elif method.upper() == "POST":
                response = requests.post(full_url, data=all_params, timeout=timeout_seconds)
            else:
                logger.error(f"Método {method} não suportado.")
                return None, False 

            logger.debug(f"Resposta de {endpoint_path}: Status {response.status_code}, Conteúdo (parcial): {response.text[:200]}")
            response.raise_for_status()
            
            try:
                response_data = response.json()
            except json.JSONDecodeError as e_json_inner:
                logger.error(f"Erro ao decodificar JSON (interno) da resposta de {endpoint_path}: {e_json_inner}", exc_info=True)
                logger.debug(f"Corpo da Resposta (não JSON) com erro interno: {response.text[:500]}")
                return None, False

            retorno_geral = response_data.get("retorno")
            
            if not retorno_geral:
                logger.error(f"Chave 'retorno' ausente na resposta JSON de {endpoint_path}. Resposta: {str(response_data)[:500]}")
                return None, False 

            if endpoint_path == ENDPOINT_CATEGORIAS:
                # ... (lógica de categorias como antes) ...
                if isinstance(retorno_geral, list): return retorno_geral, True 
                if isinstance(retorno_geral, dict):
                    if retorno_geral.get("status") == "OK" and "categorias" in retorno_geral and isinstance(retorno_geral["categorias"], list):
                        return retorno_geral["categorias"], True
                    if retorno_geral.get("status") != "OK":
                        erros = retorno_geral.get("erros", [])
                        logger.error(f"API Tiny (Categorias): Status '{retorno_geral.get('status')}', Erros: {erros}")
                        return None, False 
                logger.error(f"Resposta inesperada para Categorias: {type(retorno_geral)}. Conteúdo: {str(retorno_geral)[:300]}")
                return None, False 

            if not isinstance(retorno_geral, dict):
                logger.error(f"'retorno' não é um dicionário para {endpoint_path}. Conteúdo: {str(retorno_geral)[:300]}")
                return None, False 

            status_api = retorno_geral.get("status")
            status_processamento = str(retorno_geral.get("status_processamento", ""))
            
            if status_api != "OK":
                # ... (lógica de erro da API como antes) ...
                erros_api = retorno_geral.get("erros") 
                codigo_erro_interno = ""
                msg_erro_interno = ""
                if isinstance(erros_api, list) and len(erros_api) > 0:
                    if isinstance(erros_api[0], dict):
                        erro_obj = erros_api[0].get("erro")
                        if isinstance(erro_obj, dict): 
                            codigo_erro_interno = erro_obj.get("codigo", "")
                            msg_erro_interno = erro_obj.get("erro", "")
                        elif isinstance(erro_obj, str): 
                            msg_erro_interno = erro_obj
                    elif isinstance(erros_api[0], str) :
                        msg_erro_interno = erros_api[0]

                logger.error(f"API Tiny: Status '{status_api}' (Endpoint: {endpoint_path}). Código: {codigo_erro_interno}. Mensagem: {msg_erro_interno}. Resposta: {str(retorno_geral)[:500]}")
                if codigo_erro_interno == "2": 
                    logger.critical("Token da API Tiny inválido ou expirado. Verifique a variável de ambiente.")
                return None, False

            if status_processamento not in ["3", "10"]: 
                # ... (lógica de status de processamento como antes) ...
                msg_erro_proc = ""
                erros_retorno = retorno_geral.get("erros")
                if isinstance(erros_retorno, list) and erros_retorno:
                    if isinstance(erros_retorno[0], dict) and "erro" in erros_retorno[0]:
                        erro_detalhe = erros_retorno[0]["erro"]
                        msg_erro_proc = str(erro_detalhe.get("erro", erro_detalhe) if isinstance(erro_detalhe, dict) else erro_detalhe)
                    elif isinstance(erros_retorno[0], str):
                         msg_erro_proc = erros_retorno[0]
                
                if "Nenhum registro encontrado" in msg_erro_proc: 
                    logger.info(f"Nenhum registro encontrado para os critérios em {endpoint_path} (Status Proc: {status_processamento}).")
                    return retorno_geral, True

                logger.warning(f"API: Status de processamento é '{status_processamento}' (Endpoint: {endpoint_path}). Mensagem: '{msg_erro_proc}'. Resposta: {str(retorno_geral)[:300]}")
                if status_processamento == "2":
                     return None, False 

            return retorno_geral, True 

        except requests.exceptions.HTTPError as e_http:
            logger.warning(f"Erro HTTP (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {e_http}", exc_info=False)
            if response is not None: 
                logger.debug(f"Corpo da Resposta HTTP: {response.text[:500]}")
                if response.status_code == 429: # Too Many Requests
                    logger.warning(f"Limite de taxa (429) atingido para {endpoint_path}. Próxima tentativa com delay maior ({RETRY_DELAY_429}s).")
                    current_delay = RETRY_DELAY_429 # Força um delay maior para a próxima tentativa
                elif 400 <= response.status_code < 500: # Outros erros 4xx não são retentáveis
                    return None, False 
            # Para 5xx ou 429 (com delay ajustado), permite retentativa
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e_net:
            logger.warning(f"Erro de Rede/Timeout (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_net).__name__} - {e_net}", exc_info=False)
        except requests.exceptions.RequestException as e_req: 
            logger.warning(f"Erro de Requisição (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_req).__name__} - {e_req}", exc_info=False)
        except json.JSONDecodeError as e_json:
            logger.error(f"Erro ao decodificar JSON da resposta de {endpoint_path} (Tentativa {retries_attempted + 1}/{max_retries + 1}): {e_json}", exc_info=True)
            if response is not None: logger.debug(f"Corpo da Resposta (não JSON): {response.text[:500]}")
            return None, False 
        except Exception as e_geral: 
            logger.error(f"Erro INESPERADO em make_api_v2_request (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {e_geral}", exc_info=True)
            if response is not None: logger.debug(f"Corpo da Resposta: {response.text[:500]}")
            return None, False 

        retries_attempted += 1
        if retries_attempted > max_retries:
            logger.error(f"Máximo de {max_retries + 1} tentativas foi atingido para {endpoint_path}. Desistindo.")
            return None, False
    return None, False

# --- Funções de Lógica de Negócio (Busca e Salvamento) ---

def get_categorias_v2(conn):
    """Busca todas as categorias da API e as salva no banco."""
    # ... (código como na versão anterior) ...
    logger.info("Iniciando busca e salvamento de Categorias.")
    lista_categorias_api, sucesso_api = make_api_v2_request(ENDPOINT_CATEGORIAS)
    
    if sucesso_api and isinstance(lista_categorias_api, list): 
        if not lista_categorias_api:
            logger.info("Nenhuma categoria retornada pela API.")
            set_ultima_execucao(conn, PROCESSO_CATEGORIAS)
            return True 
        
        num_categorias_raiz_processadas = 0
        all_categories_processed_successfully = True
        for cat_raiz_dict in lista_categorias_api:
            if isinstance(cat_raiz_dict, dict): 
                try:
                    salvar_categoria_db(conn, cat_raiz_dict, id_pai=None)
                    num_categorias_raiz_processadas +=1
                except Exception as e_cat_save:
                    logger.error(f"Erro ao salvar categoria raiz ID '{cat_raiz_dict.get('id')}' ou suas filhas: {e_cat_save}", exc_info=True)
                    all_categories_processed_successfully = False 
            else:
                 logger.warning(f"Item inesperado na lista de categorias raiz (não é dict): {cat_raiz_dict}")
        
        if all_categories_processed_successfully:
            try:
                if conn and not conn.closed: conn.commit() 
                logger.info(f"Categorias processadas: {num_categorias_raiz_processadas} categorias raiz e suas filhas foram commitadas.")
                set_ultima_execucao(conn, PROCESSO_CATEGORIAS) 
                return True
            except Exception as e_commit_cat:
                logger.error(f"Erro ao commitar categorias: {e_commit_cat}", exc_info=True)
                if conn and not conn.closed: conn.rollback()
                return False
        else:
            logger.warning("Processamento de categorias concluído com erros. Fazendo rollback para categorias.")
            if conn and not conn.closed: conn.rollback()
            return False
            
    logger.error(f"Não foi possível buscar ou salvar as categorias. Sucesso API: {sucesso_api}")
    if lista_categorias_api is not None: logger.debug(f"Dados de categorias recebidos (parcial): {str(lista_categorias_api)[:300]}")
    return False

def get_produto_detalhes_v2(id_produto_tiny):
    """Busca os detalhes de um produto específico, incluindo suas categorias."""
    # ... (código como na versão anterior) ...
    logger.debug(f"Buscando detalhes para o produto ID {id_produto_tiny}...")
    params_api = {"id": id_produto_tiny}
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PRODUTO_OBTER, payload_dict=params_api)
    if sucesso and retorno_obj:
        produto_detalhe_api = retorno_obj.get("produto") 
        if produto_detalhe_api:
            return produto_detalhe_api
        else:
            logger.warning(f"Produto ID {id_produto_tiny} não encontrado ou sem dados de detalhe na API.")
    return None

def search_produtos_v2(conn, data_alteracao_inicial=None, pagina=1):
    """
    Busca produtos alterados na API (dados cadastrais e categorias) e os salva.
    Retorna: (lista_de_produtos_api, num_total_paginas_api, sucesso_pagina_db)
    """
    # ... (código como na versão anterior, com verificação de conn.closed) ...
    logger.info(f"Buscando página {pagina} de produtos alterados desde {data_alteracao_inicial or 'inicio'}.")
    params_pesquisa = {"pagina": pagina}
    if data_alteracao_inicial:
        params_pesquisa["dataAlteracaoInicial"] = data_alteracao_inicial
        
    retorno_api, sucesso_api = make_api_v2_request(ENDPOINT_PRODUTOS_PESQUISA, payload_dict=params_pesquisa)
    
    produtos_desta_pagina_api = []
    num_paginas_total_api = 0
    pagina_processada_com_sucesso_db = False

    if sucesso_api and retorno_api:
        produtos_desta_pagina_api = retorno_api.get("produtos", [])
        num_paginas_total_api = int(retorno_api.get('numero_paginas', 0))
    elif not sucesso_api:
        logger.error(f"Falha ao buscar produtos da API para a página {pagina}.")
        return None, 0, False

    if produtos_desta_pagina_api and isinstance(produtos_desta_pagina_api, list):
        all_items_in_page_processed_successfully = True 
        produtos_salvos_nesta_pagina = 0

        for produto_wrapper_api in produtos_desta_pagina_api:
            produto_data_api = produto_wrapper_api.get("produto")
            if not produto_data_api or not isinstance(produto_data_api, dict):
                logger.warning(f"Item de produto malformado na API (pág {pagina}): {produto_wrapper_api}")
                continue

            id_produto_str = str(produto_data_api.get("id","")).strip() # Usado para logging em caso de erro no int()
            try:
                id_produto_int = int(id_produto_str) # Validação primária do ID
                salvar_produto_db(conn, produto_data_api)
                
                time.sleep(0.5) 
                detalhes_produto_api = get_produto_detalhes_v2(id_produto_int)
                if detalhes_produto_api and "categorias" in detalhes_produto_api:
                    salvar_produto_categorias_db(conn, id_produto_int, detalhes_produto_api["categorias"])
                
                produtos_salvos_nesta_pagina += 1
            except (ValueError, psycopg2.DatabaseError, Exception) as e_item: 
                logger.error(f"Erro no processamento do produto ID '{id_produto_str}' (página {pagina}): {e_item}", exc_info=True)
                all_items_in_page_processed_successfully = False
                break 
        
        if all_items_in_page_processed_successfully and produtos_salvos_nesta_pagina > 0:
            try:
                if conn and not conn.closed: conn.commit()
                logger.info(f"Página {pagina} de produtos ({produtos_salvos_nesta_pagina} itens) commitada com sucesso.")
                pagina_processada_com_sucesso_db = True
            except Exception as e_commit:
                logger.error(f"Erro CRÍTICO ao commitar página {pagina} de produtos: {e_commit}", exc_info=True)
                if conn and not conn.closed: conn.rollback()
                pagina_processada_com_sucesso_db = False 
        elif not all_items_in_page_processed_successfully:
            logger.warning(f"Página {pagina} de produtos contéve erros. Fazendo ROLLBACK para a página inteira.")
            if conn and not conn.closed: conn.rollback()
            pagina_processada_com_sucesso_db = False
        
        return produtos_desta_pagina_api, num_paginas_total_api, pagina_processada_com_sucesso_db
    
    logger.info(f"Nenhum produto retornado pela API para a página {pagina} ou estrutura de resposta inválida.")
    return [], num_paginas_total_api, True


def processar_atualizacoes_estoque_v2(conn, data_alteracao_estoque_inicial=None, pagina=1):
    """
    Busca atualizações de estoque da API e salva.
    Retorna: (lista_produtos_com_estoque, num_total_paginas, sucesso_pagina_db)
    """
    # ... (código como na versão anterior, com conversão de data e verificação de conn.closed) ...
    logger.info(f"Buscando página {pagina} de atualizações de estoque desde {data_alteracao_estoque_inicial or 'inicio'}.")
    params_pesquisa = {"pagina": pagina}
    if data_alteracao_estoque_inicial:
        params_pesquisa["dataAlteracao"] = data_alteracao_estoque_inicial
        
    retorno_api, sucesso_api = make_api_v2_request(ENDPOINT_LISTA_ATUALIZACOES_ESTOQUE, payload_dict=params_pesquisa)
    
    produtos_com_estoque_api = []
    num_paginas_total_api = 0
    pagina_processada_com_sucesso_db = False

    if sucesso_api and retorno_api:
        produtos_com_estoque_api = retorno_api.get("produtos", [])
        num_paginas_total_api = int(retorno_api.get('numero_paginas', 0))
    elif not sucesso_api:
        logger.error(f"Falha ao buscar atualizações de estoque da API para a página {pagina}.")
        return None, 0, False

    if produtos_com_estoque_api and isinstance(produtos_com_estoque_api, list):
        all_items_processed_successfully = True
        estoques_salvos_nesta_pagina = 0

        for produto_wrapper_api in produtos_com_estoque_api:
            produto_estoque_data = produto_wrapper_api.get("produto")
            if not produto_estoque_data or not isinstance(produto_estoque_data, dict):
                logger.warning(f"Item de estoque malformado na API (pág {pagina}): {produto_wrapper_api}")
                continue
            
            id_produto_str = str(produto_estoque_data.get("id","")).strip()
            try:
                if not id_produto_str or not id_produto_str.isdigit():
                    logger.warning(f"ID do produto inválido em atualização de estoque: '{id_produto_str}'. Dados: {produto_estoque_data}")
                    continue
                id_produto_int = int(id_produto_str)

                nome_produto = produto_estoque_data.get("nome", f"Produto ID {id_produto_int}")
                saldo_total = produto_estoque_data.get("saldo")
                saldo_reservado = produto_estoque_data.get("saldoReservado")
                data_att_estoque_str = produto_estoque_data.get("data_alteracao")
                
                salvar_produto_estoque_total_db(conn, id_produto_int, nome_produto, saldo_total, saldo_reservado, data_att_estoque_str) # Passa a string da data
                
                lista_depositos_api = produto_estoque_data.get("depositos")
                if lista_depositos_api:
                    salvar_estoque_por_deposito_db(conn, id_produto_int, nome_produto, lista_depositos_api)
                else: 
                    salvar_estoque_por_deposito_db(conn, id_produto_int, nome_produto, [])

                estoques_salvos_nesta_pagina += 1
            except (ValueError, psycopg2.DatabaseError, Exception) as e_item:
                logger.error(f"Erro no processamento do estoque para produto ID {id_produto_str} (pág {pagina}): {e_item}", exc_info=True)
                all_items_processed_successfully = False
                break 
        
        if all_items_processed_successfully and estoques_salvos_nesta_pagina > 0:
            try:
                if conn and not conn.closed: conn.commit()
                logger.info(f"Página {pagina} de atualizações de estoque ({estoques_salvos_nesta_pagina} itens) commitada com sucesso.")
                pagina_processada_com_sucesso_db = True
            except Exception as e_commit:
                logger.error(f"Erro CRÍTICO ao commitar página {pagina} de estoques: {e_commit}", exc_info=True)
                if conn and not conn.closed: conn.rollback()
                pagina_processada_com_sucesso_db = False
        elif not all_items_processed_successfully:
            logger.warning(f"Página {pagina} de estoques contéve erros. Fazendo ROLLBACK para a página inteira.")
            if conn and not conn.closed: conn.rollback()
            pagina_processada_com_sucesso_db = False
            
        return produtos_com_estoque_api, num_paginas_total_api, pagina_processada_com_sucesso_db

    logger.info(f"Nenhuma atualização de estoque retornada pela API para a página {pagina} ou estrutura inválida.")
    return [], num_paginas_total_api, True


def get_detalhes_pedido_v2(id_pedido_api):
    """Busca os detalhes de um pedido específico, incluindo seus itens."""
    # ... (código como na versão anterior) ...
    logger.debug(f"Buscando detalhes para o pedido ID {id_pedido_api}...")
    params_api = {"id": id_pedido_api} 
    retorno_obj, sucesso = make_api_v2_request(ENDPOINT_PEDIDO_OBTER, payload_dict=params_api)
    if sucesso and retorno_obj:
        pedido_detalhe_api = retorno_obj.get("pedido")
        if pedido_detalhe_api:
            return pedido_detalhe_api 
        else:
            logger.warning(f"Pedido ID {id_pedido_api} não encontrado ou sem dados de detalhe na API.")
    return None

def search_pedidos_v2(conn, data_alteracao_ou_inicial=None, pagina=1):
    """
    Busca pedidos alterados na API e os salva, incluindo seus itens.
    Retorna: (lista_de_pedidos_api, num_total_paginas_api, sucesso_pagina_db)
    """
    # ... (código como na versão anterior, com verificação de conn.closed) ...
    logger.info(f"Buscando página {pagina} de pedidos alterados desde {data_alteracao_ou_inicial or 'inicio'}.")
    params_pesquisa = {"pagina": pagina} 
    if data_alteracao_ou_inicial:
        params_pesquisa["dataAlteracaoInicial"] = data_alteracao_ou_inicial
        
    retorno_api, sucesso_api = make_api_v2_request(ENDPOINT_PEDIDOS_PESQUISA, payload_dict=params_pesquisa)

    pedidos_desta_pagina_api = []
    num_paginas_total_api = 0
    pagina_processada_com_sucesso_db = False

    if sucesso_api and retorno_api:
        pedidos_desta_pagina_api = retorno_api.get("pedidos", [])
        num_paginas_total_api = int(retorno_api.get('numero_paginas', 0))
    elif not sucesso_api:
        logger.error(f"Falha ao buscar pedidos da API para a página {pagina}.")
        return None, 0, False

    if pedidos_desta_pagina_api and isinstance(pedidos_desta_pagina_api, list):
        all_items_processed_successfully = True
        pedidos_salvos_nesta_pagina = 0

        for pedido_wrapper_api in pedidos_desta_pagina_api:
            pedido_data_api = pedido_wrapper_api.get("pedido")
            if not pedido_data_api or not isinstance(pedido_data_api, dict):
                logger.warning(f"Item de pedido malformado na API (pág {pagina}): {pedido_wrapper_api}")
                continue

            id_pedido_str = str(pedido_data_api.get("id","")).strip() # Usado para logging em caso de erro no int()
            try:
                id_pedido_int = int(id_pedido_str) # Validação primária do ID
                salvar_pedido_db(conn, pedido_data_api)
                
                time.sleep(0.6) 
                detalhes_pedido_api = get_detalhes_pedido_v2(id_pedido_int)
                if detalhes_pedido_api and "itens" in detalhes_pedido_api:
                    lista_de_itens_bruta_api = detalhes_pedido_api["itens"]
                    salvar_pedido_itens_db(conn, id_pedido_int, lista_de_itens_bruta_api)
                
                pedidos_salvos_nesta_pagina +=1
            except (ValueError, psycopg2.DatabaseError, Exception) as e_item: 
                logger.error(f"Erro no processamento do pedido ID '{id_pedido_str}' (página {pagina}): {e_item}", exc_info=True)
                all_items_processed_successfully = False
                break 
        
        if all_items_processed_successfully and pedidos_salvos_nesta_pagina > 0:
            try:
                if conn and not conn.closed: conn.commit() 
                logger.info(f"Página {pagina} de pedidos ({pedidos_salvos_nesta_pagina} itens) commitada com sucesso.")
                pagina_processada_com_sucesso_db = True
            except Exception as e_commit:
                logger.error(f"Erro CRÍTICO ao commitar página {pagina} de pedidos: {e_commit}", exc_info=True)
                if conn and not conn.closed: conn.rollback()
                pagina_processada_com_sucesso_db = False
        elif not all_items_processed_successfully:
            logger.warning(f"Página {pagina} de pedidos contéve erros. Fazendo ROLLBACK para a página inteira.")
            if conn and not conn.closed: conn.rollback()
            pagina_processada_com_sucesso_db = False
            
        return pedidos_desta_pagina_api, num_paginas_total_api, pagina_processada_com_sucesso_db

    logger.info(f"Nenhum pedido retornado pela API para a página {pagina} ou estrutura de resposta inválida.")
    return [], num_paginas_total_api, True


# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    logger.info("=== Iniciando Cliente para API v2 do Tiny ERP (PostgreSQL, Incremental) ===")
    start_time_total = time.time()

    # ... (código como na versão anterior, com verificações de conn.closed) ...
    if not all([API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        logger.critical("Variáveis de ambiente para API ou Banco de Dados não configuradas. Encerrando.")
        logger.critical("Configure: TINY_API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT.")
        exit(1)

    db_conn = get_db_connection()
    if db_conn is None or db_conn.closed: # Verifica se a conexão foi bem sucedida e está aberta
        logger.critical("Não foi possível conectar ao banco de dados PostgreSQL ou conexão fechada. Encerrando.")
        exit(1)
    
    try:
        criar_tabelas_db(db_conn) 

        # PASSO 1: Categorias
        logger.info("--- INICIANDO PASSO 1: Processamento de Categorias ---")
        if get_categorias_v2(db_conn):
            logger.info("Passo 1 (Categorias) concluído.")
        else:
            logger.warning("Passo 1 (Categorias) concluído com falhas ou sem dados.")
        logger.info("-" * 70)

        # PASSO 2: Produtos Cadastrais e Categorias de Produtos
        logger.info("--- INICIANDO PASSO 2: Processamento de Produtos (Cadastrais e Categorias) ---")
        ultima_exec_produtos_str = get_ultima_execucao(db_conn, PROCESSO_PRODUTOS)
        data_filtro_produtos = ultima_exec_produtos_str if ultima_exec_produtos_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        produtos_cad_total_listados_api = 0
        pagina_atual_prod = 1
        timestamp_inicio_proc_prod = datetime.datetime.now(datetime.timezone.utc) 
        etapa_produtos_ok = True
        
        logger.info(f"Iniciando busca de produtos (cadastrais) alterados desde: {data_filtro_produtos}.")
        while True: 
            logger.info(f"Processando página {pagina_atual_prod} de produtos (cadastrais)...")
            produtos_api_pagina, total_paginas_api, pagina_commitada = search_produtos_v2(db_conn, 
                                                                    data_alteracao_inicial=data_filtro_produtos, 
                                                                    pagina=pagina_atual_prod)
            if produtos_api_pagina is None: 
                logger.error(f"Falha crítica ao buscar produtos (cadastrais) na API para a página {pagina_atual_prod}. Interrompendo produtos.")
                etapa_produtos_ok = False
                break 
            if not pagina_commitada and (produtos_api_pagina and len(produtos_api_pagina) > 0):
                logger.warning(f"Página {pagina_atual_prod} de produtos (cadastrais) não foi commitada devido a erros. Interrompendo etapa.")
                etapa_produtos_ok = False
                break
            
            if produtos_api_pagina: 
                produtos_cad_total_listados_api += len(produtos_api_pagina) 
            
            if total_paginas_api == 0 or pagina_atual_prod >= total_paginas_api :
                logger.info("Todas as páginas de produtos (cadastrais) disponíveis foram processadas ou API indicou 0 páginas.")
                break
            pagina_atual_prod += 1
            
            if pagina_atual_prod <= total_paginas_api: 
                logger.info("Pausa de 1 segundo antes da próxima página de produtos (cadastrais)...")
                time.sleep(1) 
        
        if etapa_produtos_ok:
            set_ultima_execucao(db_conn, PROCESSO_PRODUTOS, timestamp_inicio_proc_prod) 
            logger.info(f"Passo 2 (Produtos Cadastrais) concluído. {produtos_cad_total_listados_api} produtos listados pela API. Timestamp atualizado.")
        else:
            logger.warning("Passo 2 (Produtos Cadastrais) interrompido ou finalizado com erros em alguma página. Timestamp NÃO atualizado.")
        logger.info("-" * 70)

        # PASSO 3: Atualizações de Estoque
        logger.info("--- INICIANDO PASSO 3: Processamento de Atualizações de Estoque ---")
        ultima_exec_estoques_str = get_ultima_execucao(db_conn, PROCESSO_ESTOQUES)
        data_filtro_estoques = ultima_exec_estoques_str if ultima_exec_estoques_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL

        estoques_total_listados_api = 0
        pagina_atual_est = 1
        timestamp_inicio_proc_est = datetime.datetime.now(datetime.timezone.utc)
        etapa_estoques_ok = True

        logger.info(f"Iniciando busca de atualizações de estoque alteradas desde: {data_filtro_estoques}.")
        while True:
            logger.info(f"Processando página {pagina_atual_est} de atualizações de estoque...")
            estoques_api_pagina, total_paginas_api_est, pagina_commitada_est = processar_atualizacoes_estoque_v2(db_conn,
                                                                                         data_alteracao_estoque_inicial=data_filtro_estoques,
                                                                                         pagina=pagina_atual_est)
            if estoques_api_pagina is None:
                logger.error(f"Falha crítica ao buscar atualizações de estoque na API para a página {pagina_atual_est}. Interrompendo estoques.")
                etapa_estoques_ok = False
                break
            if not pagina_commitada_est and (estoques_api_pagina and len(estoques_api_pagina) > 0):
                logger.warning(f"Página {pagina_atual_est} de estoques não foi commitada devido a erros. Interrompendo etapa.")
                etapa_estoques_ok = False
                break
            
            if estoques_api_pagina:
                estoques_total_listados_api += len(estoques_api_pagina)

            if total_paginas_api_est == 0 or pagina_atual_est >= total_paginas_api_est:
                logger.info("Todas as páginas de atualizações de estoque disponíveis foram processadas ou API indicou 0 páginas.")
                break
            pagina_atual_est += 1

            if pagina_atual_est <= total_paginas_api_est:
                logger.info("Pausa de 1 segundo antes da próxima página de atualizações de estoque...")
                time.sleep(1)
        
        if etapa_estoques_ok:
            set_ultima_execucao(db_conn, PROCESSO_ESTOQUES, timestamp_inicio_proc_est)
            logger.info(f"Passo 3 (Estoques) concluído. {estoques_total_listados_api} atualizações de estoque listadas pela API. Timestamp atualizado.")
        else:
            logger.warning("Passo 3 (Estoques) interrompido ou finalizado com erros em alguma página. Timestamp NÃO atualizado.")
        logger.info("-" * 70)

        # PASSO 4: Pedidos e Itens de Pedidos
        logger.info("--- INICIANDO PASSO 4: Processamento de Pedidos e Itens ---")
        ultima_exec_pedidos_str = get_ultima_execucao(db_conn, PROCESSO_PEDIDOS)
        data_filtro_pedidos = ultima_exec_pedidos_str if ultima_exec_pedidos_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        pedidos_total_listados_api = 0
        pagina_atual_ped = 1
        timestamp_inicio_proc_ped = datetime.datetime.now(datetime.timezone.utc)
        etapa_pedidos_ok = True
        
        logger.info(f"Iniciando busca de pedidos alterados desde: {data_filtro_pedidos}.")
        while True: 
            logger.info(f"Processando página {pagina_atual_ped} de pedidos...")
            pedidos_api_pagina, total_paginas_api_ped, pagina_commitada_ped = search_pedidos_v2(db_conn, 
                                                                      data_alteracao_ou_inicial=data_filtro_pedidos, 
                                                                      pagina=pagina_atual_ped)
            if pedidos_api_pagina is None:
                logger.error(f"Falha crítica ao buscar pedidos na API para a página {pagina_atual_ped}. Interrompendo pedidos.")
                etapa_pedidos_ok = False
                break
            if not pagina_commitada_ped and (pedidos_api_pagina and len(pedidos_api_pagina) > 0):
                logger.warning(f"Página {pagina_atual_ped} de pedidos não foi commitada devido a erros. Interrompendo etapa.")
                etapa_pedidos_ok = False
                break

            if pedidos_api_pagina:
                pedidos_total_listados_api += len(pedidos_api_pagina)

            if total_paginas_api_ped == 0 or pagina_atual_ped >= total_paginas_api_ped:
                logger.info("Todas as páginas de pedidos disponíveis foram processadas ou API indicou 0 páginas.")
                break
            pagina_atual_ped += 1

            if pagina_atual_ped <= total_paginas_api_ped: 
                logger.info("Pausa de 1 segundo antes da próxima página de pedidos...")
                time.sleep(1) 
        
        if etapa_pedidos_ok:
            set_ultima_execucao(db_conn, PROCESSO_PEDIDOS, timestamp_inicio_proc_ped)
            logger.info(f"Passo 4 (Pedidos) concluído. {pedidos_total_listados_api} pedidos listados pela API. Timestamp atualizado.")
        else:
            logger.warning("Passo 4 (Pedidos) interrompido ou finalizado com erros em alguma página. Timestamp NÃO atualizado.")
        logger.info("-" * 70)

        logger.info("--- Contagem final dos registros no banco de dados ---")
        with db_conn.cursor() as cur:
            tabelas_para_contar = ["categorias", "produtos", "produto_categorias", 
                                   "produto_estoque_total", "produto_estoque_depositos", 
                                   "pedidos", "pedido_itens", "script_ultima_execucao"]
            for tabela in tabelas_para_contar:
                try:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tabela)))
                    count = cur.fetchone()[0]
                    logger.info(f"  - Tabela '{tabela}': {count} registros.")
                except (Exception, psycopg2.DatabaseError) as e_count:
                    logger.error(f"  Erro ao contar registros da tabela {tabela}: {e_count}", exc_info=True)
            
    except KeyboardInterrupt:
        logger.warning("Processo interrompido pelo usuário (KeyboardInterrupt).")
        if db_conn and not db_conn.closed:
            try: 
                logger.info("Tentando rollback da transação atual devido a KeyboardInterrupt...")
                db_conn.rollback()
            except Exception as e_roll_ki: 
                logger.error(f"Erro no rollback após KeyboardInterrupt: {e_roll_ki}", exc_info=True)
    except Exception as error_geral:
        logger.critical(f"ERRO GERAL NO PROCESSAMENTO PRINCIPAL: {error_geral}", exc_info=True)
        if db_conn and not db_conn.closed:
            try:
                logger.info("Tentando rollback da transação atual devido a erro geral...")
                db_conn.rollback() 
            except Exception as e_rollback:
                logger.error(f"Erro durante o rollback da transação geral: {e_rollback}", exc_info=True)
    finally:
        if db_conn and not db_conn.closed: # Verifica se db_conn não é None e se não está fechada
            db_conn.close()
            logger.info("Conexão com PostgreSQL fechada.")
        elif db_conn is None:
            logger.info("Nenhuma conexão com PostgreSQL para fechar (db_conn is None).")
        else: # db_conn existe mas está fechada
            logger.info("Conexão com PostgreSQL já estava fechada.")

    end_time_total = time.time()
    logger.info(f"=== Processo Concluído em {end_time_total - start_time_total:.2f} segundos ===")