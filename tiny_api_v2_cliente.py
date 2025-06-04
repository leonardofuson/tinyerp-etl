import requests
import json
import datetime
import time
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values 
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
PROCESSO_PRODUTOS = "produtos" 
PROCESSO_ESTOQUES = "estoques"
PROCESSO_PEDIDOS = "pedidos"

DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL = "01/10/2024 00:00:00"
DEFAULT_API_TIMEOUT = 60 
RETRY_DELAY_429 = 30 

# --- Função Auxiliar para Conversão ---
def safe_float_convert(value_str, default=0.0):
    """Converte uma string para float de forma segura, tratando None, ',', e strings vazias."""
    if value_str is None: return default
    value_str = str(value_str).strip().replace(',', '.')
    if not value_str: return default
    try: return float(value_str)
    except ValueError:
        logger.debug(f"Não foi possível converter '{value_str}' para float, usando {default}.")
        return default

# --- Funções de Banco de Dados (PostgreSQL) ---
def get_db_connection():
    """Estabelece e retorna uma conexão com o banco de dados PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER,
            password=DB_PASSWORD, port=DB_PORT, connect_timeout=10
        )
        logger.info("Conexão com PostgreSQL estabelecida com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao conectar ao PostgreSQL: {e}", exc_info=True)
    return conn

def criar_tabelas_db(conn):
    """Cria as tabelas no banco de dados se elas não existirem."""
    commands = (
        """CREATE TABLE IF NOT EXISTS categorias (id_categoria INTEGER PRIMARY KEY, descricao_categoria TEXT NOT NULL, id_categoria_pai INTEGER, FOREIGN KEY (id_categoria_pai) REFERENCES categorias (id_categoria) ON DELETE SET NULL);""",
        """CREATE TABLE IF NOT EXISTS produtos (id_produto INTEGER PRIMARY KEY, nome_produto TEXT, codigo_produto TEXT UNIQUE, preco_produto REAL, unidade_produto TEXT, situacao_produto TEXT, data_criacao_produto TEXT, gtin_produto TEXT, preco_promocional_produto REAL, preco_custo_produto REAL, preco_custo_medio_produto REAL, tipo_variacao_produto TEXT);""",
        """CREATE TABLE IF NOT EXISTS produto_categorias (id_produto INTEGER NOT NULL, id_categoria INTEGER NOT NULL, PRIMARY KEY (id_produto, id_categoria), FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE, FOREIGN KEY (id_categoria) REFERENCES categorias (id_categoria) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS produto_estoque_total (id_produto INTEGER PRIMARY KEY, nome_produto_estoque TEXT, saldo_total_api REAL, saldo_reservado_api REAL, data_ultima_atualizacao_api TIMESTAMP WITH TIME ZONE, FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS produto_estoque_depositos (id_estoque_deposito SERIAL PRIMARY KEY, id_produto INTEGER NOT NULL, nome_deposito TEXT, saldo_deposito REAL, desconsiderar_deposito TEXT, empresa_deposito TEXT, FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE, UNIQUE (id_produto, nome_deposito) );""",
        """CREATE TABLE IF NOT EXISTS pedidos (id_pedido INTEGER PRIMARY KEY, numero_pedido TEXT, numero_ecommerce TEXT, data_pedido TEXT, data_prevista TEXT, nome_cliente TEXT, valor_pedido REAL, id_vendedor INTEGER, nome_vendedor TEXT, situacao_pedido TEXT, codigo_rastreamento TEXT);""",
        """CREATE TABLE IF NOT EXISTS pedido_itens (id_item_pedido SERIAL PRIMARY KEY, id_pedido INTEGER NOT NULL, id_produto_tiny INTEGER, codigo_produto_pedido TEXT, descricao_produto_pedido TEXT, quantidade REAL, unidade_pedido TEXT, valor_unitario_pedido REAL, id_grade_pedido TEXT, FOREIGN KEY (id_pedido) REFERENCES pedidos (id_pedido) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS script_ultima_execucao (nome_processo TEXT PRIMARY KEY, timestamp_ultima_execucao TIMESTAMP WITH TIME ZONE );"""
    )
    try:
        with conn.cursor() as cur:
            for command in commands: cur.execute(command)
        if conn and not conn.closed: conn.commit()
        logger.info("Todas as tabelas foram verificadas/criadas.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Erro ao criar tabelas: {error}", exc_info=True)
        if conn and not conn.closed: conn.rollback()
        raise

def get_ultima_execucao(conn, nome_processo):
    """Obtém o timestamp da última execução bem-sucedida de um processo."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT timestamp_ultima_execucao FROM script_ultima_execucao WHERE nome_processo = %s", (nome_processo,))
            r = cur.fetchone()
            if r and r[0]: return (r[0] + datetime.timedelta(seconds=1)).strftime("%d/%m/%Y %H:%M:%S")
    except Exception as e: logger.error(f"Erro ao buscar última execução para '{nome_processo}': {e}", exc_info=True)
    return None

def set_ultima_execucao(conn, nome_processo, timestamp=None):
    """Define o timestamp da última execução de um processo."""
    ts = timestamp or datetime.datetime.now(datetime.timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO script_ultima_execucao (nome_processo, timestamp_ultima_execucao) VALUES (%s, %s)
                           ON CONFLICT (nome_processo) DO UPDATE SET timestamp_ultima_execucao = EXCLUDED.timestamp_ultima_execucao;""",
                        (nome_processo, ts))
        if conn and not conn.closed: conn.commit()
        logger.info(f"Timestamp para '{nome_processo}' definido: {ts.strftime('%d/%m/%Y %H:%M:%S %Z') if isinstance(ts, datetime.datetime) else ts}.")
    except Exception as e:
        logger.error(f"Erro ao definir última execução para '{nome_processo}': {e}", exc_info=True)
        if conn and not conn.closed: conn.rollback()

def salvar_categoria_db(conn, categoria_dict, id_pai=None):
    """Salva uma categoria e suas subcategorias recursivamente."""
    cat_id_str = categoria_dict.get("id") 
    cat_descricao = categoria_dict.get("descricao")
    if cat_id_str and cat_descricao: 
        try:
            cat_id = int(cat_id_str) 
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO categorias (id_categoria, descricao_categoria, id_categoria_pai)
                    VALUES (%s, %s, %s) ON CONFLICT (id_categoria) DO UPDATE SET
                    descricao_categoria = EXCLUDED.descricao_categoria, id_categoria_pai = EXCLUDED.id_categoria_pai;""",
                    (cat_id, cat_descricao, id_pai))
            if "nodes" in categoria_dict and isinstance(categoria_dict["nodes"], list):
                for sub_cat in categoria_dict["nodes"]: salvar_categoria_db(conn, sub_cat, id_pai=cat_id) 
        except ValueError: logger.warning(f"ID de categoria inválido '{cat_id_str}'. Categoria: {categoria_dict}", exc_info=False)
        except Exception as e: logger.error(f"Erro PostgreSQL ao salvar categoria ID '{cat_id_str}': {e}", exc_info=True)

def salvar_produto_db(conn, produto_api_data):
    """Salva os dados cadastrais de um produto no banco."""
    id_produto_str = str(produto_api_data.get("id","")).strip()
    if not id_produto_str or not id_produto_str.isdigit():
        logger.error(f"ID do produto inválido: '{id_produto_str}'. Dados: {produto_api_data}")
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
                cur.execute("SELECT id_produto FROM produtos WHERE codigo_produto = %s AND id_produto != %s", (codigo_produto_db, id_produto))
                if cur.fetchone():
                    logger.warning(f"SKU '{codigo_produto_db}' já em uso. Produto ID {id_produto} terá SKU NULL.")
                    codigo_produto_db = None
            cur.execute("""INSERT INTO produtos (id_produto, nome_produto, codigo_produto, preco_produto, unidade_produto, 
                           situacao_produto, data_criacao_produto, gtin_produto, preco_promocional_produto, 
                           preco_custo_produto, preco_custo_medio_produto, tipo_variacao_produto)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                           ON CONFLICT (id_produto) DO UPDATE SET nome_produto = EXCLUDED.nome_produto,
                           codigo_produto = EXCLUDED.codigo_produto, preco_produto = EXCLUDED.preco_produto,
                           unidade_produto = EXCLUDED.unidade_produto, situacao_produto = EXCLUDED.situacao_produto,
                           data_criacao_produto = EXCLUDED.data_criacao_produto, gtin_produto = EXCLUDED.gtin_produto,
                           preco_promocional_produto = EXCLUDED.preco_promocional_produto,
                           preco_custo_produto = EXCLUDED.preco_custo_produto,
                           preco_custo_medio_produto = EXCLUDED.preco_custo_medio_produto,
                           tipo_variacao_produto = EXCLUDED.tipo_variacao_produto;""",
                        (id_produto, nome_produto, codigo_produto_db, preco, produto_api_data.get("unidade"), 
                         produto_api_data.get("situacao"), produto_api_data.get("data_criacao"), produto_api_data.get("gtin"),
                         preco_promocional, preco_custo, preco_custo_medio, produto_api_data.get("tipoVariacao")))
        logger.debug(f"Produto ID {id_produto} salvo/atualizado.")
    except Exception as e: logger.error(f"Erro ao salvar produto ID '{id_produto_str}': {e}", exc_info=True); raise

def salvar_produto_categorias_db(conn, id_produto, categorias_lista_api):
    """Salva o relacionamento produto-categorias usando execute_values."""
    if not isinstance(id_produto, int): raise ValueError(f"ID do produto inválido: {id_produto}")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_categorias WHERE id_produto = %s", (id_produto,))
            if categorias_lista_api and isinstance(categorias_lista_api, list):
                dados = []
                for cat_item_api in categorias_lista_api:
                    if isinstance(cat_item_api, dict):
                        cat_id_str = str(cat_item_api.get("id", "")).strip()
                        if cat_id_str and cat_id_str.isdigit():
                            dados.append((id_produto, int(cat_id_str)))
                        else: logger.warning(f"ID de categoria inválido '{cat_id_str}' para produto {id_produto}. Item: {cat_item_api}")
                if dados:
                    execute_values(cur, "INSERT INTO produto_categorias (id_produto, id_categoria) VALUES %s ON CONFLICT DO NOTHING", dados)
                    logger.debug(f"{len(dados)} categorias salvas para produto ID {id_produto}.")
    except Exception as e: logger.error(f"Erro ao salvar categorias do produto ID {id_produto}: {e}", exc_info=True); raise

def salvar_produto_estoque_total_db(conn, id_produto, nome_produto, saldo_total_api, saldo_reservado_api, data_ultima_atualizacao_estoque=None):
    """Salva o estoque total de um produto."""
    id_p = int(id_produto)
    try:
        st = safe_float_convert(saldo_total_api); sr = safe_float_convert(saldo_reservado_api)
        dt_att = None
        if data_ultima_atualizacao_estoque is None: dt_att = datetime.datetime.now(datetime.timezone.utc)
        elif isinstance(data_ultima_atualizacao_estoque, str):
            try: dt_att = datetime.datetime.strptime(data_ultima_atualizacao_estoque, "%d/%m/%Y %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
            except ValueError: dt_att = datetime.datetime.now(datetime.timezone.utc); logger.warning(f"Data de estoque inválida '{data_ultima_atualizacao_estoque}', usando atual.")
        elif isinstance(data_ultima_atualizacao_estoque, datetime.datetime):
            dt_att = data_ultima_atualizacao_estoque if data_ultima_atualizacao_estoque.tzinfo else data_ultima_atualizacao_estoque.replace(tzinfo=datetime.timezone.utc)
        else: dt_att = datetime.datetime.now(datetime.timezone.utc); logger.warning(f"Tipo de data de estoque inesperado, usando atual.")
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO produto_estoque_total (id_produto, nome_produto_estoque, saldo_total_api, saldo_reservado_api, data_ultima_atualizacao_api)
                           VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id_produto) DO UPDATE SET
                           nome_produto_estoque = EXCLUDED.nome_produto_estoque, saldo_total_api = EXCLUDED.saldo_total_api,
                           saldo_reservado_api = EXCLUDED.saldo_reservado_api, data_ultima_atualizacao_api = EXCLUDED.data_ultima_atualizacao_api;""",
                        (id_p, nome_produto, st, sr, dt_att))
        logger.debug(f"Estoque total para produto ID {id_p} salvo.")
    except Exception as e: logger.error(f"Erro ao salvar estoque total para Produto ID {id_produto}: {e}", exc_info=True); raise

def salvar_estoque_por_deposito_db(conn, id_produto, nome_produto, lista_depositos_api):
    """Salva o estoque por depósito de um produto."""
    id_p = int(id_produto)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_estoque_depositos WHERE id_produto = %s", (id_p,))
            if not lista_depositos_api or not isinstance(lista_depositos_api, list): return
            dados = []
            for dep_w in lista_depositos_api:
                dep_d = dep_w.get("deposito") or (dep_w if isinstance(dep_w, dict) else None)
                if dep_d and isinstance(dep_d, dict):
                    dados.append((id_p, dep_d.get("nome"), safe_float_convert(dep_d.get("saldo")), dep_d.get("desconsiderar"), dep_d.get("empresa")))
            if dados:
                execute_values(cur, """INSERT INTO produto_estoque_depositos (id_produto, nome_deposito, saldo_deposito, desconsiderar_deposito, empresa_deposito) 
                                       VALUES %s ON CONFLICT (id_produto, nome_deposito) DO UPDATE SET saldo_deposito = EXCLUDED.saldo_deposito,
                                       desconsiderar_deposito = EXCLUDED.desconsiderar_deposito, empresa_deposito = EXCLUDED.empresa_deposito;""", dados)
                logger.debug(f"{len(dados)} depósitos salvos para produto ID {id_p}.")
    except Exception as e: logger.error(f"Erro ao salvar estoque por depósito para Produto ID {id_produto}: {e}", exc_info=True); raise

def salvar_pedido_db(conn, pedido_api_data):
    """Salva os dados de um pedido no banco."""
    id_ped_str = str(pedido_api_data.get("id","")).strip()
    if not id_ped_str or not id_ped_str.isdigit(): raise ValueError(f"ID do pedido inválido: {id_ped_str}")
    id_ped = int(id_ped_str)
    try:
        v = safe_float_convert(pedido_api_data.get("valor"))
        id_vend_str = str(pedido_api_data.get("id_vendedor","")).strip()
        id_vend_db = int(id_vend_str) if id_vend_str and id_vend_str.isdigit() else None
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO pedidos (id_pedido, numero_pedido, numero_ecommerce, data_pedido, data_prevista, nome_cliente, 
                           valor_pedido, id_vendedor, nome_vendedor, situacao_pedido, codigo_rastreamento)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_pedido) DO UPDATE SET
                           numero_pedido=EXCLUDED.numero_pedido, numero_ecommerce=EXCLUDED.numero_ecommerce, data_pedido=EXCLUDED.data_pedido,
                           data_prevista=EXCLUDED.data_prevista, nome_cliente=EXCLUDED.nome_cliente, valor_pedido=EXCLUDED.valor_pedido,
                           id_vendedor=EXCLUDED.id_vendedor, nome_vendedor=EXCLUDED.nome_vendedor, situacao_pedido=EXCLUDED.situacao_pedido,
                           codigo_rastreamento=EXCLUDED.codigo_rastreamento;""",
                        (id_ped, pedido_api_data.get("numero"), pedido_api_data.get("numero_ecommerce"), pedido_api_data.get("data_pedido"),
                         pedido_api_data.get("data_prevista"), pedido_api_data.get("nome"), v, id_vend_db, pedido_api_data.get("nome_vendedor"),
                         pedido_api_data.get("situacao"), pedido_api_data.get("codigo_rastreamento")))
        logger.debug(f"Pedido ID {id_ped} salvo/atualizado.")
    except Exception as e: logger.error(f"Erro ao salvar pedido ID '{id_ped_str}': {e}", exc_info=True); raise

def salvar_pedido_itens_db(conn, id_pedido_api, itens_lista_api):
    """Salva os itens de um pedido no banco usando execute_values."""
    id_ped_int = int(id_pedido_api)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pedido_itens WHERE id_pedido = %s", (id_ped_int,))
            if not itens_lista_api or not isinstance(itens_lista_api, list): return
            dados = []
            for item_w in itens_lista_api:
                item_d = item_w.get("item")
                if item_d and isinstance(item_d, dict):
                    id_prod_tiny_str = str(item_d.get("id_produto","")).strip()
                    id_prod_tiny = int(id_prod_tiny_str) if id_prod_tiny_str and id_prod_tiny_str.isdigit() else None
                    dados.append((id_ped_int, id_prod_tiny, item_d.get("codigo"), item_d.get("descricao"),
                                  safe_float_convert(item_d.get("quantidade")), item_d.get("unidade"),
                                  safe_float_convert(item_d.get("valor_unitario")), item_d.get("id_grade")))
            if dados:
                execute_values(cur, """INSERT INTO pedido_itens (id_pedido, id_produto_tiny, codigo_produto_pedido, descricao_produto_pedido,
                                       quantidade, unidade_pedido, valor_unitario_pedido, id_grade_pedido) VALUES %s""", dados)
                logger.debug(f"{len(dados)} itens salvos para pedido ID {id_ped_int}.")
    except Exception as e: logger.error(f"Erro ao salvar itens do pedido ID {id_pedido_api}: {e}", exc_info=True); raise

# --- Funções da API ---
def make_api_v2_request(endpoint_path, method="GET", payload_dict=None, 
                        max_retries=3, initial_retry_delay=2, timeout_seconds=DEFAULT_API_TIMEOUT):
    """Realiza uma requisição para a API v2 do Tiny ERP."""
    full_url = f"{BASE_URL_V2}{endpoint_path}"
    base_params = {"token": API_V2_TOKEN, "formato": "json"}
    all_params = base_params.copy()
    if payload_dict: all_params.update(payload_dict)
    params_log = {k: (v[:5] + '...' if k == 'token' and isinstance(v, str) and v else v) for k, v in all_params.items()}
    retries_attempted = 0
    current_delay = initial_retry_delay
    while retries_attempted <= max_retries:
        response = None 
        try:
            if retries_attempted > 0: 
                actual_retry_delay = current_delay
                logger.info(f"Aguardando {actual_retry_delay}s antes da tentativa {retries_attempted + 1}/{max_retries + 1} para {endpoint_path}...")
                time.sleep(actual_retry_delay)
                if actual_retry_delay < RETRY_DELAY_429 : 
                    current_delay = min(current_delay * 2, 30)
            logger.debug(f"Tentativa {retries_attempted + 1}/{max_retries + 1} - {method} {full_url} - Params: {params_log}")
            if method.upper() == "GET": response = requests.get(full_url, params=all_params, timeout=timeout_seconds)
            elif method.upper() == "POST": response = requests.post(full_url, data=all_params, timeout=timeout_seconds)
            else: logger.error(f"Método {method} não suportado."); return None, False 
            logger.debug(f"Resposta de {endpoint_path}: Status {response.status_code}, Conteúdo (parcial): {response.text[:200]}")
            response.raise_for_status()
            try: response_data = response.json()
            except json.JSONDecodeError as e_json_inner:
                logger.error(f"Erro ao decodificar JSON (interno) de {endpoint_path}: {e_json_inner}", exc_info=True)
                logger.debug(f"Corpo da Resposta (não JSON): {response.text[:500]}")
                return None, False
            retorno_geral = response_data.get("retorno")
            if not retorno_geral:
                logger.error(f"Chave 'retorno' ausente na resposta JSON de {endpoint_path}. Resposta: {str(response_data)[:500]}")
                return None, False 
            if endpoint_path == ENDPOINT_CATEGORIAS:
                if isinstance(retorno_geral, list): return retorno_geral, True 
                if isinstance(retorno_geral, dict):
                    if retorno_geral.get("status") == "OK" and "categorias" in retorno_geral and isinstance(retorno_geral["categorias"], list):
                        return retorno_geral["categorias"], True
                    if retorno_geral.get("status") != "OK":
                        logger.error(f"API Tiny (Categorias): Status '{retorno_geral.get('status')}', Erros: {retorno_geral.get('erros', [])}")
                        return None, False 
                logger.error(f"Resposta inesperada para Categorias: {type(retorno_geral)}. Conteúdo: {str(retorno_geral)[:300]}")
                return None, False 
            if not isinstance(retorno_geral, dict):
                logger.error(f"'retorno' não é um dicionário para {endpoint_path}. Conteúdo: {str(retorno_geral)[:300]}")
                return None, False 
            status_api = retorno_geral.get("status")
            status_processamento = str(retorno_geral.get("status_processamento", ""))
            if status_api != "OK":
                erros_api = retorno_geral.get("erros", []) 
                codigo_erro, msg_erro = "", ""
                if erros_api and isinstance(erros_api[0], dict):
                    err_obj = erros_api[0].get("erro", {})
                    codigo_erro = err_obj.get("codigo", "") if isinstance(err_obj, dict) else ""
                    msg_erro = err_obj.get("erro", str(err_obj)) if isinstance(err_obj, dict) else str(err_obj)
                elif erros_api and isinstance(erros_api[0], str): msg_erro = erros_api[0]
                logger.error(f"API Tiny: Status '{status_api}' (Endpoint: {endpoint_path}). Código: {codigo_erro}. Msg: {msg_erro}. Resp: {str(retorno_geral)[:500]}")
                if codigo_erro == "2": logger.critical("Token da API Tiny inválido ou expirado.")
                # Se o código de erro for "35" (erro genérico de consulta), não retentar infinitamente dentro desta função,
                # mas permitir que o loop de retries principal (para erros de rede/HTTP) tente algumas vezes.
                # No entanto, para erros da API, o retorno False aqui impede retries do loop while principal para este tipo de falha.
                # Apenas erros de rede/HTTP caem nos excepts abaixo que permitem o loop.
                return None, False # Falha da API, geralmente não retentável aqui.
            if status_processamento not in ["3", "10"]: 
                msg_proc_err = ""
                errs_ret = retorno_geral.get("erros", [])
                if errs_ret and isinstance(errs_ret[0], dict) and "erro" in errs_ret[0]:
                    err_det = errs_ret[0]["erro"]
                    msg_proc_err = str(err_det.get("erro", err_det) if isinstance(err_det, dict) else err_det)
                elif errs_ret and isinstance(errs_ret[0], str): msg_proc_err = errs_ret[0]
                if "Nenhum registro encontrado" in msg_proc_err: 
                    logger.info(f"Nenhum registro encontrado para {endpoint_path} (Status Proc: {status_processamento}).")
                    return retorno_geral, True
                logger.warning(f"API: Status de processamento '{status_processamento}' ({endpoint_path}). Msg: '{msg_proc_err}'. Resp: {str(retorno_geral)[:300]}")
                if status_processamento == "2": return None, False 
            return retorno_geral, True 
        except requests.exceptions.HTTPError as e_http:
            logger.warning(f"Erro HTTP (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {e_http}", exc_info=False)
            if response is not None: 
                logger.debug(f"Corpo da Resposta HTTP: {response.text[:500]}")
                if response.status_code == 429:
                    logger.warning(f"Limite de taxa (429) atingido. Próxima tentativa com delay de {RETRY_DELAY_429}s.")
                    current_delay = RETRY_DELAY_429 
                elif 400 <= response.status_code < 500: return None, False 
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e_net:
            logger.warning(f"Erro de Rede/Timeout (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_net).__name__} - {e_net}", exc_info=False)
        except requests.exceptions.RequestException as e_req: 
            logger.warning(f"Erro de Requisição (Tentativa {retries_attempted + 1}/{max_retries + 1}) em {endpoint_path}: {type(e_req).__name__} - {e_req}", exc_info=False)
        except json.JSONDecodeError as e_json: # Captura se response.json() falhar no início
            logger.error(f"Erro ao decodificar JSON de {endpoint_path} (Tentativa {retries_attempted + 1}/{max_retries + 1}): {e_json}", exc_info=True)
            if response is not None: logger.debug(f"Corpo da Resposta (não JSON): {response.text[:500]}")
            return None, False 
        except Exception as e_geral: 
            logger.error(f"Erro INESPERADO em make_api_v2_request ({endpoint_path}, Tentativa {retries_attempted + 1}): {e_geral}", exc_info=True)
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
    logger.info("Iniciando busca e salvamento de Categorias.")
    lista_cats, sucesso = make_api_v2_request(ENDPOINT_CATEGORIAS)
    if sucesso and isinstance(lista_cats, list):
        if not lista_cats: logger.info("Nenhuma categoria retornada pela API."); set_ultima_execucao(conn, PROCESSO_CATEGORIAS); return True
        ok_todas = True; num_cats_raiz = 0
        for cat_raiz in lista_cats:
            if isinstance(cat_raiz, dict):
                try: salvar_categoria_db(conn, cat_raiz); num_cats_raiz+=1
                except Exception as e: logger.error(f"Erro ao salvar cat. ID '{cat_raiz.get('id')}': {e}", exc_info=True); ok_todas = False
            else: logger.warning(f"Item de categoria raiz inesperado: {cat_raiz}")
        if ok_todas:
            try:
                if conn and not conn.closed: conn.commit(); logger.info(f"{num_cats_raiz} categorias raiz processadas e commitadas.")
                set_ultima_execucao(conn, PROCESSO_CATEGORIAS); return True
            except Exception as e: logger.error(f"Erro ao commitar categorias: {e}", exc_info=True);
        if conn and not conn.closed: conn.rollback(); logger.warning("Rollback de categorias devido a erros.")
        return False
    logger.error(f"Não foi possível buscar/salvar categorias. Sucesso API: {sucesso}.")
    if lista_cats is not None: logger.debug(f"Dados de cats (parcial): {str(lista_cats)[:300]}")
    return False

def get_produto_detalhes_v2(id_produto_tiny):
    """Busca os detalhes de um produto específico, incluindo suas categorias."""
    logger.debug(f"Buscando detalhes para produto ID {id_produto_tiny}...")
    ret_obj, suc = make_api_v2_request(ENDPOINT_PRODUTO_OBTER, payload_dict={"id": id_produto_tiny})
    if suc and ret_obj and isinstance(ret_obj.get("produto"), dict): return ret_obj["produto"]
    logger.warning(f"Produto ID {id_produto_tiny} sem detalhes na API ou erro.")
    return None

def search_produtos_v2(conn, data_alteracao_inicial=None, pagina=1):
    """Busca produtos (cadastrais e categorias) e os salva. Retorna (lista_api, num_pags, sucesso_db_pag)."""
    logger.info(f"Buscando pág {pagina} de produtos desde {data_alteracao_inicial or 'inicio'}.")
    params = {"pagina": pagina}
    if data_alteracao_inicial: params["dataAlteracaoInicial"] = data_alteracao_inicial
    ret_api, suc_api = make_api_v2_request(ENDPOINT_PRODUTOS_PESQUISA, payload_dict=params)
    prods_pag_api, num_pags_tot, pag_ok_db = [], 0, False
    if suc_api and ret_api: prods_pag_api = ret_api.get("produtos",[]); num_pags_tot = int(ret_api.get('numero_paginas',0))
    elif not suc_api: logger.error(f"Falha ao buscar produtos (API) pág {pagina}."); return None, 0, False
    if prods_pag_api and isinstance(prods_pag_api, list):
        todos_ok, salvos_pag = True, 0
        for prod_w in prods_pag_api:
            prod_d = prod_w.get("produto")
            if not prod_d or not isinstance(prod_d, dict): logger.warning(f"Item produto malformado (pág {pagina}): {prod_w}"); continue
            id_prod_str = str(prod_d.get("id","")).strip()
            try:
                id_prod_int = int(id_prod_str); salvar_produto_db(conn, prod_d)
                time.sleep(0.5) # Pausa antes de buscar detalhes do produto
                det_prod = get_produto_detalhes_v2(id_prod_int)
                if det_prod and "categorias" in det_prod: salvar_produto_categorias_db(conn, id_prod_int, det_prod["categorias"])
                salvos_pag += 1
            except Exception as e: logger.error(f"Erro no produto ID '{id_prod_str}' (pág {pagina}): {e}", exc_info=True); todos_ok=False; break
        if todos_ok and salvos_pag > 0:
            try:
                if conn and not conn.closed: conn.commit(); logger.info(f"Pág {pagina} produtos ({salvos_pag} itens) commitada.")
                pag_ok_db = True
            except Exception as e: 
                logger.error(f"Erro CRÍTICO ao commitar pág {pagina} produtos: {e}", exc_info=True);
                if conn and not conn.closed: conn.rollback() # Tenta rollback se o commit falhou
                # pag_ok_db permanece False se o commit falhar
        elif not todos_ok and conn and not conn.closed: conn.rollback(); logger.warning(f"Pág {pagina} produtos com erros. ROLLBACK.")
        # Se salvos_pag == 0 mas todos_ok é True (ex: página vazia), pag_ok_db permanece False mas não é um erro de rollback.
        # A função chamadora decide se continua baseada em pag_ok_db e se prods_pag_api é None.
        return prods_pag_api, num_pags_tot, pag_ok_db
    logger.info(f"Nenhum produto na API para pág {pagina} ou estrutura inválida.")
    return [], num_pags_tot, True # Página vazia é "OK" para continuar paginação.

def processar_atualizacoes_estoque_v2(conn, data_alteracao_estoque_inicial=None, pagina=1):
    """Busca atualizações de estoque e salva. Retorna (lista_api, num_pags, sucesso_db_pag)."""
    logger.info(f"Buscando pág {pagina} de atualizações de estoque desde {data_alteracao_estoque_inicial or 'inicio'}.")
    params_api = {"pagina": pagina}
    if data_alteracao_estoque_inicial: params_api["dataAlteracao"] = data_alteracao_estoque_inicial
    ret_api, suc_api = make_api_v2_request(ENDPOINT_LISTA_ATUALIZACOES_ESTOQUE, payload_dict=params_api)
    prods_est_api, num_pags_tot, pag_ok_db = [], 0, False
    if suc_api and ret_api: prods_est_api = ret_api.get("produtos",[]); num_pags_tot = int(ret_api.get('numero_paginas',0))
    elif not suc_api: logger.error(f"Falha ao buscar atualizações de estoque (API) pág {pagina}."); return None, 0, False
    if prods_est_api and isinstance(prods_est_api, list):
        todos_ok, salvos_pag = True, 0
        for prod_w in prods_est_api:
            prod_est_d = prod_w.get("produto")
            if not prod_est_d or not isinstance(prod_est_d, dict): logger.warning(f"Item estoque malformado (pág {pagina}): {prod_w}"); continue
            id_prod_str = str(prod_est_d.get("id","")).strip()
            try:
                if not id_prod_str or not id_prod_str.isdigit(): logger.warning(f"ID produto inválido (estoque): '{id_prod_str}'. Dados: {prod_est_d}"); continue
                id_prod_int = int(id_prod_str)
                # VERIFICA SE O PRODUTO EXISTE ANTES DE SALVAR ESTOQUE
                produto_existe_no_db = False
                with conn.cursor() as cur_chk: cur_chk.execute("SELECT 1 FROM produtos WHERE id_produto = %s", (id_prod_int,)); prod_exists = cur_chk.fetchone()
                if prod_exists: produto_existe_no_db = True
                
                if not produto_existe_no_db:
                    logger.warning(f"Produto ID {id_prod_int} (da lista de estoque) não encontrado na tabela 'produtos'. O estoque para este produto específico será pulado nesta passagem.")
                    continue # Pula para o próximo item de estoque, não quebra o loop da página nem marca como erro total da página
                
                salvar_produto_estoque_total_db(conn, id_prod_int, prod_est_d.get("nome", f"Produto ID {id_prod_int}"), 
                                                prod_est_d.get("saldo"), prod_est_d.get("saldoReservado"), prod_est_d.get("data_alteracao"))
                deps = prod_est_d.get("depositos", [])
                salvar_estoque_por_deposito_db(conn, id_prod_int, prod_est_d.get("nome", f"Produto ID {id_prod_int}"), deps)
                salvos_pag += 1
            except Exception as e: logger.error(f"Erro no estoque produto ID {id_prod_str} (pág {pagina}): {e}", exc_info=True); todos_ok=False; break
        if todos_ok and salvos_pag > 0:
            try:
                if conn and not conn.closed: conn.commit(); logger.info(f"Pág {pagina} estoques ({salvos_pag} itens) commitada.")
                pag_ok_db = True
            except Exception as e: 
                logger.error(f"Erro CRÍTICO ao commitar pág {pagina} estoques: {e}", exc_info=True);
                if conn and not conn.closed: conn.rollback()
        elif not todos_ok and conn and not conn.closed: conn.rollback(); logger.warning(f"Pág {pagina} estoques com erros. ROLLBACK.")
        return prods_est_api, num_pags_tot, pag_ok_db
    logger.info(f"Nenhuma atualização de estoque na API para pág {pagina} ou estrutura inválida.")
    return [], num_pags_tot, True

def get_detalhes_pedido_v2(id_pedido_api):
    """Busca os detalhes de um pedido específico, incluindo seus itens."""
    logger.debug(f"Buscando detalhes para pedido ID {id_pedido_api}...")
    ret_obj, suc = make_api_v2_request(ENDPOINT_PEDIDO_OBTER, payload_dict={"id": id_pedido_api})
    if suc and ret_obj and isinstance(ret_obj.get("pedido"), dict): return ret_obj["pedido"]
    logger.warning(f"Pedido ID {id_pedido_api} sem detalhes na API ou erro.")
    return None

def search_pedidos_v2(conn, data_alteracao_ou_inicial=None, pagina=1):
    """Busca pedidos e salva. Retorna (lista_api, num_pags, sucesso_db_pag)."""
    logger.info(f"Buscando pág {pagina} de pedidos desde {data_alteracao_ou_inicial or 'inicio'}.")
    params_api = {"pagina": pagina}
    if data_alteracao_ou_inicial: params_api["dataAlteracaoInicial"] = data_alteracao_ou_inicial
    ret_api, suc_api = make_api_v2_request(ENDPOINT_PEDIDOS_PESQUISA, payload_dict=params_api)
    peds_pag_api, num_pags_tot, pag_ok_db = [], 0, False
    if suc_api and ret_api: peds_pag_api = ret_api.get("pedidos",[]); num_pags_tot = int(ret_api.get('numero_paginas',0))
    elif not suc_api: logger.error(f"Falha ao buscar pedidos (API) pág {pagina}."); return None, 0, False
    if peds_pag_api and isinstance(peds_pag_api, list):
        todos_ok, salvos_pag = True, 0
        for ped_w in peds_pag_api:
            ped_d = ped_w.get("pedido")
            if not ped_d or not isinstance(ped_d, dict): logger.warning(f"Item pedido malformado (pág {pagina}): {ped_w}"); continue
            id_ped_str = str(ped_d.get("id","")).strip()
            try:
                id_ped_int = int(id_ped_str); salvar_pedido_db(conn, ped_d)
                time.sleep(0.6)
                det_ped = get_detalhes_pedido_v2(id_ped_int)
                if det_ped and "itens" in det_ped: salvar_pedido_itens_db(conn, id_ped_int, det_ped["itens"])
                salvos_pag +=1
            except Exception as e: logger.error(f"Erro no pedido ID '{id_ped_str}' (pág {pagina}): {e}", exc_info=True); todos_ok=False; break
        if todos_ok and salvos_pag > 0:
            try:
                if conn and not conn.closed: conn.commit(); logger.info(f"Pág {pagina} pedidos ({salvos_pag} itens) commitada.")
                pag_ok_db = True
            except Exception as e: 
                logger.error(f"Erro CRÍTICO ao commitar pág {pagina} pedidos: {e}", exc_info=True);
                if conn and not conn.closed: conn.rollback()
        elif not todos_ok and conn and not conn.closed: conn.rollback(); logger.warning(f"Pág {pagina} pedidos com erros. ROLLBACK.")
        return peds_pag_api, num_pags_tot, pag_ok_db
    logger.info(f"Nenhum pedido na API para pág {pagina} ou estrutura inválida.")
    return [], num_pags_tot, True


# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    logger.info("=== Iniciando Cliente para API v2 do Tiny ERP (PostgreSQL, Incremental) ===")
    start_time_total = time.time()

    if not all([API_V2_TOKEN, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        logger.critical("Variáveis de ambiente cruciais não configuradas. Encerrando.")
        exit(1)

    db_conn = get_db_connection()
    if db_conn is None or (hasattr(db_conn, 'closed') and db_conn.closed): # Verifica se a conexão é None ou já está fechada
        logger.critical("Falha na conexão com o banco de dados. Encerrando.")
        exit(1)
    
    try:
        criar_tabelas_db(db_conn) 

        # PASSO 1: Categorias
        logger.info("--- INICIANDO PASSO 1: Processamento de Categorias ---")
        if get_categorias_v2(db_conn): logger.info("Passo 1 (Categorias) concluído.")
        else: logger.warning("Passo 1 (Categorias) com falhas ou sem dados.")
        logger.info("-" * 70)

        # PASSO 2: Produtos Cadastrais e Categorias de Produtos
        logger.info("--- INICIANDO PASSO 2: Produtos (Cadastrais e Categorias) ---")
        ultima_exec_prod_str = get_ultima_execucao(db_conn, PROCESSO_PRODUTOS)
        data_filtro_prod = ultima_exec_prod_str if ultima_exec_prod_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        total_prods_listados, pag_prod, ts_inicio_prod, etapa_prod_ok = 0, 1, datetime.datetime.now(datetime.timezone.utc), True
        logger.info(f"Iniciando busca de produtos (cadastrais) desde: {data_filtro_prod}.")
        while True: 
            logger.info(f"Processando pág {pag_prod} de produtos (cadastrais)...")
            prods_pag, total_pags, pag_commit = search_produtos_v2(db_conn, data_filtro_prod, pag_prod)
            if prods_pag is None: logger.error(f"Falha crítica (API) pág {pag_prod} produtos. Interrompendo."); etapa_prod_ok=False; break 
            if not pag_commit and prods_pag: logger.warning(f"Pág {pag_prod} produtos não commitada. Interrompendo."); etapa_prod_ok=False; break
            if prods_pag: total_prods_listados += len(prods_pag) 
            if total_pags == 0 or pag_prod >= total_pags: logger.info("Todas as págs de produtos (cadastrais) processadas."); break
            pag_prod += 1
            if pag_prod <= total_pags: logger.info("Pausa (1s) antes da próxima pág de produtos..."); time.sleep(1) 
        if etapa_prod_ok: set_ultima_execucao(db_conn, PROCESSO_PRODUTOS, ts_inicio_prod); logger.info(f"Passo 2 (Produtos Cadastrais) concluído. {total_prods_listados} produtos listados. Timestamp atualizado.")
        else: logger.warning("Passo 2 (Produtos Cadastrais) com erros. Timestamp NÃO atualizado.")
        logger.info("-" * 70)

        # PASSO 3: Atualizações de Estoque
        logger.info("--- INICIANDO PASSO 3: Atualizações de Estoque ---")
        ultima_exec_est_str = get_ultima_execucao(db_conn, PROCESSO_ESTOQUES)
        data_filtro_est_api = None
        hoje_utc = datetime.datetime.now(datetime.timezone.utc)
        dias_lim_est = 29 
        data_lim_est_api = hoje_utc - datetime.timedelta(days=dias_lim_est)

        if ultima_exec_est_str:
            try:
                # Convertendo string para datetime. Presume-se que a string de get_ultima_execucao não tem timezone.
                # Adicionamos UTC para consistência com data_lim_est_api que é UTC.
                ult_exec_obj_naive = datetime.datetime.strptime(ultima_exec_est_str, "%d/%m/%Y %H:%M:%S")
                ult_exec_obj_utc = ult_exec_obj_naive.replace(tzinfo=datetime.timezone.utc)
                
                if ult_exec_obj_utc < data_lim_est_api:
                    logger.warning(f"Última sync de estoque ({ultima_exec_est_str}) > {dias_lim_est} dias. Ajustando filtro para últimos {dias_lim_est} dias ({data_lim_est_api.strftime('%d/%m/%Y %H:%M:%S %Z')}).")
                    data_filtro_est_api = data_lim_est_api.strftime("%d/%m/%Y %H:%M:%S")
                else: 
                    data_filtro_est_api = ultima_exec_est_str
            except ValueError:
                logger.error(f"Data inválida para ultima_exec_estoques: {ultima_exec_est_str}. Usando filtro de {dias_lim_est} dias."); 
                data_filtro_est_api = data_lim_est_api.strftime("%d/%m/%Y %H:%M:%S")
        else:
            logger.info(f"Primeira execução de estoques. Busca limitada aos últimos {dias_lim_est} dias ({data_lim_est_api.strftime('%d/%m/%Y %H:%M:%S %Z')}).")
            logger.warning("Para carga inicial completa de todos os estoques, uma estratégia de busca individual seria necessária na primeira vez.")
            data_filtro_est_api = data_lim_est_api.strftime("%d/%m/%Y %H:%M:%S")
        
        total_est_listados, pag_est, ts_inicio_est, etapa_est_ok = 0, 1, datetime.datetime.now(datetime.timezone.utc), True
        logger.info(f"Iniciando busca de atualizações de estoque desde: {data_filtro_est_api}.")
        while True:
            logger.info(f"Processando pág {pag_est} de atualizações de estoque...")
            est_pag, total_pags_est, pag_commit_est = processar_atualizacoes_estoque_v2(db_conn,data_filtro_est_api,pag_est)
            if est_pag is None: logger.error(f"Falha crítica (API) pág {pag_est} estoques. Interrompendo."); etapa_est_ok=False; break
            if not pag_commit_est and est_pag: logger.warning(f"Pág {pag_est} estoques não commitada. Interrompendo."); etapa_est_ok=False; break
            if est_pag: total_est_listados += len(est_pag)
            if total_pags_est == 0 or pag_est >= total_pags_est: logger.info("Todas as págs de estoques processadas."); break
            pag_est += 1
            if pag_est <= total_pags_est: logger.info("Pausa (1s) antes da próxima pág de estoques..."); time.sleep(1)
        
        if etapa_est_ok: 
            set_ultima_execucao(db_conn, PROCESSO_ESTOQUES, ts_inicio_est)
            logger.info(f"Passo 3 (Estoques) concluído. {total_est_listados} atualizações listadas. Timestamp atualizado.")
        else: 
            logger.warning("Passo 3 (Estoques) com erros. Timestamp NÃO atualizado.")
        logger.info("-" * 70)

        # PASSO 4: Pedidos e Itens de Pedidos
        logger.info("--- INICIANDO PASSO 4: Pedidos e Itens ---")
        ultima_exec_ped_str = get_ultima_execucao(db_conn, PROCESSO_PEDIDOS)
        data_filtro_ped = ultima_exec_ped_str if ultima_exec_ped_str else DATA_INICIAL_PRIMEIRA_CARGA_INCREMENTAL
        
        total_peds_listados, pag_ped, ts_inicio_ped, etapa_peds_ok = 0, 1, datetime.datetime.now(datetime.timezone.utc), True
        logger.info(f"Iniciando busca de pedidos desde: {data_filtro_ped}.")
        while True: 
            logger.info(f"Processando pág {pag_ped} de pedidos...")
            peds_pag, total_pags_ped, pag_commit_ped = search_pedidos_v2(db_conn,data_filtro_ped,pag_ped)
            if peds_pag is None: 
                logger.error(f"Falha crítica (API) pág {pag_ped} pedidos. Interrompendo."); etapa_peds_ok=False; break
            if not pag_commit_ped and peds_pag: 
                logger.warning(f"Pág {pag_ped} pedidos não commitada. Interrompendo."); etapa_peds_ok=False; break
            if peds_pag: 
                total_peds_listados += len(peds_pag)
            if total_pags_ped == 0 or pag_ped >= total_pags_ped: 
                logger.info("Todas as págs de pedidos processadas."); break
            pag_ped += 1
            if pag_ped <= total_pags_ped: 
                logger.info("Pausa (1s) antes da próxima pág de pedidos..."); time.sleep(1) 
        
        if etapa_peds_ok: 
            set_ultima_execucao(db_conn, PROCESSO_PEDIDOS, ts_inicio_ped)
            logger.info(f"Passo 4 (Pedidos) concluído. {total_peds_listados} pedidos listados. Timestamp atualizado.")
        else: 
            logger.warning("Passo 4 (Pedidos) com erros. Timestamp NÃO atualizado.")
        logger.info("-" * 70)

        logger.info("--- Contagem final dos registros no banco de dados ---")
        if db_conn and not db_conn.closed:
            with db_conn.cursor() as cur:
                tabelas = ["categorias", "produtos", "produto_categorias", "produto_estoque_total", 
                           "produto_estoque_depositos", "pedidos", "pedido_itens", "script_ultima_execucao"]
                for tab in tabelas:
                    try:
                        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tab)))
                        logger.info(f"  - Tabela '{tab}': {cur.fetchone()[0]} registros.")
                    except Exception as e: logger.error(f"  Erro ao contar '{tab}': {e}", exc_info=True)
        else: logger.warning("Não foi possível contar registros, DB fechado/indisponível.")
            
    except KeyboardInterrupt:
        logger.warning("Processo interrompido (KeyboardInterrupt).")
        if db_conn and not db_conn.closed: try: db_conn.rollback(); logger.info("Rollback por KI.")
        except Exception as e: logger.error(f"Erro no rollback KI: {e}", exc_info=True)
    except Exception as e_geral:
        logger.critical(f"ERRO GERAL NO PROCESSAMENTO: {e_geral}", exc_info=True)
        if db_conn and not db_conn.closed: try: db_conn.rollback(); logger.info("Rollback por erro geral.")
        except Exception as e: logger.error(f"Erro no rollback geral: {e}", exc_info=True)
    finally:
        if db_conn and not (hasattr(db_conn, 'closed') and db_conn.closed):
            db_conn.close(); logger.info("Conexão PostgreSQL fechada.")
        elif db_conn is None: logger.info("Nenhuma conexão DB para fechar.")
        else: logger.info("Conexão DB já estava fechada.")

    logger.info(f"=== Processo Concluído em {time.time() - start_time_total:.2f} segundos ===")