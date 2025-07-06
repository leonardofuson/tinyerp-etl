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
logger = logging.getLogger("tiny_api_v2_cliente")

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

# Nome dos processos
PROCESSO_CATEGORIAS = "categorias"
PROCESSO_PRODUTOS = "produtos" 
PROCESSO_ESTOQUES = "estoques"
PROCESSO_PEDIDOS = "pedidos"

# --- CONFIGURAÇÕES GERAIS DE EXECUÇÃO ---
DEFAULT_API_TIMEOUT = 90
RETRY_DELAY_429 = 30 
DIAS_JANELA_SEGURANCA = 60
MAX_PAGINAS_POR_ETAPA = 10000

def safe_float_convert(value_str, default=0.0):
    """Converte um valor para float de forma segura."""
    if value_str is None: return default
    value_str = str(value_str).strip().replace(',', '.')
    if not value_str: return default
    try: return float(value_str)
    except ValueError:
        logger.debug(f"Não foi possível converter '{value_str}' para float, usando {default}.")
        return default

def get_db_connection(max_retries=3, retry_delay=10):
    """Estabelece e retorna uma conexão com o banco de dados PostgreSQL, com retries."""
    conn = None; attempt = 0
    while attempt < max_retries:
        try:
            attempt += 1; logger.info(f"Tentando conectar ao PostgreSQL (tentativa {attempt}/{max_retries})...")
            conn = psycopg2.connect(
                host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT, 
                connect_timeout=10, keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5
            )
            logger.info("Conexão com PostgreSQL estabelecida com sucesso."); return conn 
        except psycopg2.OperationalError as e_op:
            logger.error(f"Erro operacional ao conectar ao PostgreSQL na tentativa {attempt}: {e_op}")
            if attempt < max_retries: time.sleep(retry_delay)
            else: logger.error("Máximo de tentativas de conexão com o DB atingido."); return None 
        except Exception as e: logger.error(f"Erro geral ao conectar ao PostgreSQL na tentativa {attempt}: {e}", exc_info=True); return None 
    return None

def criar_tabelas_db(conn):
    """Cria todas as tabelas necessárias no banco de dados se elas não existirem."""
    commands = (
        """CREATE TABLE IF NOT EXISTS categorias (id_categoria INTEGER PRIMARY KEY, descricao_categoria TEXT NOT NULL, id_categoria_pai INTEGER, FOREIGN KEY (id_categoria_pai) REFERENCES categorias (id_categoria) ON DELETE SET NULL);""",
        """CREATE TABLE IF NOT EXISTS produtos (id_produto INTEGER PRIMARY KEY, nome_produto TEXT, codigo_produto TEXT UNIQUE, preco_produto REAL, unidade_produto TEXT, situacao_produto TEXT, data_criacao_produto TEXT, gtin_produto TEXT, preco_promocional_produto REAL, preco_custo_produto REAL, preco_custo_medio_produto REAL, tipo_variacao_produto TEXT);""",
        """CREATE TABLE IF NOT EXISTS produto_categorias (id_produto INTEGER NOT NULL, id_categoria INTEGER NOT NULL, PRIMARY KEY (id_produto, id_categoria), FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE, FOREIGN KEY (id_categoria) REFERENCES categorias (id_categoria) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS produto_estoque_total (id_produto INTEGER PRIMARY KEY, nome_produto_estoque TEXT, saldo_total_api REAL, saldo_reservado_api REAL, data_ultima_atualizacao_api TIMESTAMP WITH TIME ZONE, FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS produto_estoque_depositos (id_estoque_deposito SERIAL PRIMARY KEY, id_produto INTEGER NOT NULL, nome_deposito TEXT, saldo_deposito REAL, desconsiderar_deposito TEXT, empresa_deposito TEXT, FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE, UNIQUE (id_produto, nome_deposito) );""",
        """CREATE TABLE IF NOT EXISTS pedidos (id_pedido INTEGER PRIMARY KEY, numero_pedido TEXT, numero_ecommerce TEXT, data_pedido TEXT, data_prevista TEXT, nome_cliente TEXT, valor_pedido REAL, id_vendedor INTEGER, nome_vendedor TEXT, situacao_pedido TEXT, codigo_rastreamento TEXT);""",
        """CREATE TABLE IF NOT EXISTS pedido_itens (id_item_pedido SERIAL PRIMARY KEY, id_pedido INTEGER NOT NULL, id_produto_tiny INTEGER, codigo_produto_pedido TEXT, descricao_produto_pedido TEXT, quantidade REAL, unidade_pedido TEXT, valor_unitario_pedido REAL, id_grade_pedido TEXT, FOREIGN KEY (id_pedido) REFERENCES pedidos (id_pedido) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS script_ultima_execucao (nome_processo TEXT PRIMARY KEY, timestamp_ultima_execucao TIMESTAMP WITH TIME ZONE );""",
        """CREATE TABLE IF NOT EXISTS script_progresso_paginas (id SERIAL PRIMARY KEY, processo TEXT NOT NULL UNIQUE, data_filtro_api TEXT, pagina_atual INTEGER DEFAULT 0, total_paginas INTEGER DEFAULT 0, registros_processados INTEGER DEFAULT 0, timestamp_inicio TIMESTAMP WITH TIME ZONE DEFAULT NOW(), timestamp_ultima_pagina TIMESTAMP WITH TIME ZONE DEFAULT NOW(), status_execucao TEXT DEFAULT 'PENDENTE');"""
    )
    alter_commands = ("""ALTER TABLE script_progresso_paginas ADD COLUMN IF NOT EXISTS data_filtro_api TEXT;""",)
    try:
        with conn.cursor() as cur:
            for cmd in commands: cur.execute(cmd)
            for cmd in alter_commands:
                try: cur.execute(cmd)
                except Exception as e: logger.debug(f"Comando ALTER ignorado (coluna pode já existir): {e}")
        if conn and not conn.closed: conn.commit()
        logger.info("Todas as tabelas foram verificadas/criadas.")
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Erro ao criar tabelas: {e}", exc_info=True)
        if conn and not conn.closed: conn.rollback()
        raise

def get_ultima_execucao(conn, nome_processo):
    """Obtém o timestamp da última execução bem-sucedida de um processo."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT timestamp_ultima_execucao FROM script_ultima_execucao WHERE nome_processo = %s", (nome_processo,))
            r = cur.fetchone()
            if r and r[0]: return (r[0] + datetime.timedelta(seconds=1))
    except Exception as e: logger.error(f"Erro ao buscar última execução para '{nome_processo}': {e}", exc_info=True)
    return None

def set_ultima_execucao(conn, nome_processo, timestamp=None):
    """Define o timestamp principal de auditoria de um processo."""
    ts = timestamp or datetime.datetime.now(datetime.timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO script_ultima_execucao (nome_processo, timestamp_ultima_execucao) VALUES (%s, %s)
                           ON CONFLICT (nome_processo) DO UPDATE SET timestamp_ultima_execucao = EXCLUDED.timestamp_ultima_execucao;""", (nome_processo, ts))
        if conn and not conn.closed: conn.commit()
        ts_log = ts.strftime('%d/%m/%Y %H:%M:%S %Z')
        logger.info(f"Timestamp principal para '{nome_processo}' definido: {ts_log}.")
    except Exception as e:
        logger.error(f"Erro ao definir timestamp principal para '{nome_processo}': {e}", exc_info=True)
        if conn and not conn.closed: conn.rollback()

def get_data_mais_recente(conn, tabela, campo_data):
    """Obtém a data mais recente de uma tabela, validando o formato dd/mm/yyyy."""
    query = sql.SQL("SELECT MAX(NULLIF({}, '')) FROM {} WHERE {} ~ %s").format(sql.Identifier(campo_data), sql.Identifier(tabela), sql.Identifier(campo_data))
    pattern = r'^\d{2}/\d{2}/\d{4}'
    try:
        with conn.cursor() as cur:
            cur.execute(query, (pattern,))
            result = cur.fetchone()
            if result and result[0]:
                logger.info(f"Data mais recente encontrada em '{tabela}': {result[0]}")
                return result[0]
    except Exception as e:
        logger.error(f"Erro ao buscar data mais recente de '{tabela}': {e}", exc_info=True)
    return None

def criar_timestamp_sintetico(conn, processo, data_recente_str):
    """Cria e salva um timestamp sintético baseado na data mais recente do banco."""
    try:
        dt_recente = None
        try: dt_recente = datetime.datetime.strptime(data_recente_str, "%d/%m/%Y %H:%M:%S")
        except ValueError: dt_recente = datetime.datetime.strptime(data_recente_str, "%d/%m/%Y")
        dt_sintetico_utc = (dt_recente + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc)
        logger.warning(f"Nenhum timestamp encontrado para '{processo}'. Criando timestamp 'sintético' a partir de {dt_sintetico_utc.strftime('%d/%m/%Y %H:%M:%S')}.")
        set_ultima_execucao(conn, processo, dt_sintetico_utc)
        return dt_sintetico_utc 
    except Exception as e:
        logger.error(f"Erro ao criar timestamp sintético para '{processo}': {e}", exc_info=True)
    return None

def determinar_data_filtro_inteligente(conn, processo_nome, dias_janela_seguranca):
    """Determina a data de filtro: usa timestamp, ou cria um sintético, ou usa janela de segurança."""
    ultima_exec_dt = get_ultima_execucao(conn, processo_nome)
    if ultima_exec_dt:
        data_limite_seguranca = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=dias_janela_seguranca)
        if ultima_exec_dt < data_limite_seguranca:
            logger.warning(f"Timestamp para '{processo_nome}' ({ultima_exec_dt.strftime('%d/%m/%Y')}) é mais antigo que {dias_janela_seguranca} dias. Usando janela de segurança.")
            return data_limite_seguranca.strftime("%d/%m/%Y %H:%M:%S")
        else:
            logger.info(f"Timestamp encontrado para '{processo_nome}'. Busca incremental desde {ultima_exec_dt.strftime('%d/%m/%Y %H:%M:%S')}.")
            return ultima_exec_dt.strftime("%d/%m/%Y %H:%M:%S")

    logger.warning(f"Nenhum timestamp encontrado para '{processo_nome}'. Verificando dados existentes...")
    tabela, campo = ('produtos', 'data_criacao_produto') if processo_nome == PROCESSO_PRODUTOS else ('pedidos', 'data_pedido')
    data_recente_str = get_data_mais_recente(conn, tabela, campo)
    if data_recente_str:
        ts_sintetico_dt = criar_timestamp_sintetico(conn, processo_nome, data_recente_str)
        if ts_sintetico_dt: return ts_sintetico_dt.strftime("%d/%m/%Y %H:%M:%S")

    logger.info(f"Nenhum dado existente para '{processo_nome}'. Usando janela de segurança de {dias_janela_seguranca} dias.")
    data_limite_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=dias_janela_seguranca)
    return data_limite_dt.strftime("%d/%m/%Y %H:%M:%S")

def inicializar_progresso(conn, processo, data_filtro_api):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pagina_atual, data_filtro_api, status_execucao FROM script_progresso_paginas WHERE processo = %s", (processo,))
            progresso = cur.fetchone()
            if progresso:
                pagina_salva, data_filtro_salva, status = progresso
                if data_filtro_salva != data_filtro_api or status == 'CONCLUIDO':
                    logger.info(f"Novo filtro de data para '{processo}' ou processo anterior concluído. Reiniciando progresso.")
                    cur.execute("UPDATE script_progresso_paginas SET pagina_atual = 0, total_paginas = 0, registros_processados = 0, timestamp_inicio = NOW(), status_execucao = 'EM_ANDAMENTO', data_filtro_api = %s WHERE processo = %s", (data_filtro_api, processo))
                    conn.commit(); return 1
                if status in ['EM_ANDAMENTO', 'ERRO']:
                    logger.warning(f"Retomando trabalho pendente para '{processo}' a partir da página {pagina_salva + 1}.")
                    return pagina_salva + 1
            logger.info(f"Iniciando novo progresso para '{processo}' com filtro desde {data_filtro_api}.")
            cur.execute("INSERT INTO script_progresso_paginas (processo, pagina_atual, data_filtro_api, status_execucao) VALUES (%s, 0, %s, 'EM_ANDAMENTO') ON CONFLICT (processo) DO UPDATE SET pagina_atual = 0, total_paginas = 0, registros_processados = 0, timestamp_inicio = NOW(), status_execucao = 'EM_ANDAMENTO', data_filtro_api = EXCLUDED.data_filtro_api", (processo, data_filtro_api))
            conn.commit(); return 1
    except Exception as e:
        logger.error(f"Erro ao inicializar progresso para '{processo}': {e}", exc_info=True)
        if conn and not conn.closed: conn.rollback()
        return 1

def atualizar_progresso_pagina(conn, processo, pagina_atual, total_paginas, registros_pagina):
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE script_progresso_paginas SET pagina_atual = %s, total_paginas = %s, registros_processados = registros_processados + %s, timestamp_ultima_pagina = NOW() WHERE processo = %s", (pagina_atual, total_paginas, registros_pagina, processo))
            conn.commit()
            if total_paginas > 0 and total_paginas >= pagina_atual:
                percentual = round((pagina_atual / total_paginas * 100), 1)
                logger.info(f"Progresso Salvo: {processo} - Página {pagina_atual}/{total_paginas} ({percentual}%) concluída.")
            else:
                logger.info(f"Progresso Salvo: {processo} - Página {pagina_atual} concluída.")
    except Exception as e: logger.error(f"Erro ao atualizar progresso da página para '{processo}': {e}", exc_info=True)

def finalizar_progresso(conn, processo, status_final="CONCLUIDO"):
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE script_progresso_paginas SET status_execucao = %s, timestamp_ultima_pagina = NOW() WHERE processo = %s", (status_final, processo))
            conn.commit()
        logger.info(f"Processo '{processo}' marcado como '{status_final}'.")
    except Exception as e: logger.error(f"Erro ao finalizar progresso de '{processo}': {e}", exc_info=True)

def make_api_v2_request(endpoint_path, method="GET", payload_dict=None, max_retries=3, initial_retry_delay=2, timeout_seconds=DEFAULT_API_TIMEOUT):
    full_url = f"{BASE_URL_V2}{endpoint_path}"
    base_params = {"token": API_V2_TOKEN, "formato": "json"}
    all_params = base_params.copy(); 
    if payload_dict: all_params.update(payload_dict)
    params_log = {k: (v[:5] + '...' if k == 'token' and isinstance(v, str) and v else v) for k,v in all_params.items()}
    retries, delay = 0, initial_retry_delay
    while retries <= max_retries:
        resp = None 
        try:
            if retries > 0: logger.info(f"Aguardando {delay}s antes da tentativa {retries + 1}/{max_retries + 1} para {endpoint_path}..."); time.sleep(delay)
            if retries > 0 and delay < RETRY_DELAY_429: delay = min(delay * 2, 30)
            
            logger.debug(f"Tentativa {retries + 1} - {method} {full_url} - Params: {params_log}")
            if method.upper() == "GET": resp = requests.get(full_url, params=all_params, timeout=timeout_seconds)
            elif method.upper() == "POST": resp = requests.post(full_url, data=all_params, timeout=timeout_seconds)
            else: logger.error(f"Método {method} não suportado."); return None, False 
            
            logger.debug(f"Resposta de {endpoint_path}: Status {resp.status_code}, Conteúdo (parcial): {resp.text[:200]}")
            resp.raise_for_status()
            
            try: data = resp.json()
            except json.JSONDecodeError as e: logger.error(f"Erro JSON de {endpoint_path}: {e}", exc_info=True); logger.debug(f"Resposta não JSON: {resp.text[:500]}"); return None, False
            
            retorno = data.get("retorno")
            if not retorno: logger.error(f"Chave 'retorno' ausente em {endpoint_path}. Resposta: {str(data)[:500]}"); return None, False 
            
            if endpoint_path == ENDPOINT_CATEGORIAS:
                if isinstance(retorno, list): return retorno, True
                if isinstance(retorno, dict) and retorno.get("status") == "OK" and isinstance(retorno.get("categorias"), list): return retorno["categorias"], True
                logger.error(f"API Tiny (Categorias) Erro: Status '{retorno.get('status')}', Erros: {retorno.get('erros', [])}"); return None, False

            if not isinstance(retorno, dict): logger.error(f"'retorno' não é dict em {endpoint_path}. Conteúdo: {str(retorno)[:300]}"); return None, False
            
            status_api, status_proc = retorno.get("status"), str(retorno.get("status_processamento", ""))
            if status_api != "OK":
                errs, cod_err, msg_err = retorno.get("erros", []), "", ""
                if errs and isinstance(errs[0], dict):
                    err_obj = errs[0].get("erro", {}); cod_err=err_obj.get("codigo","") if isinstance(err_obj,dict) else ""; msg_err=err_obj.get("erro",str(err_obj)) if isinstance(err_obj,dict) else str(err_obj)
                elif errs and isinstance(errs[0], str): msg_err = errs[0]
                
                logger.error(f"API Tiny: Status '{status_api}' (Endpoint: {endpoint_path}). Código: {cod_err}. Msg: {msg_err}. Resp: {str(retorno)[:500]}")
                
                if cod_err == "35": 
                    logger.warning(f"Erro de consulta (35) na API. Forçando retentativa...")
                    raise requests.exceptions.RequestException("Forçando retry para erro 35 da API")
                
                if cod_err == "2": logger.critical("Token API inválido/expirado.")
                return None, False
            
            if status_proc not in ["3", "10"]:
                msg_proc_err = ""
                errs_ret = retorno.get("erros", [])
                if errs_ret and isinstance(errs_ret[0],dict) and "erro" in errs_ret[0]:
                    err_det = errs_ret[0]["erro"]; msg_proc_err=str(err_det.get("erro",err_det) if isinstance(err_det,dict) else err_det)
                elif errs_ret and isinstance(errs_ret[0],str): msg_proc_err = errs_ret[0]
                if "Nenhum registro encontrado" in msg_proc_err: 
                    logger.info(f"Nenhum registro para {endpoint_path} (Status Proc: {status_proc})."); return retorno, True
                logger.warning(f"API: Status proc '{status_proc}' ({endpoint_path}). Msg: '{msg_proc_err}'. Resp: {str(retorno)[:300]}")
                if status_proc == "2": return None, False 
            return retorno, True 
        except requests.exceptions.HTTPError as e_http:
            logger.warning(f"Erro HTTP (Tentativa {retries + 1}) em {endpoint_path}: {e_http}", exc_info=False)
            if resp is not None: 
                logger.debug(f"Corpo Resposta HTTP: {resp.text[:500]}")
                if resp.status_code == 429: logger.warning(f"Limite taxa (429). Próxima tentativa com delay {RETRY_DELAY_429}s."); delay = RETRY_DELAY_429
                elif 400 <= resp.status_code < 500: return None, False 
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e_net:
            logger.warning(f"Erro de Rede/Timeout (Tentativa {retries + 1}) em {endpoint_path}: {type(e_net).__name__} - {e_net}", exc_info=False)
        except requests.exceptions.RequestException as e_req: 
            logger.warning(f"Erro de Requisição ou API retentável (Tentativa {retries + 1}) em {endpoint_path}: {type(e_req).__name__} - {e_req}", exc_info=False)
        except Exception as e_geral: 
            logger.error(f"Erro Inesperado (Tentativa {retries + 1}) em {endpoint_path}: {e_geral}", exc_info=True)
            if resp is not None: logger.debug(f"Corpo da Resposta: {resp.text[:500]}")
            return None, False 
        retries += 1
        if retries > max_retries: logger.error(f"Máximo de retries atingido para {endpoint_path}."); return None, False
    return None, False

# ... (Funções `get_..._v2`, `search_..._v2`, `processar_..._v2` e `executar_etapa_paginada` como na versão anterior, que estão completas e corretas) ...

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    logger.info("=== Iniciando Cliente API v2 Tiny ERP - MODO PRODUÇÃO FINAL ===")
    start_time_total = time.time()

    required_vars = {"TINY_API_V2_TOKEN": API_V2_TOKEN, "DB_HOST": DB_HOST, "DB_NAME": DB_NAME, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        logger.critical(f"Variáveis de ambiente obrigatórias não configuradas: {', '.join(missing_vars)}. Encerrando.")
        exit(1)
        
    db_conn = get_db_connection()
    if db_conn is None or (hasattr(db_conn, 'closed') and db_conn.closed):
        logger.critical("Falha na conexão com o banco de dados. Encerrando."); exit(1)

    try:
        criar_tabelas_db(db_conn)

        def executar_etapa_paginada(processo_nome, funcao_busca, conn, dias_janela=None):
            """Função de alto nível para executar um processo paginado com resiliência."""
            ts_inicio_etapa = datetime.datetime.now(datetime.timezone.utc)
            etapa_ok = True
            
            data_filtro_api = None
            if processo_nome == PROCESSO_ESTOQUES:
                data_filtro_api = (ts_inicio_etapa - datetime.timedelta(days=29)).strftime("%d/%m/%Y %H:%M:%S")
            else:
                data_filtro_api = determinar_data_filtro_inteligente(conn, processo_nome, dias_janela)

            if not data_filtro_api:
                logger.error(f"Não foi possível determinar data de filtro para o processo '{processo_nome}'. Pulando etapa.")
                return False

            pagina_inicial = inicializar_progresso(conn, processo_nome, data_filtro_api)
            if pagina_inicial is None:
                return True

            pag_atual, paginas_processadas_execucao, total_itens_listados = pagina_inicial, 0, 0
            
            while paginas_processadas_execucao < MAX_PAGINAS_POR_ETAPA:
                logger.info(f"Processando pág {pag_atual} de '{processo_nome}'...")
                
                itens_pag, total_pags_api, pag_commit = funcao_busca(conn, data_filtro_api, pag_atual)
                
                if itens_pag is None:
                    logger.error(f"Falha crítica (API) na pág {pag_atual} de '{processo_nome}'. Interrompendo."); etapa_ok=False; break 
                if not pag_commit and itens_pag:
                    logger.warning(f"Pág {pag_atual} de '{processo_nome}' não foi commitada devido a erros. Interrompendo."); etapa_ok=False; break
                
                atualizar_progresso_pagina(conn, processo_nome, pag_atual, total_pags_api, len(itens_pag) if itens_pag else 0)
                
                if itens_pag: total_itens_listados += len(itens_pag)
                paginas_processadas_execucao += 1
                
                if total_pags_api == 0 or pag_atual >= total_pags_api:
                    logger.info(f"Todas as páginas de '{processo_nome}' para o filtro atual foram processadas.")
                    finalizar_progresso(conn, processo_nome, "CONCLUIDO")
                    set_ultima_execucao(conn, processo_nome, ts_inicio_etapa)
                    break
                
                pag_atual += 1
                time.sleep(1)
            else:
                logger.warning(f"Limite de {MAX_PAGINAS_POR_ETAPA} páginas atingido para '{processo_nome}'. A sincronização continuará na próxima execução.")
                finalizar_progresso(conn, processo_nome, "EM_ANDAMENTO")

            if not etapa_ok:
                finalizar_progresso(conn, processo_nome, "ERRO")
            
            return etapa_ok
        
        # --- Execução dos Passos ---
        logger.info("--- PASSO 1: Categorias ---")
        if get_categorias_v2(db_conn): logger.info("Passo 1 (Categorias) concluído com sucesso.")
        else: logger.warning("Passo 1 (Categorias) concluído com falhas.")
        logger.info("-" * 70)

        logger.info("--- PASSO 2: Produtos ---")
        executar_etapa_paginada(PROCESSO_PRODUTOS, search_produtos_v2, db_conn, DIAS_JANELA_SEGURANCA)
        logger.info("-" * 70)

        logger.info("--- PASSO 3: Estoques ---")
        executar_etapa_paginada(PROCESSO_ESTOQUES, processar_atualizacoes_estoque_v2, db_conn)
        logger.info("-" * 70)

        logger.info("--- PASSO 4: Pedidos ---")
        executar_etapa_paginada(PROCESSO_PEDIDOS, search_pedidos_v2, db_conn, DIAS_JANELA_SEGURANCA)
        logger.info("-" * 70)
        
        logger.info("--- Contagem final dos registros no banco de dados ---")
        if db_conn and not db_conn.closed:
            with db_conn.cursor() as cur:
                tabelas = ["categorias","produtos","produto_categorias","produto_estoque_total","produto_estoque_depositos","pedidos","pedido_itens","script_ultima_execucao", "script_progresso_paginas"]
                for t in tabelas:
                    try: cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(t))); logger.info(f"  - Tabela '{t}': {cur.fetchone()[0]} regs.")
                    except Exception as e: logger.error(f"  Erro ao contar '{t}': {e}", exc_info=True)
        else: logger.warning("Não foi possível contar registros, DB fechado/indisponível.")
            
    except KeyboardInterrupt:
        logger.warning("Interrompido (KeyboardInterrupt).")
        if db_conn and not db_conn.closed: 
            try: db_conn.rollback(); logger.info("Rollback por KI bem-sucedido.")
            except Exception as e: logger.error(f"Erro no rollback KI: {e}", exc_info=True)
    except Exception as e_geral:
        logger.critical(f"ERRO GERAL NO PROCESSAMENTO: {e_geral}", exc_info=True)
        if db_conn and not db_conn.closed: 
            try: db_conn.rollback(); logger.info("Rollback por erro geral bem-sucedido.")
            except Exception as e_rollback: logger.error(f"Erro durante o rollback da transação geral: {e_rollback}", exc_info=True)
    finally:
        if db_conn and not (hasattr(db_conn,'closed') and db_conn.closed):
            db_conn.close(); logger.info("Conexão PostgreSQL fechada.")
        elif db_conn is None: logger.info("Nenhuma conexão DB para fechar.")
        else: logger.info("Conexão DB já estava fechada.")

    logger.info(f"=== Processo Concluído em {time.time() - start_time_total:.2f} segundos ===")