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

# --- CONFIGURAÇÕES GERAIS DE EXECUÇÃO ---
DEFAULT_API_TIMEOUT = 90
RETRY_DELAY_429 = 30 
DIAS_JANELA_SEGURANCA = 60  # Janela unificada para produtos e pedidos
MAX_PAGINAS_POR_ETAPA = 500  # Limite seguro para execução diária

def safe_float_convert(value_str, default=0.0):
    if value_str is None: return default
    value_str = str(value_str).strip().replace(',', '.')
    if not value_str: return default
    try: return float(value_str)
    except ValueError:
        logger.debug(f"Não foi possível converter '{value_str}' para float, usando {default}.")
        return default

def get_db_connection(max_retries=3, retry_delay=10):
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
    commands = (
        """CREATE TABLE IF NOT EXISTS categorias (id_categoria INTEGER PRIMARY KEY, descricao_categoria TEXT NOT NULL, id_categoria_pai INTEGER, FOREIGN KEY (id_categoria_pai) REFERENCES categorias (id_categoria) ON DELETE SET NULL);""",
        """CREATE TABLE IF NOT EXISTS produtos (id_produto INTEGER PRIMARY KEY, nome_produto TEXT, codigo_produto TEXT UNIQUE, preco_produto REAL, unidade_produto TEXT, situacao_produto TEXT, data_criacao_produto TEXT, gtin_produto TEXT, preco_promocional_produto REAL, preco_custo_produto REAL, preco_custo_medio_produto REAL, tipo_variacao_produto TEXT);""",
        """CREATE TABLE IF NOT EXISTS produto_categorias (id_produto INTEGER NOT NULL, id_categoria INTEGER NOT NULL, PRIMARY KEY (id_produto, id_categoria), FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE, FOREIGN KEY (id_categoria) REFERENCES categorias (id_categoria) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS produto_estoque_total (id_produto INTEGER PRIMARY KEY, nome_produto_estoque TEXT, saldo_total_api REAL, saldo_reservado_api REAL, data_ultima_atualizacao_api TIMESTAMP WITH TIME ZONE, FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS produto_estoque_depositos (id_estoque_deposito SERIAL PRIMARY KEY, id_produto INTEGER NOT NULL, nome_deposito TEXT, saldo_deposito REAL, desconsiderar_deposito TEXT, empresa_deposito TEXT, FOREIGN KEY (id_produto) REFERENCES produtos (id_produto) ON DELETE CASCADE, UNIQUE (id_produto, nome_deposito) );""",
        """CREATE TABLE IF NOT EXISTS pedidos (id_pedido INTEGER PRIMARY KEY, numero_pedido TEXT, numero_ecommerce TEXT, data_pedido TEXT, data_prevista TEXT, nome_cliente TEXT, valor_pedido REAL, id_vendedor INTEGER, nome_vendedor TEXT, situacao_pedido TEXT, codigo_rastreamento TEXT);""",
        """CREATE TABLE IF NOT EXISTS pedido_itens (id_item_pedido SERIAL PRIMARY KEY, id_pedido INTEGER NOT NULL, id_produto_tiny INTEGER, codigo_produto_pedido TEXT, descricao_produto_pedido TEXT, quantidade REAL, unidade_pedido TEXT, valor_unitario_pedido REAL, id_grade_pedido TEXT, FOREIGN KEY (id_pedido) REFERENCES pedidos (id_pedido) ON DELETE CASCADE);""",
        """CREATE TABLE IF NOT EXISTS script_ultima_execucao (nome_processo TEXT PRIMARY KEY, timestamp_ultima_execucao TIMESTAMP WITH TIME ZONE );""",
        """CREATE TABLE IF NOT EXISTS script_progresso_paginas (
            id_progresso SERIAL PRIMARY KEY,
            processo TEXT NOT NULL,
            data_filtro TEXT NOT NULL,
            pagina_atual INTEGER NOT NULL,
            total_paginas INTEGER,
            registros_processados INTEGER DEFAULT 0,
            timestamp_inicio TIMESTAMP WITH TIME ZONE,
            timestamp_ultima_pagina TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            status_execucao TEXT DEFAULT 'EM_ANDAMENTO',
            observacoes TEXT,
            UNIQUE (processo, data_filtro)
        );""",
        """CREATE INDEX IF NOT EXISTS idx_progresso_processo_data ON script_progresso_paginas (processo, data_filtro);""",
        """CREATE INDEX IF NOT EXISTS idx_progresso_status ON script_progresso_paginas (status_execucao);"""
    )
    try:
        with conn.cursor() as cur:
            for cmd in commands: cur.execute(cmd)
        if conn and not conn.closed: conn.commit()
        logger.info("Todas as tabelas foram verificadas/criadas.")
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Erro ao criar tabelas: {e}", exc_info=True)
        if conn and not conn.closed: conn.rollback()
        raise

def get_ultima_execucao(conn, nome_processo):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT timestamp_ultima_execucao FROM script_ultima_execucao WHERE nome_processo = %s", (nome_processo,))
            r = cur.fetchone()
            if r and r[0]: return (r[0] + datetime.timedelta(seconds=1))
    except Exception as e: logger.error(f"Erro ao buscar última execução para '{nome_processo}': {e}", exc_info=True)
    return None

def set_ultima_execucao(conn, nome_processo, timestamp=None):
    ts = timestamp or datetime.datetime.now(datetime.timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO script_ultima_execucao (nome_processo, timestamp_ultima_execucao) VALUES (%s, %s)
                           ON CONFLICT (nome_processo) DO UPDATE SET timestamp_ultima_execucao = EXCLUDED.timestamp_ultima_execucao;""", (nome_processo, ts))
        if conn and not conn.closed: conn.commit()
        ts_log = ts.strftime('%d/%m/%Y %H:%M:%S %Z')
        logger.info(f"Timestamp para '{nome_processo}' definido: {ts_log}.")
    except Exception as e:
        logger.error(f"Erro ao definir última execução para '{nome_processo}': {e}", exc_info=True)
        if conn and not conn.closed: conn.rollback()

def get_data_mais_recente(conn, tabela, campo_data):
    """Obtém a data mais recente de uma tabela, validando o formato."""
    query = sql.SQL("SELECT MAX({}) FROM {} WHERE {} ~ %s").format(
        sql.Identifier(campo_data), sql.Identifier(tabela), sql.Identifier(campo_data)
    )
    pattern = r'^\d{2}/\d{2}/\d{4}$' # Regex para dd/mm/yyyy
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
        dt_recente = datetime.datetime.strptime(data_recente_str, "%d/%m/%Y")
        dt_sintetico = dt_recente.replace(hour=0, minute=0, second=0) + datetime.timedelta(days=1)
        dt_sintetico_utc = dt_sintetico.replace(tzinfo=datetime.timezone.utc)
        
        logger.warning(f"Nenhum timestamp encontrado para '{processo}', mas dados existentes foram detectados.")
        logger.warning(f"Criando e salvando timestamp 'sintético' para iniciar a busca a partir de {dt_sintetico.strftime('%d/%m/%Y %H:%M:%S')}.")
        
        set_ultima_execucao(conn, processo, dt_sintetico_utc)
        return dt_sintetico # Retorna objeto datetime
    except Exception as e:
        logger.error(f"Erro ao criar timestamp sintético para '{processo}': {e}", exc_info=True)
    return None

def determinar_data_filtro_inteligente(conn, processo_nome, dias_janela_seguranca):
    """Determina a data de filtro: usa timestamp, ou cria um sintético, ou usa janela de segurança."""
    ultima_exec_dt = get_ultima_execucao(conn, processo_nome)
    if ultima_exec_dt:
        logger.info(f"Timestamp encontrado para '{processo_nome}'. Busca incremental desde {ultima_exec_dt.strftime('%d/%m/%Y %H:%M:%S')}.")
        return ultima_exec_dt.strftime("%d/%m/%Y %H:%M:%S")

    logger.warning(f"Nenhum timestamp encontrado para '{processo_nome}'. Verificando dados existentes...")
    data_recente_str = None
    if processo_nome == PROCESSO_PRODUTOS:
        data_recente_str = get_data_mais_recente(conn, 'produtos', 'data_criacao_produto')
    elif processo_nome == PROCESSO_PEDIDOS:
        data_recente_str = get_data_mais_recente(conn, 'pedidos', 'data_pedido')

    if data_recente_str:
        ts_sintetico = criar_timestamp_sintetico(conn, processo_nome, data_recente_str)
        if ts_sintetico:
            return ts_sintetico.strftime("%d/%m/%Y %H:%M:%S")

    logger.info(f"Nenhum dado existente para '{processo_nome}'. Usando janela de segurança de {dias_janela_seguranca} dias.")
    data_limite_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=dias_janela_seguranca)
    return data_limite_dt.strftime("%d/%m/%Y %H:%M:%S")

# --- FUNÇÕES DE CONTROLE GRANULAR DE PROGRESSO ---

def inicializar_progresso(conn, processo, data_filtro):
    """Inicializa ou recupera o progresso de um processo."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pagina_atual, total_paginas, registros_processados, status_execucao
                FROM script_progresso_paginas 
                WHERE processo = %s AND data_filtro = %s
            """, (processo, data_filtro))
            
            result = cur.fetchone()
            if result:
                pagina_atual, total_paginas, registros_proc, status = result
                if status == 'CONCLUIDO':
                    logger.info(f"✅ Processo {processo} já foi concluído para {data_filtro}")
                    return None, None, True  # Já concluído
                else:
                    proxima_pagina = pagina_atual + 1
                    logger.warning(f"🔄 RECUPERANDO PROGRESSO: {processo} estava na página {pagina_atual}")
                    logger.warning(f"▶️ CONTINUANDO da página {proxima_pagina} (já processados: {registros_proc} registros)")
                    return proxima_pagina, total_paginas, False  # Continuar de onde parou
            else:
                # Criar novo registro de progresso
                cur.execute("""
                    INSERT INTO script_progresso_paginas 
                    (processo, data_filtro, pagina_atual, timestamp_inicio, status_execucao)
                    VALUES (%s, %s, 0, NOW(), 'EM_ANDAMENTO')
                """, (processo, data_filtro))
                conn.commit()
                logger.info(f"🆕 INICIANDO novo progresso para {processo} desde {data_filtro}")
                return 1, None, False  # Começar do início
                
    except Exception as e:
        logger.error(f"Erro ao inicializar progresso: {e}", exc_info=True)
        return 1, None, False

def atualizar_progresso_pagina(conn, processo, data_filtro, pagina_atual, total_paginas, registros_pagina):
    """Atualiza o progresso após processar uma página."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE script_progresso_paginas 
                SET pagina_atual = %s,
                    total_paginas = %s,
                    registros_processados = registros_processados + %s,
                    timestamp_ultima_pagina = NOW(),
                    observacoes = %s
                WHERE processo = %s AND data_filtro = %s
            """, (
                pagina_atual, 
                total_paginas, 
                registros_pagina,
                f"✅ Página {pagina_atual}/{total_paginas or '?'} - {registros_pagina} registros",
                processo, 
                data_filtro
            ))
            conn.commit()
            
            # Log de progresso
            if total_paginas:
                percentual = (pagina_atual / total_paginas) * 100
                logger.info(f"📊 PROGRESSO SALVO: {processo} - {pagina_atual}/{total_paginas} ({percentual:.1f}%) - {registros_pagina} registros")
            else:
                logger.info(f"📊 PROGRESSO SALVO: {processo} - página {pagina_atual} - {registros_pagina} registros")
                
    except Exception as e:
        logger.error(f"Erro ao atualizar progresso: {e}", exc_info=True)

def finalizar_progresso(conn, processo, data_filtro, sucesso=True):
    """Marca o progresso como concluído ou com erro."""
    try:
        status = 'CONCLUIDO' if sucesso else 'ERRO'
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE script_progresso_paginas 
                SET status_execucao = %s,
                    timestamp_ultima_pagina = NOW(),
                    observacoes = %s
                WHERE processo = %s AND data_filtro = %s
            """, (
                status,
                f"🏁 Processo finalizado: {'✅ SUCESSO' if sucesso else '❌ ERRO'}",
                processo, 
                data_filtro
            ))
            conn.commit()
            logger.info(f"🏁 PROGRESSO FINALIZADO: {processo} - {status}")
            
    except Exception as e:
        logger.error(f"Erro ao finalizar progresso: {e}", exc_info=True)

def limpar_progresso_antigo(conn, dias_manter=7):
    """Remove registros de progresso antigos."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM script_progresso_paginas 
                WHERE timestamp_ultima_pagina < NOW() - INTERVAL '%s days'
                AND status_execucao IN ('CONCLUIDO', 'ERRO')
            """, (dias_manter,))
            deletados = cur.rowcount
            conn.commit()
            if deletados > 0:
                logger.info(f"🧹 Limpeza: {deletados} registros de progresso antigos removidos")
                
    except Exception as e:
        logger.error(f"Erro na limpeza de progresso: {e}", exc_info=True)

# --- FUNÇÕES DE SALVAMENTO NO BANCO ---

def salvar_categoria_db(conn, categoria_dict, id_pai=None):
    cat_id_str = categoria_dict.get("id"); cat_desc = categoria_dict.get("descricao")
    if cat_id_str and cat_desc: 
        try:
            cat_id = int(cat_id_str) 
            with conn.cursor() as cur:
                cur.execute("INSERT INTO categorias (id_categoria, descricao_categoria, id_categoria_pai) VALUES (%s, %s, %s) ON CONFLICT (id_categoria) DO UPDATE SET descricao_categoria = EXCLUDED.descricao_categoria, id_categoria_pai = EXCLUDED.id_categoria_pai;", (cat_id, cat_desc, id_pai))
            if "nodes" in categoria_dict and isinstance(categoria_dict["nodes"], list):
                for sub_cat in categoria_dict["nodes"]: salvar_categoria_db(conn, sub_cat, id_pai=cat_id) 
        except ValueError: logger.warning(f"ID da categoria inválido '{cat_id_str}'. Categoria: {categoria_dict}", exc_info=False)
        except Exception as e: logger.error(f"Erro PostgreSQL ao salvar categoria ID '{cat_id_str}': {e}", exc_info=True)

def salvar_produto_db(conn, produto_api_data):
    id_prod_str = str(produto_api_data.get("id","")).strip()
    if not id_prod_str or not id_prod_str.isdigit(): raise ValueError(f"ID do produto inválido: {id_prod_str}")
    id_prod = int(id_prod_str)
    try:
        dados = { "id_produto": id_prod, "nome_produto": produto_api_data.get("nome"), "codigo_produto": str(p_api_cod).strip() if (p_api_cod := produto_api_data.get("codigo")) and str(p_api_cod).strip() else None, "preco_produto": safe_float_convert(produto_api_data.get("preco")), "unidade_produto": produto_api_data.get("unidade"), "situacao_produto": produto_api_data.get("situacao"), "data_criacao_produto": produto_api_data.get("data_criacao"), "gtin_produto": produto_api_data.get("gtin"), "preco_promocional_produto": safe_float_convert(produto_api_data.get("preco_promocional")), "preco_custo_produto": safe_float_convert(produto_api_data.get("preco_custo")), "preco_custo_medio_produto": safe_float_convert(produto_api_data.get("preco_custo_medio")), "tipo_variacao_produto": produto_api_data.get("tipoVariacao") }
        with conn.cursor() as cur:
            if dados["codigo_produto"] is not None:
                cur.execute("SELECT id_produto FROM produtos WHERE codigo_produto = %s AND id_produto != %s", (dados["codigo_produto"], id_prod))
                if cur.fetchone(): logger.warning(f"SKU '{dados['codigo_produto']}' em uso. Produto ID {id_prod} terá SKU NULL."); dados["codigo_produto"] = None
            cols = ", ".join(dados.keys()); vals_template = ", ".join(["%s"] * len(dados))
            updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in dados.keys() if col != "id_produto"])
            query = f"INSERT INTO produtos ({cols}) VALUES ({vals_template}) ON CONFLICT (id_produto) DO UPDATE SET {updates};"
            cur.execute(query, tuple(dados.values()))
        logger.debug(f"Produto ID {id_prod} salvo/atualizado.")
    except Exception as e: logger.error(f"Erro ao salvar produto ID '{id_prod_str}': {e}", exc_info=True); raise

def salvar_produto_categorias_db(conn, id_produto, categorias_lista_api):
    if not isinstance(id_produto, int): raise ValueError(f"ID do produto inválido: {id_produto}")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_categorias WHERE id_produto = %s", (id_produto,))
            if categorias_lista_api and isinstance(categorias_lista_api, list):
                dados = [(id_produto, int(c.get("id"))) for c in categorias_lista_api if isinstance(c,dict) and str(c.get("id","")).strip().isdigit()]
                if dados: execute_values(cur, "INSERT INTO produto_categorias (id_produto, id_categoria) VALUES %s ON CONFLICT DO NOTHING", dados); logger.debug(f"{len(dados)} categorias salvas para produto ID {id_produto}.")
    except Exception as e: logger.error(f"Erro ao salvar categorias do produto ID {id_produto}: {e}", exc_info=True); raise

def salvar_produto_estoque_total_db(conn, id_produto, nome_produto, saldo_total_api, saldo_reservado_api, data_ultima_atualizacao_estoque=None):
    id_p = int(id_produto)
    try:
        st = safe_float_convert(saldo_total_api); sr = safe_float_convert(saldo_reservado_api); dt_att = None
        if data_ultima_atualizacao_estoque is None: dt_att = datetime.datetime.now(datetime.timezone.utc)
        elif isinstance(data_ultima_atualizacao_estoque, str):
            try: dt_att = datetime.datetime.strptime(data_ultima_atualizacao_estoque, "%d/%m/%Y %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
            except ValueError: dt_att = datetime.datetime.now(datetime.timezone.utc); logger.warning(f"Data de estoque inválida '{data_ultima_atualizacao_estoque}', usando atual.")
        elif isinstance(data_ultima_atualizacao_estoque, datetime.datetime):
            dt_att = data_ultima_atualizacao_estoque if data_ultima_atualizacao_estoque.tzinfo else data_ultima_atualizacao_estoque.replace(tzinfo=datetime.timezone.utc)
        else: dt_att = datetime.datetime.now(datetime.timezone.utc); logger.warning(f"Tipo de data de estoque inesperado, usando atual.")
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO produto_estoque_total (id_produto, nome_produto_estoque, saldo_total_api, saldo_reservado_api, data_ultima_atualizacao_api)
                           VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id_produto) DO UPDATE SET nome_produto_estoque=EXCLUDED.nome_produto_estoque,
                           saldo_total_api=EXCLUDED.saldo_total_api, saldo_reservado_api=EXCLUDED.saldo_reservado_api, data_ultima_atualizacao_api=EXCLUDED.data_ultima_atualizacao_api;""",
                        (id_p, nome_produto, st, sr, dt_att))
        logger.debug(f"Estoque total para produto ID {id_p} salvo.")
    except Exception as e: logger.error(f"Erro ao salvar estoque total para Produto ID {id_produto}: {e}", exc_info=True); raise

def salvar_estoque_por_deposito_db(conn, id_produto, nome_produto, lista_depositos_api):
    id_p = int(id_produto)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_estoque_depositos WHERE id_produto = %s", (id_p,))
            if not lista_depositos_api or not isinstance(lista_depositos_api, list): return
            dados = []
            for dep_w in lista_depositos_api:
                dep_d = dep_w.get("deposito") or (dep_w if isinstance(dep_w, dict) else None)
                if dep_d and isinstance(dep_d, dict): dados.append((id_p, dep_d.get("nome"), safe_float_convert(dep_d.get("saldo")), dep_d.get("desconsiderar"), dep_d.get("empresa")))
            if dados:
                execute_values(cur, """INSERT INTO produto_estoque_depositos (id_produto, nome_deposito, saldo_deposito, desconsiderar_deposito, empresa_deposito) 
                                       VALUES %s ON CONFLICT (id_produto, nome_deposito) DO UPDATE SET saldo_deposito=EXCLUDED.saldo_deposito,
                                       desconsiderar_deposito=EXCLUDED.desconsiderar_deposito, empresa_deposito=EXCLUDED.empresa_deposito;""", dados)
                logger.debug(f"{len(dados)} depósitos salvos para produto ID {id_p}.")
    except Exception as e: logger.error(f"Erro ao salvar estoque por depósito para Produto ID {id_produto}: {e}", exc_info=True); raise

def salvar_pedido_db(conn, pedido_api_data):
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

# --- FUNÇÕES DE API COM RETRY MELHORADO ---

def make_api_v2_request_com_retry_35(endpoint_path, method="GET", payload_dict=None, 
                                    max_retries=3, max_retries_35=3, initial_retry_delay=2, timeout_seconds=DEFAULT_API_TIMEOUT):
    """Versão melhorada com retry específico para erro 35."""
    
    retries_35 = 0
    
    while retries_35 <= max_retries_35:
        ret, suc = make_api_v2_request(endpoint_path, method, payload_dict, max_retries, initial_retry_delay, timeout_seconds)
        
        if suc:
            if retries_35 > 0:
                logger.info(f"✅ SUCESSO após {retries_35} tentativas para erro 35")
            return ret, suc
        
        # Verificar se é erro 35 específico
        if ret and isinstance(ret, dict):
            erros = ret.get("erros", [])
            if erros and isinstance(erros[0], dict):
                erro_obj = erros[0].get("erro", {})
                if "Ocorreu um erro ao executar a consulta" in str(erro_obj):
                    retries_35 += 1
                    if retries_35 <= max_retries_35:
                        delay = 30 + (retries_35 * 15)  # 30s, 45s, 60s
                        logger.warning(f"⚠️ ERRO 35 detectado (tentativa {retries_35}/{max_retries_35})")
                        logger.warning(f"⏳ Aguardando {delay}s antes de tentar novamente...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"❌ ERRO 35 persistente após {max_retries_35} tentativas")
                        return None, False
        
        # Se não é erro 35, retornar falha imediatamente
        return ret, suc
    
    return None, False

def make_api_v2_request(endpoint_path, method="GET", payload_dict=None, 
                        max_retries=3, initial_retry_delay=2, timeout_seconds=DEFAULT_API_TIMEOUT):
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

# --- FUNÇÕES DE PROCESSAMENTO ---

def get_categorias_v2(conn):
    """Busca todas as categorias da API e as salva no banco."""
    logger.info("Iniciando Categorias.")
    lista_cats, suc = make_api_v2_request(ENDPOINT_CATEGORIAS)
    if suc and isinstance(lista_cats, list):
        if not lista_cats: logger.info("Nenhuma categoria da API."); set_ultima_execucao(conn, PROCESSO_CATEGORIAS); return True
        ok, n_raiz = True, 0
        for cat_r in lista_cats:
            if isinstance(cat_r, dict):
                try: salvar_categoria_db(conn, cat_r); n_raiz+=1
                except Exception as e: logger.error(f"Erro salvar cat ID '{cat_r.get('id')}': {e}",True); ok=False
            else: logger.warning(f"Item cat raiz inesperado: {cat_r}")
        if ok:
            try:
                if conn and not conn.closed: conn.commit(); logger.info(f"{n_raiz} cats raiz commitadas.")
                set_ultima_execucao(conn, PROCESSO_CATEGORIAS); return True
            except Exception as e: logger.error(f"Erro commit categorias: {e}",True);
        if conn and not conn.closed: conn.rollback(); logger.warning("Rollback de categorias devido a erros.")
        return False
    logger.error(f"Falha buscar/salvar categorias. API ok: {suc}."); 
    if lista_cats is not None: logger.debug(f"Dados cats (parcial): {str(lista_cats)[:300]}")
    return False

def get_produto_detalhes_v2(id_produto_tiny):
    logger.debug(f"Buscando detalhes produto ID {id_produto_tiny}...")
    ret, suc = make_api_v2_request_com_retry_35(ENDPOINT_PRODUTO_OBTER, payload_dict={"id": id_produto_tiny})
    if suc and ret and isinstance(ret.get("produto"), dict): return ret["produto"]
    logger.warning(f"Produto ID {id_produto_tiny} sem detalhes API ou erro.")
    return None

def search_produtos_v2_com_controle(conn, data_alteracao_inicial=None):
    """Versão com controle granular de progresso para produtos."""
    
    data_filtro_id = data_alteracao_inicial or "sem_filtro"
    
    # Inicializar ou recuperar progresso
    pagina_inicial, total_paginas_conhecido, ja_concluido = inicializar_progresso(
        conn, PROCESSO_PRODUTOS, data_filtro_id
    )
    
    if ja_concluido:
        logger.info("✅ Processo de produtos já foi concluído para este período")
        return [], 0, True
    
    pagina_atual = pagina_inicial
    total_produtos_processados = 0
    
    logger.info(f"🚀 INICIANDO busca de produtos desde página {pagina_atual}")
    
    try:
        while pagina_atual <= MAX_PAGINAS_POR_ETAPA:
            logger.info(f"📄 Processando página {pagina_atual} de produtos...")
            
            params = {"pagina": pagina_atual}
            if data_alteracao_inicial: params["dataAlteracaoInicial"] = data_alteracao_inicial
            
            ret_api, suc_api = make_api_v2_request_com_retry_35(ENDPOINT_PRODUTOS_PESQUISA, payload_dict=params)
            
            if not suc_api:
                logger.error(f"❌ FALHA DEFINITIVA na API - página {pagina_atual}")
                if pagina_atual > pagina_inicial:
                    atualizar_progresso_pagina(conn, PROCESSO_PRODUTOS, data_filtro_id, pagina_atual - 1, 
                                             total_paginas_conhecido or 0, 0)
                finalizar_progresso(conn, PROCESSO_PRODUTOS, data_filtro_id, sucesso=False)
                return None, 0, False
            
            prods_pag_api = ret_api.get("produtos", []) if ret_api else []
            num_pags_tot = int(ret_api.get('numero_paginas', 0)) if ret_api else 0
            
            produtos_salvos = 0
            if prods_pag_api and isinstance(prods_pag_api, list):
                for prod_w in prods_pag_api:
                    prod_d = prod_w.get("produto")
                    if not prod_d or not isinstance(prod_d, dict): 
                        logger.warning(f"Item prod malformado (página {pagina_atual}): {prod_w}")
                        continue
                    
                    try:
                        id_prod_str = str(prod_d.get("id","")).strip()
                        id_prod_int = int(id_prod_str)
                        
                        salvar_produto_db(conn, prod_d)
                        time.sleep(0.5)
                        
                        det_prod = get_produto_detalhes_v2(id_prod_int)
                        if det_prod and "categorias" in det_prod:
                            salvar_produto_categorias_db(conn, id_prod_int, det_prod["categorias"])
                        
                        produtos_salvos += 1
                        
                    except Exception as e:
                        logger.error(f"❌ Erro no produto ID '{id_prod_str}' (página {pagina_atual}): {e}")
                        continue
                
                # Commit da página
                conn.commit()
                total_produtos_processados += produtos_salvos
                
                # Salvar progresso a cada página
                atualizar_progresso_pagina(
                    conn, PROCESSO_PRODUTOS, data_filtro_id, pagina_atual, num_pags_tot, produtos_salvos
                )
                
                logger.info(f"✅ Página {pagina_atual} CONCLUÍDA: {produtos_salvos} produtos salvos")
            
            # Verificar se terminou
            if num_pags_tot == 0 or pagina_atual >= num_pags_tot:
                logger.info("🏁 Todas as páginas de produtos foram processadas")
                break
            
            pagina_atual += 1
            time.sleep(1)
        
        # Finalizar com sucesso
        finalizar_progresso(conn, PROCESSO_PRODUTOS, data_filtro_id, sucesso=True)
        logger.info(f"🎉 PROCESSO DE PRODUTOS CONCLUÍDO: {total_produtos_processados} produtos processados")
        
        return prods_pag_api, num_pags_tot, True
        
    except Exception as e:
        logger.error(f"❌ Erro geral no processamento de produtos: {e}", exc_info=True)
        if pagina_atual > pagina_inicial:
            atualizar_progresso_pagina(conn, PROCESSO_PRODUTOS, data_filtro_id, pagina_atual - 1, 
                                     total_paginas_conhecido or 0, 0)
        finalizar_progresso(conn, PROCESSO_PRODUTOS, data_filtro_id, sucesso=False)
        return None, 0, False

def search_pedidos_v2_com_controle(conn, data_alteracao_inicial=None):
    """Versão resiliente com controle granular e recuperação automática para pedidos."""
    
    data_filtro_id = data_alteracao_inicial or "sem_filtro"
    
    # Inicializar ou recuperar progresso
    pagina_inicial, total_paginas_conhecido, ja_concluido = inicializar_progresso(
        conn, PROCESSO_PEDIDOS, data_filtro_id
    )
    
    if ja_concluido:
        logger.info("✅ Processo de pedidos já foi concluído para este período")
        return [], 0, True
    
    pagina_atual = pagina_inicial
    total_pedidos_processados = 0
    
    logger.info(f"🚀 INICIANDO busca de pedidos desde página {pagina_atual}")
    
    try:
        while pagina_atual <= MAX_PAGINAS_POR_ETAPA:
            logger.info(f"📄 Processando página {pagina_atual} de pedidos...")
            
            # Parâmetros da API
            params_api = {"pagina": pagina_atual}
            if data_alteracao_inicial:
                params_api["dataAlteracaoInicial"] = data_alteracao_inicial
            
            # Fazer requisição com retry automático para erro 35
            ret_api, suc_api = make_api_v2_request_com_retry_35(
                ENDPOINT_PEDIDOS_PESQUISA, 
                payload_dict=params_api,
                max_retries_35=3  # Retry específico para erro 35
            )
            
            if not suc_api:
                logger.error(f"❌ FALHA DEFINITIVA na API - página {pagina_atual}")
                # Salvar progresso antes de parar
                if pagina_atual > pagina_inicial:
                    atualizar_progresso_pagina(conn, PROCESSO_PEDIDOS, data_filtro_id, pagina_atual - 1, 
                                             total_paginas_conhecido or 0, 0)
                finalizar_progresso(conn, PROCESSO_PEDIDOS, data_filtro_id, sucesso=False)
                return None, 0, False
            
            peds_pag = ret_api.get("pedidos", []) if ret_api else []
            total_paginas = int(ret_api.get('numero_paginas', 0)) if ret_api else 0
            
            # Processar pedidos da página
            pedidos_salvos = 0
            if peds_pag and isinstance(peds_pag, list):
                for ped_w in peds_pag:
                    ped_d = ped_w.get("pedido")
                    if not ped_d or not isinstance(ped_d, dict):
                        continue
                    
                    try:
                        id_ped_str = str(ped_d.get("id", "")).strip()
                        id_ped_int = int(id_ped_str)
                        
                        # Salvar pedido
                        salvar_pedido_db(conn, ped_d)
                        time.sleep(0.6)
                        
                        # Buscar e salvar detalhes/itens
                        det_ped = get_detalhes_pedido_v2(id_ped_int)
                        if det_ped and "itens" in det_ped:
                            salvar_pedido_itens_db(conn, id_ped_int, det_ped["itens"])
                        
                        pedidos_salvos += 1
                        
                    except Exception as e:
                        logger.error(f"❌ Erro no pedido ID '{id_ped_str}' (página {pagina_atual}): {e}")
                        # Continuar com próximo pedido em vez de parar tudo
                        continue
                
                # Commit da página
                conn.commit()
                total_pedidos_processados += pedidos_salvos
                
                # 🔥 SALVAR PROGRESSO A CADA PÁGINA (CRÍTICO!)
                atualizar_progresso_pagina(
                    conn, PROCESSO_PEDIDOS, data_filtro_id, pagina_atual, total_paginas, pedidos_salvos
                )
                
                logger.info(f"✅ Página {pagina_atual} CONCLUÍDA: {pedidos_salvos} pedidos salvos")
            
            # Verificar se terminou
            if total_paginas == 0 or pagina_atual >= total_paginas:
                logger.info("🏁 Todas as páginas de pedidos foram processadas")
                break
            
            pagina_atual += 1
            time.sleep(1)
        
        # Finalizar com sucesso
        finalizar_progresso(conn, PROCESSO_PEDIDOS, data_filtro_id, sucesso=True)
        logger.info(f"🎉 PROCESSO DE PEDIDOS CONCLUÍDO: {total_pedidos_processados} pedidos processados")
        
        return peds_pag, total_paginas, True
        
    except Exception as e:
        logger.error(f"❌ Erro geral no processamento de pedidos: {e}", exc_info=True)
        # Salvar progresso mesmo em caso de erro
        if pagina_atual > pagina_inicial:
            atualizar_progresso_pagina(conn, PROCESSO_PEDIDOS, data_filtro_id, pagina_atual - 1, 
                                     total_paginas_conhecido or 0, 0)
        finalizar_progresso(conn, PROCESSO_PEDIDOS, data_filtro_id, sucesso=False)
        return None, 0, False

def processar_atualizacoes_estoque_v2(conn, data_alteracao_estoque_inicial=None, pagina=1):
    logger.info(f"Buscando pág {pagina} de estoques desde {data_alteracao_estoque_inicial or 'inicio'}.")
    params_api = {"pagina": pagina}; 
    if data_alteracao_estoque_inicial: params_api["dataAlteracao"] = data_alteracao_estoque_inicial
    ret, suc = make_api_v2_request_com_retry_35(ENDPOINT_LISTA_ATUALIZACOES_ESTOQUE, payload_dict=params_api)
    prods_est_api, num_pags, pag_ok = [],0,False
    if suc and ret: prods_est_api=ret.get("produtos",[]); num_pags=int(ret.get('numero_paginas',0))
    elif not suc: logger.error(f"Falha API estoques pág {pagina}."); return None,0,False
    if prods_est_api and isinstance(prods_est_api, list):
        todos_ok, salvos = True,0
        for prod_w in prods_est_api:
            prod_est_d = prod_w.get("produto")
            if not prod_est_d or not isinstance(prod_est_d,dict): logger.warning(f"Item estoque malformado (pág {pagina}): {prod_w}"); continue
            id_str = str(prod_est_d.get("id","")).strip()
            try:
                if not id_str or not id_str.isdigit(): logger.warning(f"ID prod inválido (estoque): '{id_str}'. Dados: {prod_est_d}"); continue
                id_int = int(id_str); prod_exists = False
                with conn.cursor() as cur: cur.execute("SELECT 1 FROM produtos WHERE id_produto = %s",(id_int,)); prod_exists=cur.fetchone()
                if not prod_exists: logger.warning(f"Prod ID {id_int} (estoque) não cadastrado. Pulando."); continue
                salvar_produto_estoque_total_db(conn,id_int,prod_est_d.get("nome",f"Prod ID {id_int}"),prod_est_d.get("saldo"),prod_est_d.get("saldoReservado"),prod_est_d.get("data_alteracao"))
                salvar_estoque_por_deposito_db(conn,id_int,prod_est_d.get("nome",f"Prod ID {id_int}"),prod_est_d.get("depositos",[]))
                salvos+=1
            except Exception as e: logger.error(f"Erro estoque prod ID {id_str} (pág {pagina}): {e}",True); todos_ok=False; break
        if todos_ok and salvos > 0:
            try:
                if conn and not conn.closed: conn.commit(); logger.info(f"Pág {pagina} estoques ({salvos} itens) commitada.")
                pag_ok=True
            except Exception as e: logger.error(f"Erro CRÍTICO commit pág {pagina} estoques: {e}",True);
            if conn and not conn.closed and not pag_ok: conn.rollback()
        elif not todos_ok and conn and not conn.closed: conn.rollback(); logger.warning(f"Pág {pagina} estoques com erros. ROLLBACK.")
        elif todos_ok and salvos == 0 and prods_est_api: logger.info(f"Pág {pagina} estoques processada, 0 salvos (ex: prods não cadastrados)."); pag_ok=True
        return prods_est_api, num_pags, pag_ok
    logger.info(f"Nenhuma atualização estoque API para pág {pagina} ou estrutura inválida.")
    return [], num_pags, True

def get_detalhes_pedido_v2(id_pedido_api):
    logger.debug(f"Buscando detalhes pedido ID {id_pedido_api}...")
    ret, suc = make_api_v2_request_com_retry_35(ENDPOINT_PEDIDO_OBTER, payload_dict={"id":id_pedido_api})
    if suc and ret and isinstance(ret.get("pedido"),dict): return ret["pedido"]
    logger.warning(f"Pedido ID {id_pedido_api} sem detalhes API ou erro.")
    return None

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    logger.info("=== Iniciando Cliente API v2 Tiny ERP - VERSÃO RESILIENTE COM CONTROLE GRANULAR ===")
    start_time_total = time.time()

    required_vars = {"TINY_API_V2_TOKEN": API_V2_TOKEN, "DB_HOST": DB_HOST, "DB_NAME": DB_NAME, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        logger.critical(f"Variáveis de ambiente obrigatórias não configuradas: {', '.join(missing_vars)}. Encerrando.")
        exit(1)
    
    db_conn = get_db_connection()
    if db_conn is None or (hasattr(db_conn, 'closed') and db_conn.closed):
        logger.critical("Falha conexão DB. Encerrando."); exit(1)
    
    try:
        criar_tabelas_db(db_conn)
        
        # Limpeza de progresso antigo
        limpar_progresso_antigo(db_conn, dias_manter=7)
        
        # PASSO 1: Categorias (Carga completa)
        logger.info("--- PASSO 1: Categorias ---")
        if get_categorias_v2(db_conn): logger.info("Passo 1 (Categorias) concluído.")
        else: logger.warning("Passo 1 (Categorias) com falhas.")
        logger.info("-" * 70)
        
        # PASSO 2: Produtos (Híbrido: Incremental com Janela de Segurança + Controle Granular)
        logger.info("--- PASSO 2: Produtos (Cadastrais e Categorias) ---")
        ts_inicio_prod = datetime.datetime.now(datetime.timezone.utc)
        etapa_prod_ok = True
        data_filtro_prod_api = determinar_data_filtro_inteligente(db_conn, PROCESSO_PRODUTOS, DIAS_JANELA_SEGURANCA)
        
        if data_filtro_prod_api:
            logger.info(f"Iniciando busca de produtos (cadastrais) desde: {data_filtro_prod_api}.")
            prods_resultado, total_pags_prod, prod_sucesso = search_produtos_v2_com_controle(db_conn, data_filtro_prod_api)
            if not prod_sucesso:
                logger.error("Falha no processamento de produtos.")
                etapa_prod_ok = False
        else:
            logger.error("Não foi possível determinar data de filtro para produtos.")
            etapa_prod_ok = False

        if etapa_prod_ok: set_ultima_execucao(db_conn, PROCESSO_PRODUTOS, ts_inicio_prod)
        else: logger.warning("Passo 2 (Produtos) com erros. Timestamp de auditoria NÃO atualizado.")
        logger.info("-" * 70)
        
        # PASSO 3: Estoques (Janela de ~29 dias da API)
        logger.info("--- PASSO 3: Atualizações de Estoque ---")
        ts_inicio_est = datetime.datetime.now(datetime.timezone.utc)
        etapa_est_ok = True
        filtro_est_api = (ts_inicio_est - datetime.timedelta(days=29)).strftime("%d/%m/%Y %H:%M:%S")
        
        total_est_listados, pag_est = 0, 1
        logger.info(f"Buscando atualizações de estoque nos últimos 29 dias (desde {filtro_est_api}).")
        while pag_est <= MAX_PAGINAS_POR_ETAPA:
            logger.info(f"Processando pág {pag_est} de atualizações de estoque...")
            est_pag, total_pags_est, pag_commit_est = processar_atualizacoes_estoque_v2(db_conn,filtro_est_api,pag_est)
            if est_pag is None: logger.error(f"Falha crítica (API) pág {pag_est} estoques. Interrompendo."); etapa_est_ok=False; break
            if not pag_commit_est and est_pag: logger.warning(f"Pág {pag_est} estoques não commitada. Interrompendo."); etapa_est_ok=False; break
            if est_pag: total_est_listados += len(est_pag)
            if total_pags_est == 0 or pag_est >= total_pags_est: logger.info("Todas págs estoques processadas."); break
            pag_est += 1; time.sleep(1)
            
        if etapa_est_ok: set_ultima_execucao(db_conn, PROCESSO_ESTOQUES, ts_inicio_est)
        else: logger.warning("Passo 3 (Estoques) com erros. Timestamp de auditoria NÃO atualizado.")
        logger.info("-" * 70)

        # PASSO 4: Pedidos (Híbrido: Incremental com Janela de Segurança + Controle Granular)
        logger.info("--- PASSO 4: Pedidos e Itens ---")
        ts_inicio_ped = datetime.datetime.now(datetime.timezone.utc)
        etapa_peds_ok = True
        data_filtro_pedidos_api = determinar_data_filtro_inteligente(db_conn, PROCESSO_PEDIDOS, DIAS_JANELA_SEGURANCA)

        if data_filtro_pedidos_api:
            logger.info(f"Iniciando busca de pedidos alterados desde: {data_filtro_pedidos_api}.")
            peds_resultado, total_pags_ped, ped_sucesso = search_pedidos_v2_com_controle(db_conn, data_filtro_pedidos_api)
            if not ped_sucesso:
                logger.error("Falha no processamento de pedidos.")
                etapa_peds_ok = False
        else:
            logger.error("Não foi possível determinar uma data de filtro para o processo de pedidos.")
            etapa_peds_ok = False
        
        if etapa_peds_ok: 
            set_ultima_execucao(db_conn, PROCESSO_PEDIDOS, ts_inicio_ped)
            logger.info(f"Passo 4 (Pedidos) concluído. Timestamp de auditoria atualizado.")
        else: 
            logger.warning("Passo 4 (Pedidos) com erros. Timestamp de auditoria NÃO atualizado.")
        logger.info("-" * 70)

        # Contagem Final
        logger.info("--- Contagem final dos registros no banco de dados ---")
        if db_conn and not db_conn.closed:
            with db_conn.cursor() as cur:
                tabelas = ["categorias","produtos","produto_categorias","produto_estoque_total","produto_estoque_depositos","pedidos","pedido_itens","script_ultima_execucao","script_progresso_paginas"]
                for t in tabelas:
                    try: cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(t))); logger.info(f"  - Tabela '{t}': {cur.fetchone()[0]} regs.")
                    except Exception as e: logger.error(f"  Erro ao contar '{t}': {e}",True)
        else: logger.warning("Não foi possível contar registros, DB fechado/indisponível.")
            
    except KeyboardInterrupt:
        logger.warning("Interrompido (KeyboardInterrupt).")
        if db_conn and not db_conn.closed: 
            try: db_conn.rollback(); logger.info("Rollback por KI bem-sucedido.")
            except Exception as e: logger.error(f"Erro no rollback KI: {e}",True)
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

