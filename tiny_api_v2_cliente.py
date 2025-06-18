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

# Nome dos processos para controle de última execução
PROCESSO_CATEGORIAS = "categorias"
PROCESSO_PRODUTOS = "produtos" 
PROCESSO_ESTOQUES = "estoques"
PROCESSO_PEDIDOS = "pedidos"

# --- CONFIGURAÇÕES GERAIS DE EXECUÇÃO ---
DEFAULT_API_TIMEOUT = 90
RETRY_DELAY_429 = 30 
DIAS_JANELA_SEGURANCA = 60
MAX_PAGINAS_POR_ETAPA = 500

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
            id SERIAL PRIMARY KEY,
            processo TEXT NOT NULL,
            data_filtro_api TEXT,
            pagina_atual INTEGER DEFAULT 0,
            total_paginas INTEGER DEFAULT 0,
            registros_processados INTEGER DEFAULT 0,
            timestamp_inicio TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            timestamp_ultima_pagina TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            status_execucao TEXT DEFAULT 'EM_ANDAMENTO',
            observacoes TEXT,
            UNIQUE(processo)
        );"""
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
    pattern = r'^\d{2}/\d{2}/\d{4}$'
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
        return dt_sintetico
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

# ===== NOVA FUNÇÃO: VERIFICAÇÃO DE PROCESSO CONCLUÍDO =====
def verificar_processo_concluido(conn, processo):
    """Verifica se processo já foi concluído."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status_execucao, pagina_atual, total_paginas, registros_processados
                FROM script_progresso_paginas 
                WHERE processo = %s
            """, (processo,))
            result = cur.fetchone()
            
            if result:
                status, pagina_atual, total_paginas, registros = result
                if status == 'CONCLUIDO' and pagina_atual == total_paginas and total_paginas > 0:
                    return True, f"Processo já concluído: {pagina_atual}/{total_paginas} páginas ({registros} registros)"
                elif status == 'EM_ANDAMENTO':
                    return False, f"Processo em andamento: página {pagina_atual}/{total_paginas}"
                elif status == 'ERRO':
                    return False, f"Processo com erro na página {pagina_atual}, continuando..."
            
            return False, "Nenhum progresso encontrado"
    except Exception as e:
        logger.error(f"Erro ao verificar processo concluído: {e}")
        return False, "Erro na verificação"

# ===== FUNÇÃO MODIFICADA: INICIALIZAÇÃO INTELIGENTE =====
def inicializar_progresso(conn, processo, data_filtro_api):
    """Inicializa progresso de forma inteligente, verificando se já foi concluído."""
    
    # 1. Verificar se processo já foi concluído
    concluido, mensagem = verificar_processo_concluido(conn, processo)
    
    if concluido:
        logger.info(f"⏭️ PULANDO: {processo} - {mensagem}")
        return "CONCLUIDO"
    
    # 2. Se não concluído, verificar se há progresso em andamento
    logger.info(f"🔍 VERIFICANDO: {processo} - {mensagem}")
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pagina_atual, total_paginas, registros_processados, status_execucao
                FROM script_progresso_paginas 
                WHERE processo = %s
            """, (processo,))
            result = cur.fetchone()
            
            if result:
                pagina_atual, total_paginas, registros, status = result
                logger.warning(f"🔄 RECUPERANDO PROGRESSO: {processo} estava na página {pagina_atual}")
                logger.warning(f"▶️ CONTINUANDO da página {pagina_atual + 1} (já processados: {registros} registros)")
                return pagina_atual + 1
            else:
                logger.info(f"🆕 INICIANDO novo progresso para {processo} desde {data_filtro_api}")
                # Criar novo registro de progresso
                cur.execute("""
                    INSERT INTO script_progresso_paginas 
                    (processo, data_filtro_api, pagina_atual, total_paginas, registros_processados, timestamp_inicio, timestamp_ultima_pagina, status_execucao)
                    VALUES (%s, %s, 0, 0, 0, NOW(), NOW(), 'EM_ANDAMENTO')
                    ON CONFLICT (processo) DO UPDATE SET
                        data_filtro_api = EXCLUDED.data_filtro_api,
                        pagina_atual = 0,
                        total_paginas = 0,
                        registros_processados = 0,
                        timestamp_inicio = NOW(),
                        status_execucao = 'EM_ANDAMENTO'
                """, (processo, data_filtro_api))
                conn.commit()
                return 1
                
    except Exception as e:
        logger.error(f"Erro ao inicializar progresso para {processo}: {e}")
        return 1

def atualizar_progresso_pagina(conn, processo, pagina_atual, total_paginas, registros_processados):
    """Atualiza progresso da página atual."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE script_progresso_paginas 
                SET pagina_atual = %s, 
                    total_paginas = %s, 
                    registros_processados = registros_processados + %s,
                    timestamp_ultima_pagina = NOW()
                WHERE processo = %s
            """, (pagina_atual, total_paginas, registros_processados, processo))
            conn.commit()
            
            if total_paginas > 0:
                percentual = round((pagina_atual / total_paginas) * 100, 1)
                logger.info(f"📊 PROGRESSO SALVO: {processo} - {pagina_atual}/{total_paginas} ({percentual}%) - {registros_processados} registros")
            else:
                logger.info(f"📊 PROGRESSO SALVO: {processo} - página {pagina_atual} - {registros_processados} registros")
                
    except Exception as e:
        logger.error(f"Erro ao atualizar progresso: {e}")

def finalizar_progresso(conn, processo, status_final="CONCLUIDO", observacoes=None):
    """Finaliza progresso com status final."""
    try:
        with conn.cursor() as cur:
            if observacoes:
                cur.execute("""
                    UPDATE script_progresso_paginas 
                    SET status_execucao = %s, observacoes = %s, timestamp_ultima_pagina = NOW()
                    WHERE processo = %s
                """, (status_final, observacoes, processo))
            else:
                cur.execute("""
                    UPDATE script_progresso_paginas 
                    SET status_execucao = %s, timestamp_ultima_pagina = NOW()
                    WHERE processo = %s
                """, (status_final, processo))
            conn.commit()
            
            if status_final == "CONCLUIDO":
                logger.info(f"🏁 PROGRESSO FINALIZADO: {processo} - ✅ SUCESSO")
            else:
                logger.info(f"🏁 PROGRESSO FINALIZADO: {processo} - {status_final}")
                
    except Exception as e:
        logger.error(f"Erro ao finalizar progresso: {e}")

# ===== FUNÇÕES DE API E SALVAMENTO (mantidas iguais) =====
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
        logger.debug(f"Estoque total salvo para produto ID {id_p}.")
    except Exception as e: logger.error(f"Erro ao salvar estoque total do produto ID {id_p}: {e}", exc_info=True); raise

def salvar_produto_estoque_depositos_db(conn, id_produto, depositos_lista_api):
    id_p = int(id_produto)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM produto_estoque_depositos WHERE id_produto = %s", (id_p,))
            if depositos_lista_api and isinstance(depositos_lista_api, list):
                dados = []
                for dep in depositos_lista_api:
                    if isinstance(dep, dict):
                        nome_dep = dep.get("nome"); saldo_dep = safe_float_convert(dep.get("saldo"))
                        desconsiderar = dep.get("desconsiderar"); empresa = dep.get("empresa")
                        if nome_dep is not None: dados.append((id_p, nome_dep, saldo_dep, desconsiderar, empresa))
                if dados: execute_values(cur, "INSERT INTO produto_estoque_depositos (id_produto, nome_deposito, saldo_deposito, desconsiderar_deposito, empresa_deposito) VALUES %s", dados); logger.debug(f"{len(dados)} depósitos salvos para produto ID {id_p}.")
    except Exception as e: logger.error(f"Erro ao salvar depósitos do produto ID {id_p}: {e}", exc_info=True); raise

def salvar_pedido_db(conn, pedido_api_data):
    id_ped_str = str(pedido_api_data.get("id","")).strip()
    if not id_ped_str or not id_ped_str.isdigit(): raise ValueError(f"ID do pedido inválido: {id_ped_str}")
    id_ped = int(id_ped_str)
    try:
        dados = { "id_pedido": id_ped, "numero_pedido": pedido_api_data.get("numero"), "numero_ecommerce": pedido_api_data.get("numero_ecommerce"), "data_pedido": pedido_api_data.get("data_pedido"), "data_prevista": pedido_api_data.get("data_prevista"), "nome_cliente": pedido_api_data.get("nome_cliente"), "valor_pedido": safe_float_convert(pedido_api_data.get("valor_pedido")), "id_vendedor": int(pedido_api_data.get("id_vendedor")) if str(pedido_api_data.get("id_vendedor","")).strip().isdigit() else None, "nome_vendedor": pedido_api_data.get("nome_vendedor"), "situacao_pedido": pedido_api_data.get("situacao"), "codigo_rastreamento": pedido_api_data.get("codigo_rastreamento") }
        with conn.cursor() as cur:
            cols = ", ".join(dados.keys()); vals_template = ", ".join(["%s"] * len(dados))
            updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in dados.keys() if col != "id_pedido"])
            query = f"INSERT INTO pedidos ({cols}) VALUES ({vals_template}) ON CONFLICT (id_pedido) DO UPDATE SET {updates};"
            cur.execute(query, tuple(dados.values()))
        logger.debug(f"Pedido ID {id_ped} salvo/atualizado.")
    except Exception as e: logger.error(f"Erro ao salvar pedido ID '{id_ped_str}': {e}", exc_info=True); raise

def salvar_pedido_itens_db(conn, id_pedido, itens_lista_api):
    if not isinstance(id_pedido, int): raise ValueError(f"ID do pedido inválido: {id_pedido}")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pedido_itens WHERE id_pedido = %s", (id_pedido,))
            if itens_lista_api and isinstance(itens_lista_api, list):
                dados = []
                for item in itens_lista_api:
                    if isinstance(item, dict):
                        id_prod_tiny = int(item.get("id_produto_tiny")) if str(item.get("id_produto_tiny","")).strip().isdigit() else None
                        cod_prod = item.get("codigo_produto"); desc_prod = item.get("descricao_produto")
                        qtd = safe_float_convert(item.get("quantidade")); unidade = item.get("unidade")
                        val_unit = safe_float_convert(item.get("valor_unitario")); id_grade = item.get("id_grade")
                        dados.append((id_pedido, id_prod_tiny, cod_prod, desc_prod, qtd, unidade, val_unit, id_grade))
                if dados: execute_values(cur, "INSERT INTO pedido_itens (id_pedido, id_produto_tiny, codigo_produto_pedido, descricao_produto_pedido, quantidade, unidade_pedido, valor_unitario_pedido, id_grade_pedido) VALUES %s", dados); logger.debug(f"{len(dados)} itens salvos para pedido ID {id_pedido}.")
    except Exception as e: logger.error(f"Erro ao salvar itens do pedido ID {id_pedido}: {e}", exc_info=True); raise

def make_api_v2_request(endpoint, params=None, timeout=DEFAULT_API_TIMEOUT, max_retries=3):
    url = BASE_URL_V2 + endpoint; params = params or {}; params["token"] = API_V2_TOKEN; params["formato"] = "json"
    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"Fazendo requisição para API (tentativa {attempt}): {endpoint}")
            response = requests.post(url, data=params, timeout=timeout)
            if response.status_code == 429:
                logger.warning(f"Rate limit atingido (429). Aguardando {RETRY_DELAY_429}s antes de tentar novamente...")
                time.sleep(RETRY_DELAY_429); continue
            response.raise_for_status(); data = response.json()
            if data.get("status") == "Erro":
                codigo_erro = data.get("codigo_erro", ""); msg_erro = data.get("erros", [{}])[0].get("erro", "Erro desconhecido") if data.get("erros") else "Erro desconhecido"
                logger.error(f"API Tiny: Status 'Erro' (Endpoint: {endpoint}). Código: {codigo_erro}. Msg: {msg_erro}. Resp: {data}")
                return None
            return data
        except requests.exceptions.Timeout: logger.warning(f"Timeout na requisição para {endpoint} (tentativa {attempt}). Tentando novamente...")
        except requests.exceptions.RequestException as e: logger.error(f"Erro de requisição para {endpoint} (tentativa {attempt}): {e}")
        except json.JSONDecodeError as e: logger.error(f"Erro ao decodificar JSON da resposta de {endpoint} (tentativa {attempt}): {e}")
        except Exception as e: logger.error(f"Erro inesperado na requisição para {endpoint} (tentativa {attempt}): {e}", exc_info=True)
        if attempt < max_retries: time.sleep(5)
    logger.error(f"Falha definitiva na API após {max_retries} tentativas: {endpoint}"); return None

def get_categorias_v2(conn):
    logger.info("Iniciando Categorias.")
    data = make_api_v2_request(ENDPOINT_CATEGORIAS)
    if not data or "retorno" not in data: logger.error("Falha ao obter categorias da API."); return False
    try:
        categorias_arvore = data["retorno"].get("categorias", [])
        if not isinstance(categorias_arvore, list): logger.warning("Estrutura de categorias inesperada."); return False
        with conn.cursor() as cur:
            for cat_raiz in categorias_arvore: salvar_categoria_db(conn, cat_raiz)
        if conn and not conn.closed: conn.commit()
        logger.info(f"{len(categorias_arvore)} cats raiz commitadas.")
        return True
    except Exception as e: logger.error(f"Erro ao processar categorias: {e}", exc_info=True); return False

def search_produtos_v2_com_controle(conn, data_filtro_api):
    """Busca produtos com controle granular de progresso."""
    
    # Verificar se processo já foi concluído
    pagina_inicial = inicializar_progresso(conn, PROCESSO_PRODUTOS, data_filtro_api)
    
    if pagina_inicial == "CONCLUIDO":
        logger.info("⏭️ Produtos já processados completamente. Pulando etapa.")
        return True
    
    logger.info(f"🚀 INICIANDO busca de produtos desde página {pagina_inicial}")
    
    pagina_atual = pagina_inicial
    produtos_salvos_total = 0
    
    try:
        while pagina_atual <= MAX_PAGINAS_POR_ETAPA:
            logger.info(f"📄 Processando página {pagina_atual} de produtos...")
            
            params = {
                "dataAlteracaoInicial": data_filtro_api,
                "pagina": pagina_atual
            }
            
            data = make_api_v2_request(ENDPOINT_PRODUTOS_PESQUISA, params)
            if not data or "retorno" not in data:
                logger.error(f"Falha API produtos pág {pagina_atual} por DATA DE ALTERAÇÃO desde {data_filtro_api}.")
                finalizar_progresso(conn, PROCESSO_PRODUTOS, "ERRO", f"❌ Falha na API - página {pagina_atual}")
                return False
            
            retorno = data["retorno"]
            produtos_lista = retorno.get("produtos", [])
            total_paginas = int(retorno.get("numero_paginas", 1))
            
            if not produtos_lista:
                logger.info(f"Nenhum produto encontrado na página {pagina_atual}. Finalizando busca.")
                break
            
            produtos_salvos_pagina = 0
            for produto_data in produtos_lista:
                try:
                    produto_info = produto_data.get("produto", {})
                    if produto_info and produto_info.get("id"):
                        salvar_produto_db(conn, produto_info)
                        
                        id_produto = int(produto_info.get("id"))
                        categorias_lista = produto_info.get("categoria", [])
                        if categorias_lista:
                            salvar_produto_categorias_db(conn, id_produto, categorias_lista)
                        
                        produtos_salvos_pagina += 1
                except Exception as e:
                    logger.error(f"Erro ao processar produto na página {pagina_atual}: {e}")
                    continue
            
            conn.commit()
            produtos_salvos_total += produtos_salvos_pagina
            
            # Atualizar progresso
            atualizar_progresso_pagina(conn, PROCESSO_PRODUTOS, pagina_atual, total_paginas, produtos_salvos_pagina)
            logger.info(f"✅ Página {pagina_atual} CONCLUÍDA: {produtos_salvos_pagina} produtos salvos")
            
            # Verificar se chegou ao fim
            if pagina_atual >= total_paginas:
                logger.info(f"Todas as páginas de produtos processadas ({pagina_atual}/{total_paginas}).")
                finalizar_progresso(conn, PROCESSO_PRODUTOS, "CONCLUIDO", "🏁 Processo finalizado: ✅ SUCESSO")
                break
            
            pagina_atual += 1
            time.sleep(1)  # Pausa entre páginas
        
        logger.info(f"Busca de produtos finalizada. Total de produtos salvos: {produtos_salvos_total}")
        return True
        
    except Exception as e:
        logger.error(f"Erro geral na busca de produtos: {e}", exc_info=True)
        finalizar_progresso(conn, PROCESSO_PRODUTOS, "ERRO", f"❌ Erro geral: {str(e)}")
        return False

def search_pedidos_v2_com_controle(conn, data_filtro_api):
    """Busca pedidos com controle granular de progresso."""
    
    # Verificar se processo já foi concluído
    pagina_inicial = inicializar_progresso(conn, PROCESSO_PEDIDOS, data_filtro_api)
    
    if pagina_inicial == "CONCLUIDO":
        logger.info("⏭️ Pedidos já processados completamente. Pulando etapa.")
        return True
    
    logger.info(f"🚀 INICIANDO busca de pedidos desde página {pagina_inicial}")
    
    pagina_atual = pagina_inicial
    pedidos_salvos_total = 0
    
    try:
        while pagina_atual <= MAX_PAGINAS_POR_ETAPA:
            logger.info(f"📄 Processando página {pagina_atual} de pedidos...")
            
            params = {
                "dataAlteracaoInicial": data_filtro_api,
                "pagina": pagina_atual
            }
            
            data = make_api_v2_request(ENDPOINT_PEDIDOS_PESQUISA, params)
            if not data or "retorno" not in data:
                logger.error(f"Falha API pedidos pág {pagina_atual} por DATA DE ALTERAÇÃO desde {data_filtro_api}.")
                finalizar_progresso(conn, PROCESSO_PEDIDOS, "ERRO", f"❌ Falha na API - página {pagina_atual}")
                return False
            
            retorno = data["retorno"]
            pedidos_lista = retorno.get("pedidos", [])
            total_paginas = int(retorno.get("numero_paginas", 1))
            
            if not pedidos_lista:
                logger.info(f"Nenhum pedido encontrado na página {pagina_atual}. Finalizando busca.")
                break
            
            pedidos_salvos_pagina = 0
            for pedido_data in pedidos_lista:
                try:
                    pedido_info = pedido_data.get("pedido", {})
                    if pedido_info and pedido_info.get("id"):
                        salvar_pedido_db(conn, pedido_info)
                        
                        id_pedido = int(pedido_info.get("id"))
                        itens_lista = pedido_info.get("itens", [])
                        if itens_lista:
                            salvar_pedido_itens_db(conn, id_pedido, itens_lista)
                        
                        pedidos_salvos_pagina += 1
                except Exception as e:
                    logger.error(f"Erro ao processar pedido na página {pagina_atual}: {e}")
                    continue
            
            conn.commit()
            pedidos_salvos_total += pedidos_salvos_pagina
            
            # Atualizar progresso
            atualizar_progresso_pagina(conn, PROCESSO_PEDIDOS, pagina_atual, total_paginas, pedidos_salvos_pagina)
            logger.info(f"✅ Página {pagina_atual} CONCLUÍDA: {pedidos_salvos_pagina} pedidos salvos")
            
            # Verificar se chegou ao fim
            if pagina_atual >= total_paginas:
                logger.info(f"Todas as páginas de pedidos processadas ({pagina_atual}/{total_paginas}).")
                finalizar_progresso(conn, PROCESSO_PEDIDOS, "CONCLUIDO", "🏁 Processo finalizado: ✅ SUCESSO")
                break
            
            pagina_atual += 1
            time.sleep(1)  # Pausa entre páginas
        
        logger.info(f"Busca de pedidos finalizada. Total de pedidos salvos: {pedidos_salvos_total}")
        return True
        
    except Exception as e:
        logger.error(f"Erro geral na busca de pedidos: {e}", exc_info=True)
        finalizar_progresso(conn, PROCESSO_PEDIDOS, "ERRO", f"❌ Erro geral: {str(e)}")
        return False

def get_estoques_v2(conn, data_filtro_api):
    logger.info(f"Iniciando Estoques desde {data_filtro_api}.")
    pagina = 1; estoques_salvos = 0
    while pagina <= MAX_PAGINAS_POR_ETAPA:
        logger.info(f"Buscando pág {pagina} de estoques por DATA DE ALTERAÇÃO desde {data_filtro_api}.")
        params = {"dataAlteracaoInicial": data_filtro_api, "pagina": pagina}
        data = make_api_v2_request(ENDPOINT_LISTA_ATUALIZACOES_ESTOQUE, params)
        if not data or "retorno" not in data: logger.error(f"Falha API estoques pág {pagina} por DATA DE ALTERAÇÃO desde {data_filtro_api}."); return False
        retorno = data["retorno"]; produtos_lista = retorno.get("produtos", [])
        if not produtos_lista: logger.info(f"Nenhum estoque encontrado na pág {pagina}. Finalizando busca."); break
        for produto_data in produtos_lista:
            try:
                produto_info = produto_data.get("produto", {})
                if produto_info and produto_info.get("id"):
                    id_produto = int(produto_info.get("id")); nome_produto = produto_info.get("nome")
                    saldo_total = produto_info.get("saldo"); saldo_reservado = produto_info.get("saldoReservado")
                    data_atualizacao = produto_info.get("dataUltimaAtualizacao")
                    salvar_produto_estoque_total_db(conn, id_produto, nome_produto, saldo_total, saldo_reservado, data_atualizacao)
                    depositos_lista = produto_info.get("depositos", [])
                    if depositos_lista: salvar_produto_estoque_depositos_db(conn, id_produto, depositos_lista)
                    estoques_salvos += 1
            except Exception as e: logger.error(f"Erro ao processar estoque na pág {pagina}: {e}"); continue
        if conn and not conn.closed: conn.commit()
        logger.info(f"Pág {pagina} estoques commitada.")
        total_paginas = int(retorno.get("numero_paginas", 1))
        if pagina >= total_paginas: logger.info(f"Todas págs estoques processadas ({pagina}/{total_paginas})."); break
        pagina += 1; time.sleep(1)
    logger.info(f"Busca de estoques finalizada. Total de estoques salvos: {estoques_salvos}"); return True

def contar_registros_tabela(conn, nome_tabela):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(nome_tabela)))
            return cur.fetchone()[0]
    except Exception as e: logger.error(f"Erro ao contar registros da tabela '{nome_tabela}': {e}"); return 0

# ===== FUNÇÃO PRINCIPAL =====
if __name__ == "__main__":
    inicio_script = time.time()
    logger.info("=== Iniciando Cliente API v2 Tiny ERP - MODO PRODUÇÃO (Lógica Híbrida) ===")
    
    db_conn = get_db_connection()
    if not db_conn:
        logger.critical("Não foi possível conectar ao banco de dados. Encerrando.")
        exit(1)
    
    try:
        criar_tabelas_db(db_conn)
        
        # PASSO 1: Categorias
        logger.info("--- PASSO 1: Categorias ---")
        if get_categorias_v2(db_conn):
            set_ultima_execucao(db_conn, PROCESSO_CATEGORIAS)
            logger.info("Passo 1 (Categorias) concluído.")
        else:
            logger.error("Falha no passo 1 (Categorias).")
        
        logger.info("----------------------------------------------------------------------")
        
        # PASSO 2: Produtos (com verificação inteligente)
        logger.info("--- PASSO 2: Produtos (Cadastrais e Categorias) ---")
        data_filtro_prod_api = determinar_data_filtro_inteligente(db_conn, PROCESSO_PRODUTOS, DIAS_JANELA_SEGURANCA)
        logger.info(f"Iniciando busca de produtos (cadastrais) desde: {data_filtro_prod_api}.")
        
        if search_produtos_v2_com_controle(db_conn, data_filtro_prod_api):
            set_ultima_execucao(db_conn, PROCESSO_PRODUTOS)
            logger.info("Passo 2 (Produtos) concluído.")
        else:
            logger.error("Falha no processamento de produtos.")
            logger.warning("Passo 2 (Produtos) com erros. Timestamp de auditoria NÃO atualizado.")
        
        logger.info("----------------------------------------------------------------------")
        
        # PASSO 3: Estoques
        logger.info("--- PASSO 3: Estoques ---")
        data_filtro_est_api = determinar_data_filtro_inteligente(db_conn, PROCESSO_ESTOQUES, DIAS_JANELA_SEGURANCA)
        logger.info(f"Iniciando busca de estoques desde: {data_filtro_est_api}.")
        
        if get_estoques_v2(db_conn, data_filtro_est_api):
            set_ultima_execucao(db_conn, PROCESSO_ESTOQUES)
            logger.info("Todas págs estoques processadas.")
        else:
            logger.error("Falha no processamento de estoques.")
            logger.warning("Passo 3 (Estoques) com erros. Timestamp de auditoria NÃO atualizado.")
        
        logger.info("----------------------------------------------------------------------")
        
        # PASSO 4: Pedidos (com controle granular)
        logger.info("--- PASSO 4: Pedidos e Itens ---")
        data_filtro_ped_api = determinar_data_filtro_inteligente(db_conn, PROCESSO_PEDIDOS, DIAS_JANELA_SEGURANCA)
        logger.info(f"Iniciando busca de pedidos alterados desde: {data_filtro_ped_api}.")
        
        if search_pedidos_v2_com_controle(db_conn, data_filtro_ped_api):
            set_ultima_execucao(db_conn, PROCESSO_PEDIDOS)
            logger.info("Passo 4 (Pedidos) concluído.")
        else:
            logger.error("Falha no processamento de pedidos.")
            logger.warning("Passo 4 (Pedidos) com erros. Timestamp de auditoria NÃO atualizado.")
        
        logger.info("----------------------------------------------------------------------")
        
        # Contagem final
        logger.info("--- Contagem final dos registros no banco de dados ---")
        tabelas = ["categorias", "produtos", "produto_categorias", "produto_estoque_total", "produto_estoque_depositos", "pedidos", "pedido_itens", "script_ultima_execucao", "script_progresso_paginas"]
        for tabela in tabelas:
            count = contar_registros_tabela(db_conn, tabela)
            logger.info(f"  - Tabela '{tabela}': {count} regs.")
    
    except Exception as e:
        logger.critical(f"ERRO GERAL NO PROCESSAMENTO: {e}", exc_info=True)
        if db_conn and not db_conn.closed:
            try:
                db_conn.rollback()
                logger.info("Rollback por erro geral bem-sucedido.")
            except Exception as rollback_error:
                logger.error(f"Erro durante rollback: {rollback_error}")
    
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            logger.info("Conexão PostgreSQL fechada.")
        
        tempo_total = time.time() - inicio_script
        logger.info(f"=== Processo Concluído em {tempo_total:.2f} segundos ===")

