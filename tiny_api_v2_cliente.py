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
MAX_PAGINAS_POR_ETAPA = 10000  # Aumentado para permitir processar todas as páginas

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
def inicializar_progresso(conn, processo, data_filtro_api, eh_busca_incremental=True):
    """Inicializa progresso de forma inteligente, verificando se já foi concluído."""
    
    # Para buscas incrementais (por data), sempre verificar novos dados
    if eh_busca_incremental:
        logger.info(f"🔄 BUSCA INCREMENTAL: {processo} - Verificando dados desde {data_filtro_api}")
        # Resetar progresso para busca incremental
        try:
            with conn.cursor() as cur:
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
            logger.error(f"Erro ao inicializar busca incremental para {processo}: {e}")
            return 1
    
    # Para cargas completas, verificar se já foi concluído
    concluido, mensagem = verificar_processo_concluido(conn, processo)
    
    if concluido:
        logger.info(f"⏭️ PULANDO: {processo} - {mensagem}")
        return "CONCLUIDO"
    
    # Se não concluído, verificar se há progresso em andamento
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
            logger.info(f"✅ PROCESSO FINALIZADO: {processo} - Status: {status_final}")
            
    except Exception as e:
        logger.error(f"Erro ao finalizar progresso: {e}")

def make_api_v2_request(endpoint, params=None, max_retries=3):
    """Faz requisição para API v2 com retry automático."""
    if params is None: params = {}
    params["token"] = API_V2_TOKEN
    params["formato"] = "JSON"
    
    for attempt in range(max_retries):
        try:
            response = requests.post(f"{BASE_URL_V2}{endpoint}", data=params, timeout=DEFAULT_API_TIMEOUT)
            
            if response.status_code == 429:
                logger.warning(f"Rate limit atingido (429). Aguardando {RETRY_DELAY_429}s antes de tentar novamente...")
                time.sleep(RETRY_DELAY_429)
                continue
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout na tentativa {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição (tentativa {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON: {e}")
            return None
    
    logger.error(f"Falha após {max_retries} tentativas para {endpoint}")
    return None

def salvar_categoria_db(conn, categoria, id_pai=None):
    """Salva categoria e suas subcategorias recursivamente."""
    try:
        id_categoria = int(categoria.get("id", 0))
        descricao = categoria.get("descricao", "")
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO categorias (id_categoria, descricao_categoria, id_categoria_pai)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_categoria) DO UPDATE SET
                    descricao_categoria = EXCLUDED.descricao_categoria,
                    id_categoria_pai = EXCLUDED.id_categoria_pai
            """, (id_categoria, descricao, id_pai))
        
        # Processar subcategorias (nodes)
        nodes = categoria.get("nodes", [])
        if isinstance(nodes, list):
            for subcategoria in nodes:
                salvar_categoria_db(conn, subcategoria, id_categoria)
                
    except Exception as e:
        logger.error(f"Erro ao salvar categoria {categoria}: {e}")

def get_categorias_v2(conn):
    """FUNÇÃO CORRIGIDA: Obtém categorias da API v2."""
    logger.info("Iniciando Categorias.")
    data = make_api_v2_request(ENDPOINT_CATEGORIAS)
    if not data or "retorno" not in data: 
        logger.error("Falha ao obter categorias da API.")
        return False
    
    try:
        retorno = data["retorno"]
        
        # CORREÇÃO: Verificar se retorno é uma lista ou dicionário
        if isinstance(retorno, list):
            # Se retorno é uma lista, as categorias estão diretamente nela
            categorias_arvore = retorno
        elif isinstance(retorno, dict):
            # Se retorno é um dicionário, verificar se tem campo "categorias"
            if "categorias" in retorno:
                categorias_arvore = retorno["categorias"]
            else:
                # Se não tem campo "categorias", pode ser que as categorias estejam no próprio retorno
                logger.warning("Estrutura de retorno inesperada, tentando usar retorno diretamente")
                categorias_arvore = [retorno] if retorno else []
        else:
            logger.error(f"Tipo de retorno inesperado: {type(retorno)}")
            return False
        
        if not isinstance(categorias_arvore, list): 
            logger.warning(f"Estrutura de categorias inesperada. Tipo: {type(categorias_arvore)}")
            return False
        
        # Salvar categorias no banco
        with conn.cursor() as cur:
            for cat_raiz in categorias_arvore: 
                salvar_categoria_db(conn, cat_raiz)
        
        if conn and not conn.closed: 
            conn.commit()
        
        logger.info(f"{len(categorias_arvore)} categorias raiz processadas e commitadas.")
        return True
        
    except Exception as e: 
        logger.error(f"Erro ao processar categorias: {e}", exc_info=True)
        if conn and not conn.closed:
            conn.rollback()
        return False

def search_produtos_v2_com_controle(conn, data_filtro_api):
    """Busca produtos com controle granular de progresso."""
    
    # Para produtos, sempre fazer busca incremental (verificar alterações por data)
    pagina_inicial = inicializar_progresso(conn, PROCESSO_PRODUTOS, data_filtro_api, eh_busca_incremental=True)
    
    if pagina_inicial == "CONCLUIDO":
        logger.info("⏭️ Produtos já processados completamente. Pulando etapa.")
        return True
    
    logger.info(f"🚀 INICIANDO busca de produtos desde página {pagina_inicial}")
    
    pagina_atual = pagina_inicial
    total_produtos_salvos = 0
    total_paginas_api = None
    
    while True:  # Loop infinito, controlado pelas condições internas
        logger.info(f"📄 Processando página {pagina_atual} de produtos...")
        
        params = {
            "pagina": pagina_atual,
            "dataAlteracao": data_filtro_api
        }
        
        data = make_api_v2_request(ENDPOINT_PRODUTOS_PESQUISA, params)
        if not data or "retorno" not in data:
            logger.error(f"Falha na API para página {pagina_atual}")
            finalizar_progresso(conn, PROCESSO_PRODUTOS, "ERRO", f"Falha na API página {pagina_atual}")
            return False
        
        retorno = data["retorno"]
        
        # Verificar se há produtos
        if "produtos" not in retorno or not retorno["produtos"]:
            logger.info(f"Nenhum produto encontrado na página {pagina_atual}. Finalizando busca.")
            finalizar_progresso(conn, PROCESSO_PRODUTOS, "CONCLUIDO")
            break
        
        produtos = retorno["produtos"]
        total_paginas_api = int(retorno.get("numero_paginas", 1))
        
        # Processar produtos da página
        produtos_salvos_pagina = 0
        for produto_resumo in produtos:
            if processar_produto_completo(conn, produto_resumo):
                produtos_salvos_pagina += 1
        
        total_produtos_salvos += produtos_salvos_pagina
        
        # Atualizar progresso
        atualizar_progresso_pagina(conn, PROCESSO_PRODUTOS, pagina_atual, total_paginas_api, produtos_salvos_pagina)
        
        logger.info(f"✅ Página {pagina_atual} CONCLUÍDA: {produtos_salvos_pagina} produtos salvos")
        
        # Verificar se chegou na última página
        if pagina_atual >= total_paginas_api:
            logger.info(f"🏁 TODAS as páginas processadas! Total: {total_produtos_salvos} produtos")
            finalizar_progresso(conn, PROCESSO_PRODUTOS, "CONCLUIDO")
            break
        
        # Verificar limite de segurança para evitar loops infinitos
        if pagina_atual > MAX_PAGINAS_POR_ETAPA:
            logger.warning(f"⚠️ LIMITE DE SEGURANÇA atingido na página {pagina_atual}. Pausando para próxima execução.")
            finalizar_progresso(conn, PROCESSO_PRODUTOS, "EM_ANDAMENTO", f"Pausado na página {pagina_atual}")
            break
        
        pagina_atual += 1
        time.sleep(1)  # Rate limiting
    
    return True

def processar_produto_completo(conn, produto_resumo):
    """Processa um produto completo obtendo detalhes da API."""
    try:
        id_produto = produto_resumo.get("id")
        if not id_produto:
            return False
        
        # Obter detalhes completos do produto
        params = {"id": id_produto}
        data = make_api_v2_request(ENDPOINT_PRODUTO_OBTER, params)
        
        if not data or "retorno" not in data:
            logger.warning(f"Falha ao obter detalhes do produto {id_produto}")
            return False
        
        produto_completo = data["retorno"]["produto"]
        
        # Salvar produto no banco
        return salvar_produto_db(conn, produto_completo)
        
    except Exception as e:
        logger.error(f"Erro ao processar produto {produto_resumo}: {e}")
        return False

def salvar_produto_db(conn, produto):
    """Salva produto no banco de dados."""
    try:
        with conn.cursor() as cur:
            # Inserir/atualizar produto principal
            cur.execute("""
                INSERT INTO produtos (
                    id_produto, nome_produto, codigo_produto, preco_produto, 
                    unidade_produto, situacao_produto, data_criacao_produto, 
                    gtin_produto, preco_promocional_produto, preco_custo_produto, 
                    preco_custo_medio_produto, tipo_variacao_produto
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
                    tipo_variacao_produto = EXCLUDED.tipo_variacao_produto
            """, (
                int(produto.get("id", 0)),
                produto.get("nome", ""),
                produto.get("codigo", ""),
                safe_float_convert(produto.get("preco", 0)),
                produto.get("unidade", ""),
                produto.get("situacao", ""),
                produto.get("data_criacao", ""),
                produto.get("gtin", ""),
                safe_float_convert(produto.get("preco_promocional", 0)),
                safe_float_convert(produto.get("preco_custo", 0)),
                safe_float_convert(produto.get("preco_custo_medio", 0)),
                produto.get("tipo_variacao", "")
            ))
            
            # Processar categorias do produto
            categorias = produto.get("categoria", [])
            if categorias:
                # Limpar categorias antigas
                cur.execute("DELETE FROM produto_categorias WHERE id_produto = %s", (int(produto.get("id", 0)),))
                
                # Inserir novas categorias
                for categoria in categorias:
                    id_categoria = categoria.get("id")
                    if id_categoria:
                        cur.execute("""
                            INSERT INTO produto_categorias (id_produto, id_categoria)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (int(produto.get("id", 0)), int(id_categoria)))
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar produto {produto.get('id', 'N/A')}: {e}")
        return False

def get_estoques_v2(conn, data_filtro_api):
    """Obtém atualizações de estoque da API v2."""
    logger.info(f"Iniciando Estoques desde {data_filtro_api}.")
    
    pagina = 1
    total_estoques_salvos = 0
    
    while True:
        logger.info(f"Buscando pág {pagina} de estoques por DATA DE ALTERAÇÃO desde {data_filtro_api}.")
        
        params = {
            "pagina": pagina,
            "dataAlteracao": data_filtro_api
        }
        
        data = make_api_v2_request(ENDPOINT_LISTA_ATUALIZACOES_ESTOQUE, params)
        if not data or "retorno" not in data:
            logger.error(f"Falha na API de estoques para página {pagina}")
            return False
        
        retorno = data["retorno"]
        
        # DEBUG: Log da estrutura de retorno
        logger.info(f"DEBUG ESTOQUES - Estrutura retorno página {pagina}: {list(retorno.keys()) if isinstance(retorno, dict) else type(retorno)}")
        
        if "registros" not in retorno or not retorno["registros"]:
            logger.info(f"Nenhum estoque encontrado na pág {pagina}. Finalizando busca.")
            # DEBUG: Log do conteúdo completo se não há registros
            if isinstance(retorno, dict) and len(str(retorno)) < 500:
                logger.info(f"DEBUG ESTOQUES - Conteúdo completo retorno: {retorno}")
            break
        
        registros = retorno["registros"]
        logger.info(f"DEBUG ESTOQUES - Encontrados {len(registros)} registros na página {pagina}")
        
        # Processar estoques da página
        estoques_salvos_pagina = 0
        for estoque in registros:
            if salvar_estoque_db(conn, estoque):
                estoques_salvos_pagina += 1
        
        total_estoques_salvos += estoques_salvos_pagina
        logger.info(f"Página {pagina}: {estoques_salvos_pagina} estoques salvos")
        
        pagina += 1
        time.sleep(1)  # Rate limiting
    
    logger.info(f"Busca de estoques finalizada. Total de estoques salvos: {total_estoques_salvos}")
    return True

def salvar_estoque_db(conn, estoque):
    """Salva estoque no banco de dados."""
    try:
        id_produto = int(estoque.get("id", 0))
        
        with conn.cursor() as cur:
            # Atualizar estoque total
            cur.execute("""
                INSERT INTO produto_estoque_total (
                    id_produto, nome_produto_estoque, saldo_total_api, 
                    saldo_reservado_api, data_ultima_atualizacao_api
                ) VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (id_produto) DO UPDATE SET
                    nome_produto_estoque = EXCLUDED.nome_produto_estoque,
                    saldo_total_api = EXCLUDED.saldo_total_api,
                    saldo_reservado_api = EXCLUDED.saldo_reservado_api,
                    data_ultima_atualizacao_api = NOW()
            """, (
                id_produto,
                estoque.get("nome", ""),
                safe_float_convert(estoque.get("saldo", 0)),
                safe_float_convert(estoque.get("saldo_reservado", 0))
            ))
            
            # Processar depósitos
            depositos = estoque.get("depositos", [])
            if depositos:
                # Limpar depósitos antigos
                cur.execute("DELETE FROM produto_estoque_depositos WHERE id_produto = %s", (id_produto,))
                
                # Inserir novos depósitos
                for deposito in depositos:
                    cur.execute("""
                        INSERT INTO produto_estoque_depositos (
                            id_produto, nome_deposito, saldo_deposito, 
                            desconsiderar_deposito, empresa_deposito
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        id_produto,
                        deposito.get("nome", ""),
                        safe_float_convert(deposito.get("saldo", 0)),
                        deposito.get("desconsiderar", ""),
                        deposito.get("empresa", "")
                    ))
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar estoque {estoque.get('id', 'N/A')}: {e}")
        return False

def search_pedidos_v2_com_controle(conn, data_filtro_api):
    """Busca pedidos com controle granular de progresso."""
    
    # Para pedidos, verificar se há progresso em andamento (não é busca incremental pura)
    pagina_inicial = inicializar_progresso(conn, PROCESSO_PEDIDOS, data_filtro_api, eh_busca_incremental=False)
    
    if pagina_inicial == "CONCLUIDO":
        logger.info("⏭️ Pedidos já processados completamente. Pulando etapa.")
        return True
    
    logger.info(f"🚀 INICIANDO busca de pedidos desde página {pagina_inicial}")
    
    pagina_atual = pagina_inicial
    total_pedidos_salvos = 0
    total_paginas_api = None
    
    while True:  # Loop infinito, controlado pelas condições internas
        logger.info(f"📄 Processando página {pagina_atual} de pedidos...")
        
        params = {
            "pagina": pagina_atual,
            "dataAlteracao": data_filtro_api
        }
        
        data = make_api_v2_request(ENDPOINT_PEDIDOS_PESQUISA, params)
        if not data or "retorno" not in data:
            logger.error(f"Falha na API para página {pagina_atual}")
            finalizar_progresso(conn, PROCESSO_PEDIDOS, "ERRO", f"Falha na API página {pagina_atual}")
            return False
        
        retorno = data["retorno"]
        
        # Verificar se há pedidos
        if "pedidos" not in retorno or not retorno["pedidos"]:
            logger.info(f"Nenhum pedido encontrado na página {pagina_atual}. Finalizando busca.")
            finalizar_progresso(conn, PROCESSO_PEDIDOS, "CONCLUIDO")
            break
        
        pedidos = retorno["pedidos"]
        total_paginas_api = int(retorno.get("numero_paginas", 1))
        
        # Processar pedidos da página
        pedidos_salvos_pagina = 0
        for pedido_resumo in pedidos:
            if processar_pedido_completo(conn, pedido_resumo):
                pedidos_salvos_pagina += 1
        
        total_pedidos_salvos += pedidos_salvos_pagina
        
        # Atualizar progresso
        atualizar_progresso_pagina(conn, PROCESSO_PEDIDOS, pagina_atual, total_paginas_api, pedidos_salvos_pagina)
        
        logger.info(f"✅ Página {pagina_atual} CONCLUÍDA: {pedidos_salvos_pagina} pedidos salvos")
        
        # Verificar se chegou na última página
        if pagina_atual >= total_paginas_api:
            logger.info(f"🏁 TODAS as páginas processadas! Total: {total_pedidos_salvos} pedidos")
            finalizar_progresso(conn, PROCESSO_PEDIDOS, "CONCLUIDO")
            break
        
        # Verificar limite de segurança para evitar loops infinitos
        if pagina_atual > MAX_PAGINAS_POR_ETAPA:
            logger.warning(f"⚠️ LIMITE DE SEGURANÇA atingido na página {pagina_atual}. Pausando para próxima execução.")
            finalizar_progresso(conn, PROCESSO_PEDIDOS, "EM_ANDAMENTO", f"Pausado na página {pagina_atual}")
            break
        
        pagina_atual += 1
        time.sleep(1)  # Rate limiting
    
    return True

def processar_pedido_completo(conn, pedido_resumo):
    """Processa um pedido completo obtendo detalhes da API."""
    try:
        id_pedido = pedido_resumo.get("id")
        if not id_pedido:
            return False
        
        # Obter detalhes completos do pedido
        params = {"id": id_pedido}
        data = make_api_v2_request(ENDPOINT_PEDIDO_OBTER, params)
        
        if not data or "retorno" not in data:
            logger.warning(f"Falha ao obter detalhes do pedido {id_pedido}")
            return False
        
        pedido_completo = data["retorno"]["pedido"]
        
        # Salvar pedido no banco
        return salvar_pedido_db(conn, pedido_completo)
        
    except Exception as e:
        logger.error(f"Erro ao processar pedido {pedido_resumo}: {e}")
        return False

def salvar_pedido_db(conn, pedido):
    """Salva pedido e seus itens no banco de dados."""
    try:
        id_pedido = int(pedido.get("id", 0))
        
        with conn.cursor() as cur:
            # Inserir/atualizar pedido principal
            cur.execute("""
                INSERT INTO pedidos (
                    id_pedido, numero_pedido, numero_ecommerce, data_pedido, 
                    data_prevista, nome_cliente, valor_pedido, id_vendedor, 
                    nome_vendedor, situacao_pedido, codigo_rastreamento
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
                    codigo_rastreamento = EXCLUDED.codigo_rastreamento
            """, (
                id_pedido,
                pedido.get("numero", ""),
                pedido.get("numero_ecommerce", ""),
                pedido.get("data_pedido", ""),
                pedido.get("data_prevista", ""),
                pedido.get("nome_cliente", ""),
                safe_float_convert(pedido.get("valor_pedido", 0)),
                int(pedido.get("id_vendedor", 0)) if pedido.get("id_vendedor") else None,
                pedido.get("nome_vendedor", ""),
                pedido.get("situacao", ""),
                pedido.get("codigo_rastreamento", "")
            ))
            
            # Processar itens do pedido
            itens = pedido.get("itens", [])
            if itens:
                # Limpar itens antigos
                cur.execute("DELETE FROM pedido_itens WHERE id_pedido = %s", (id_pedido,))
                
                # Inserir novos itens
                for item in itens:
                    cur.execute("""
                        INSERT INTO pedido_itens (
                            id_pedido, id_produto_tiny, codigo_produto_pedido, 
                            descricao_produto_pedido, quantidade, unidade_pedido, 
                            valor_unitario_pedido, id_grade_pedido
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        id_pedido,
                        int(item.get("id_produto", 0)) if item.get("id_produto") else None,
                        item.get("codigo", ""),
                        item.get("descricao", ""),
                        safe_float_convert(item.get("quantidade", 0)),
                        item.get("unidade", ""),
                        safe_float_convert(item.get("valor_unitario", 0)),
                        item.get("id_grade", "")
                    ))
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar pedido {pedido.get('id', 'N/A')}: {e}")
        return False

# ===== FUNÇÃO PRINCIPAL =====
def main():
    """Função principal do script."""
    logger.info("=== Iniciando Cliente API v2 Tiny ERP - MODO PRODUÇÃO (Lógica Híbrida) ===")
    
    # Conectar ao banco
    conn = get_db_connection()
    if not conn:
        logger.error("Falha na conexão com o banco de dados. Encerrando.")
        return False
    
    try:
        # Criar tabelas
        criar_tabelas_db(conn)
        
        # PASSO 1: Categorias
        logger.info("--- PASSO 1: Categorias ---")
        if not get_categorias_v2(conn):
            logger.error("Falha no passo 1 (Categorias).")
        else:
            logger.info("Passo 1 (Categorias) concluído.")
        
        logger.info("----------------------------------------------------------------------")
        
        # PASSO 2: Produtos
        logger.info("--- PASSO 2: Produtos (Cadastrais e Categorias) ---")
        data_filtro_produtos = determinar_data_filtro_inteligente(conn, PROCESSO_PRODUTOS, DIAS_JANELA_SEGURANCA)
        logger.info(f"Iniciando busca de produtos (cadastrais) desde: {data_filtro_produtos}.")
        
        if search_produtos_v2_com_controle(conn, data_filtro_produtos):
            set_ultima_execucao(conn, PROCESSO_PRODUTOS)
            logger.info("Passo 2 (Produtos) concluído.")
        else:
            logger.error("Falha no passo 2 (Produtos).")
        
        logger.info("----------------------------------------------------------------------")
        
        # PASSO 3: Estoques
        logger.info("--- PASSO 3: Estoques ---")
        data_filtro_estoques = determinar_data_filtro_inteligente(conn, PROCESSO_ESTOQUES, DIAS_JANELA_SEGURANCA)
        logger.info(f"Iniciando busca de estoques desde: {data_filtro_estoques}.")
        
        if get_estoques_v2(conn, data_filtro_estoques):
            set_ultima_execucao(conn, PROCESSO_ESTOQUES)
            logger.info("Todas págs estoques processadas.")
        else:
            logger.error("Falha no passo 3 (Estoques).")
        
        logger.info("----------------------------------------------------------------------")
        
        # PASSO 4: Pedidos
        logger.info("--- PASSO 4: Pedidos e Itens ---")
        data_filtro_pedidos = determinar_data_filtro_inteligente(conn, PROCESSO_PEDIDOS, DIAS_JANELA_SEGURANCA)
        logger.info(f"Iniciando busca de pedidos alterados desde: {data_filtro_pedidos}.")
        
        if search_pedidos_v2_com_controle(conn, data_filtro_pedidos):
            set_ultima_execucao(conn, PROCESSO_PEDIDOS)
            logger.info("Passo 4 (Pedidos) concluído.")
        else:
            logger.error("Falha no passo 4 (Pedidos).")
        
        logger.info("----------------------------------------------------------------------")
        logger.info("=== SINCRONIZAÇÃO CONCLUÍDA ===")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro geral na execução: {e}", exc_info=True)
        return False
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Conexão com banco de dados fechada.")

if __name__ == "__main__":
    main()

