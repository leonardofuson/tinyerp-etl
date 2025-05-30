# Integração Tiny ERP API v2 para PostgreSQL no Render

Este projeto contém um script Python (`tiny_api_v2_cliente.py`) projetado para extrair dados de Categorias, Produtos (incluindo Estoque Total e por Depósito) e Pedidos (incluindo Itens) da API v2 do Tiny ERP. Os dados extraídos são carregados de forma incremental em um banco de dados PostgreSQL hospedado no Render.com para posterior análise e criação de dashboards.

## Funcionalidades Principais

* Extração de Categorias de Produtos (com hierarquia).
* Extração de dados básicos de Produtos.
* Extração de Estoque Total por Produto e Estoque Detalhado por Depósito para cada produto (usando o endpoint `produto.obter.estoque.php`).
* Extração de Pedidos de Venda (cabeçalhos).
* Extração dos Itens de cada Pedido de Venda (usando o endpoint `pedido.obter.php`).
* Carga incremental dos dados (novos e alterados) para Produtos e Pedidos, controlada por timestamps.
* Conexão com banco de dados PostgreSQL.

## Configuração do Ambiente

1.  **Python:** Recomenda-se Python 3.9 ou superior.
2.  **Repositório Git:** Clone este repositório.
3.  **Bibliotecas Python:** Instale as dependências listadas no arquivo `requirements.txt` usando:
    ```bash
    pip install -r requirements.txt
    ```
4.  **Banco de Dados PostgreSQL no Render:**
    * É necessário ter uma instância PostgreSQL ativa no Render.com.
    * Os detalhes de conexão desta instância serão usados nas variáveis de ambiente.
5.  **Variáveis de Ambiente (para execução no Render ou localmente):**
    O script espera as seguintes variáveis de ambiente para funcionar:
    * `TINY_API_V2_TOKEN`: Seu token de acesso para a API v2 do Tiny ERP.
    * `DB_HOST`: Host do seu banco de dados PostgreSQL (fornecido pelo Render).
    * `DB_NAME`: Nome do seu banco de dados PostgreSQL (ex: `tinyv2`).
    * `DB_USER`: Usuário do banco de dados (ex: `tinyv2_user`).
    * `DB_PASSWORD`: Senha do banco de dados.
    * `DB_PORT`: Porta do banco de dados (geralmente `5432` para PostgreSQL).

## Estrutura do Banco de Dados (PostgreSQL)

O script criará e/ou utilizará as seguintes tabelas:

* `categorias`
* `produtos` (informações básicas do produto)
* `produto_estoque_total` (saldo total e reservado por produto, com nome do produto)
* `produto_estoque_depositos` (saldo detalhado por depósito para cada produto, com nome do produto)
* `pedidos` (cabeçalhos dos pedidos)
* `pedido_itens` (itens de cada pedido)
* `script_ultima_execucao` (para controle da carga incremental)

## Implantação e Automação no Render.com

1.  **GitHub:** Certifique-se de que este repositório está atualizado no GitHub.
2.  **Render PostgreSQL:** Crie sua instância PostgreSQL no Render e obtenha as credenciais.
3.  **Render Cron Job:**
    * Crie um novo serviço "Cron Job".
    * Conecte ao seu repositório GitHub.
    * **Environment/Ambiente:** Python (selecione uma versão recente como 3.9, 3.10, 3.11).
    * **Build Command (Comando de Build):** `pip install -r requirements.txt`
    * **Start Command (Comando de Execução):** `python tiny_api_v2_cliente.py`
    * **Schedule (Agendamento):** Defina a frequência (ex: `0 8 * * *` para 8:00 UTC, que é 05:00 no horário de Brasília).
    * **Environment Variables (Variáveis de Ambiente):** Adicione as variáveis listadas na seção "Configuração do Ambiente" com os valores corretos.

## Execução Local (para Testes)

Para executar o script localmente (após instalar as dependências e configurar as variáveis de ambiente, ou ajustando o script para ler credenciais de um arquivo local seguro - não recomendado para produção):
```bash
python tiny_api_v2_cliente.py