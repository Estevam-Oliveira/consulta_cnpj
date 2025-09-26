<<<<<<< HEAD
# consulta_cnpj
Script para pesquisar dados de cartão CNPJ dos dados aberto da receita federal do Brasil.
=======
## README
# Consulta CNPJ em Batch
Ferramenta Python para processar arquivos oficiais da Receita Federal e filtrar dados empresariais por lista de CNPJs básicos, consolidando resultados em formatos Parquet e CSV para análises eficientes.

# Funcionalidades
 * Processamento de arquivos ZIP e CSV extraídos da Receita Federal.

 * Filtro por lista personalizada de CNPJs.

 * Conversão e consolidação em formatos Parquet e CSV.

 * Junção de dados dos seguintes temas:

    - Empresas

    - Estabelecimentos

    - Simples Nacional

    - Sócios

* Registro detalhado do processo via arquivo de log.

* Configuração por variáveis de ambiente usando .env.

# Instalação
1. Clone o repositório.

2. Instale dependências executando o requirements.txt:
```bash
pip install -r requirements.txt
``` 

3. Crie um arquivo .env na raiz e defina:
```bash
PASTARAIZCNPJ=/caminho/para/dados
```

4. Adicione na pasta indicada os arquivos ZIP originais e o cnpjsearch.csv com a coluna cnpjbasico.

# Uso
1. Execute o script principal:
```bash
python consulta_cnpj.py
```

2. O script irá processar os arquivos, filtrar e consolidar os dados conforme a lista de CNPJs do arquivo cnpjsearch.csv.

3. Os resultados serão salvos nas subpastas:

    - PARQUETFILTRADO/filtered* (Parquet intermediário)

    - PARQUETFILTRADO/CONSOLIDADOCNPJ* (final consolidado, Parquet e CSV)

# Estrutura esperada:
 - Pasta de dados da Receita Federal conforme padrão

 - Arquivo de busca: cnpjsearch.csv

 - .env configurado
```bash
PASTA_RAIZ_CNPJ
```

# Dependências
 - Python >= 3.8

 - polars

 - python-dotenv

 - logging (padrão)

# Log
O script gera o arquivo applog.log com o fluxo de processamento e eventuais erros.

# Licença
MIT – Veja o arquivo LICENSE para mais detalhes.
>>>>>>> 13aba0e (Commit Start Project)
