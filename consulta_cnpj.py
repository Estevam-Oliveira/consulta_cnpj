
import polars as pl
import os
import zipfile
import shutil
import logging
from dotenv import load_dotenv
from typing import Union, Tuple, List
from datetime import datetime

# Configuração do Logging
logging.basicConfig(
    filename='app_log.log',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Importar as variávens de ambientes
load_dotenv()

# --- 1. CONFIGURAÇÃO LOCAL ---
# ATENÇÃO: SUBSTITUA ESTE CAMINHO PELO SEU CAMINHO LOCAL ONDE OS DADOS ESTÃO!
PASTA_RAIZ_CNPJ = os.getenv("PASTA_RAIZ_CNPJ")

if not PASTA_RAIZ_CNPJ or not os.path.exists(PASTA_RAIZ_CNPJ):
    raise EnvironmentError("A variável de ambiente PASTA_RAIZ_CNPJ não está configurada ou o caminho inexiste")

CNPJ_SEARCH_FILE = os.path.join(PASTA_RAIZ_CNPJ, 'cnpj_search.csv')

if not os.path.exists(CNPJ_SEARCH_FILE):
    raise EnvironmentError(f"O Arquivo de consulta '{CNPJ_SEARCH_FILE}' não foi encontrado. Verifique se o arquivo existe no diretório raiz do projeto.")

try:
    df_busca = pl.read_csv(CNPJ_SEARCH_FILE, separator=',', has_header=True, schema={'cnpj_basico': pl.Utf8})
    cnpj_basico_list: List[str] = df_busca.get_column('cnpj_basico').to_list()
    
    if not cnpj_basico_list:
        raise ValueError("O arquivo cnpj_search.csv está vazio ou o header 'cnpj_basico está incorreto.")
    
    print(f"Buscando {len(cnpj_basico_list)} CNPJs Básicos no modo BATCH.")
    print("-" * 50)

except Exception as e:
    logging.error(f"Erro ao ler o arquivo de busca: {e}")
    raise e

# Identificador de BATC para os nomes de diretórios e arquivos
BATCH_ID = "BATCH"

# CNPJ BÁSICO que você quer filtrar
# valor_busca = "51772251"

# Carimbo dos arquivos DDMMYYYY_HHMMSS
dt_datetime = datetime.now().strftime('%d%m%Y_%H%M%S')

# Diretórios
PASTA_PARQUET_FILTRADO = os.path.join(PASTA_RAIZ_CNPJ, 'PARQUET_FILTRADO')
TEMP_UNZIP_DIR = os.path.join(PASTA_RAIZ_CNPJ, 'TEMP_UNZIP')
os.makedirs(PASTA_PARQUET_FILTRADO, exist_ok=True)
os.makedirs(TEMP_UNZIP_DIR, exist_ok=True)

# Diretórios específicos para o resultado final
PARQUET_DIR_EMPRESAS = os.path.join(PASTA_PARQUET_FILTRADO, f'filtered_empresas_{dt_datetime}')
PARQUET_DIR_ESTABELE = os.path.join(PASTA_PARQUET_FILTRADO, f'filtered_estabele_{dt_datetime}')
PARQUET_DIR_SIMPLES = os.path.join(PASTA_PARQUET_FILTRADO, f'filtered_simples_{dt_datetime}')
PARQUET_DIR_SOCIOS = os.path.join(PASTA_PARQUET_FILTRADO, f'filtered_socios_{dt_datetime}')
os.makedirs(PARQUET_DIR_EMPRESAS, exist_ok=True)
os.makedirs(PARQUET_DIR_ESTABELE, exist_ok=True)
os.makedirs(PARQUET_DIR_SIMPLES, exist_ok=True)
os.makedirs(PARQUET_DIR_SOCIOS, exist_ok=True)

DIRETORIOS_INTERMEDIARIOS = [
    PARQUET_DIR_EMPRESAS, PARQUET_DIR_ESTABELE,
    PARQUET_DIR_SIMPLES, PARQUET_DIR_SOCIOS
]

# --- 2. SCHEMAS ---
schema_empresas = {
    'cnpj_basico': pl.Utf8, 'razao_social': pl.Utf8, 'natureza_juridica': pl.Utf8,
    'qualificacao_responsavel': pl.Utf8, 'capital_social': pl.Utf8,
    'porte_empresa': pl.Utf8, 'ente_federativo_responsavel': pl.Utf8,
} 

schema_estabelecimentos = {
    'cnpj_basico': pl.Utf8, 'cnpj_ordem': pl.Utf8, 'cnpj_dv': pl.Utf8,
    'identificador_matriz_filial': pl.Utf8, 'nome_fantasia': pl.Utf8,
    'situacao_cadastral': pl.Utf8, 'data_situacao_cadastral': pl.Utf8,
    'motivo_situacao_cadastral': pl.Utf8, 'nome_cidade_exterior': pl.Utf8,
    'pais': pl.Utf8, 'data_inicio_atividade': pl.Utf8,
    'cnae_fiscal_principal': pl.Utf8, 'cnae_fiscal_secundaria': pl.Utf8,
    'tipo_logradouro': pl.Utf8, 'logradouro': pl.Utf8,
    'numero': pl.Utf8, 'complemento': pl.Utf8,
    'bairro': pl.Utf8, 'cep': pl.Utf8, 'uf': pl.Utf8,
    'municipio': pl.Utf8, 'ddd_1': pl.Utf8, 'telefone_1': pl.Utf8,
    'ddd_2': pl.Utf8, 'telefone_2': pl.Utf8, 'ddd_fax': pl.Utf8,
    'fax': pl.Utf8, 'correio_eletronico': pl.Utf8,
    'situacao_especial': pl.Utf8, 'data_situacao_especial': pl.Utf8,
} 

schema_simples = {
    'cnpj_basico': pl.Utf8, 'opcao_pelo_simples': pl.Utf8,
    'data_de_opcao_pelo_simples': pl.Utf8, 'data_de_exclusao_do_simples': pl.Utf8,
    'opcao_pelo_mei': pl.Utf8, 'data_de_opcao_pelo_mei': pl.Utf8,
    'data_de_exclusao_do_mei': pl.Utf8,
}

schema_socios = {
    'cnpj_basico': pl.Utf8, 'identificador_de_socio': pl.Utf8,
    'nome_do_socio_razao_social': pl.Utf8, 'cnpj_cpf_do_socio': pl.Utf8,
    'qualificacao_do_socio': pl.Utf8, 'data_entrada_sociedade': pl.Utf8,
    'pais': pl.Utf8, 'representate_legal': pl.Utf8, 'nome_representante_legal': pl.Utf8,
    'qualificacao_representante_legal': pl.Utf8, 'faixa_etaria': pl.Utf8,
}

# --- 3. FUNÇÃO DE PROCESSAMENTO MODULAR ---
def process_cnpj_file(file_type: str, num_files: int, schema: dict, internal_identifier: Union[str, Tuple[str, ...]], output_dir: str, **kwargs):
    """
    Processa os arquivos CNPJ em um loop Eager.
    Filtra pela lista de CNPJs básicos fornecidos em 'valores_busca'
    """
    processed_count = 0
    valores_busca = kwargs.get('valores_busca')
    
    # Limpa a pasta de saída antes de começar, para garantir que não haja arquivos antigos
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Converte o identificador para uma tupla para facilitar a busca por sufixo
    if isinstance(internal_identifier, str):
        search_suffixes = (internal_identifier,)
    else:
        search_suffixes = internal_identifier
    
    #Converte a lista de busca para uma list no Polars para filtro otimizado
    if not valores_busca:
        print("AVISO: Lista de CNPJs Básicos vazia. Pulando processamento.")
        return
        
    
    for i in range(num_files):
        file_name_zip = f'{file_type}{i}.zip'
        file_path_zip = os.path.join(PASTA_RAIZ_CNPJ, file_name_zip)

        if os.path.exists(file_path_zip):
            print(f"  [{i + 1}/{num_files}] Processando: {file_name_zip}")

            try:
                # 1. Descompactar o arquivo CSV interno
                with zipfile.ZipFile(file_path_zip, 'r') as zf:
                    internal_names = zf.namelist()                  
                    csv_file_names = [name for name in internal_names if name.endswith(search_suffixes)]
                    
                    if not csv_file_names:
                        logging.warning(f"Nenhuma correspondência para os sufixos '{search_suffixes}' no ZIP {file_name_zip}. Conteudo: {internal_names}")
                        print(f"   AVISO: Nenhuma correspondência para os sufixos '{search_suffixes}' no ZIP")
                        print(f"   Conteúdo do ZIP: {internal_names}")
                        continue
                                     
                    csv_file_name_internal = csv_file_names[0]
                    zf.extract(csv_file_name_internal, TEMP_UNZIP_DIR)

                csv_file_path_unzipped = os.path.join(TEMP_UNZIP_DIR, csv_file_name_internal)

                # 2. Leitura Eager (pl.read_csv) - USANDO UTF8-LOSSY
                df_eager = pl.read_csv(
                    csv_file_path_unzipped,
                    separator=';',
                    has_header=False,
                    schema=schema,
                    null_values=[''],
                    encoding='utf8-lossy', # <--- IGNORAR BYTES INVÁLIDOS
                )
                
                # 3. Aplicar o filtro no DataFrame Eager
                df_filtered = df_eager.filter(
                    pl.col("cnpj_basico").is_in(valores_busca)
                )

                # 4. Salvar imediatamente para Parquet e liberar a memória
                count = df_filtered.shape[0]
                if count > 0:
                    output_file = os.path.join(output_dir, f'filtered_{file_name_zip.replace(".zip", ".parquet")}')
                    df_filtered.write_parquet(output_file, compression='zstd')
                    print(f"    -> {count} linhas salvas.")
                
                # 5. Limpeza
                os.remove(csv_file_path_unzipped)
                del df_eager
                del df_filtered
                processed_count += 1
                
            except Exception as e:
                logging.error(f"ERRO CRÍTICO ao processar {file_name_zip}. Detalhe: {e}")
                print(f"        ERRO CRÍTICO ao processar {file_name_zip}. Detalhe: {e}")
                # Tentativa de limpar o arquivo temporário em caso de erro
                if 'csv_file_path_unzipped' in locals() and os.path.exists(csv_file_path_unzipped):
                    os.remove(csv_file_path_unzipped)
        else:
            print(f"  Arquivo não encontrado: {file_name_zip}")
            
    print(f"\n[FIM] {file_type} processado. Total de arquivos processados: {processed_count}")

# --- 4. EXECUTAR PROCESSAMENTO ---

# 4.1 Processar Arquivos EMPRESAS
print("--- INICIANDO PROCESSAMENTO: EMPRESAS ---")
process_cnpj_file(
    file_type="Empresas",
    num_files=10, 
    schema=schema_empresas,
    internal_identifier=".EMPRECSV",
    output_dir=PARQUET_DIR_EMPRESAS,
    valores_busca=cnpj_basico_list
)

# 4.2 Processar Arquivos ESTABELECIMENTOS
print("\n--- INICIANDO PROCESSAMENTO: ESTABELECIMENTOS ---")
process_cnpj_file(
    file_type="Estabelecimentos",
    num_files=20, 
    schema=schema_estabelecimentos,
    internal_identifier=(".ESTABELE", ".ESTABICV"), 
    output_dir=PARQUET_DIR_ESTABELE,
    valores_busca=cnpj_basico_list
)

# 4.3 Processar Arquivos SIMPLES
print("\n--- INICIANDO PROCESSAMENTO: SIMPLES ---")
process_cnpj_file(
    file_type="Simples",
    num_files=1,
    schema=schema_simples,
    internal_identifier=("SIMPLES.CSV.D50913"),
    output_dir=PARQUET_DIR_SIMPLES,
    valores_busca=cnpj_basico_list
)

# 4.4 Processar Arquivos SOCIOS
process_cnpj_file(
    file_type="Socios",
    num_files=10,
    schema=schema_socios,
    internal_identifier=".SOCIOCSV",
    output_dir=PARQUET_DIR_SOCIOS,
    valores_busca=cnpj_basico_list
)

# --- 5. LIMPEZA FINAL DA PASTA TEMP ---
if os.path.exists(TEMP_UNZIP_DIR):
    shutil.rmtree(TEMP_UNZIP_DIR)
    print("\n--- Pasta TEMP_UNZIP removida com sucesso. ---")
    
# --- 6. JUNÇÃO DOS DADOS E TABELA FINAL CONSOLIDADA ---

print("\n--- INICIANDO JUNÇÃO E CONSOLIDAÇÃO (BATCH COMPLETO)---")

def scan_or_empty(path: str, schema: dict) -> pl.LazyFrame:
    """ 
    Tenta escanear arquivos Parquet. Se a pasta estiver vazia, retorna um LazyFrame vazio
    com o schema correto, evitando o erro de schema não encontrado
    """
    parquest_file = [f for f in os.listdir(path) if f.endswith('.parquet')]
    if not parquest_file:
        print(f"      AVISO: Pasta {os.path.basename(path)} vasia. Criando DataFrame vazio")
        return pl.DataFrame({}, schema=schema).lazy()
    return pl.scan_parquet(os.path.join(path, "*.parquet"), schema=schema)

try:
    # 1. Carregar Dados
    
    # 1.1 Empresas (Lazy)
    df_empresas_lazy = scan_or_empty(PARQUET_DIR_EMPRESAS, schema_empresas)
    
    # 1.2 Estabelecimentos (Lazy)
    df_estabele_lazy = scan_or_empty(PARQUET_DIR_ESTABELE, schema_estabelecimentos)
    
    # 1.3 Simples (Lazy)
    df_simples_lazy = scan_or_empty(PARQUET_DIR_SIMPLES, schema_simples)
    
    # 1.4 Socios (Lazy)
    df_socios_lazy = scan_or_empty(PARQUET_DIR_SOCIOS, schema_socios)

    # 2. JOIN dos data frames
    # 2.1 JOIN 1: Empresas + Estabelecimentos
    df_consolidado_lazy = df_empresas_lazy.join(
        df_estabele_lazy,
        on="cnpj_basico",
        how="left",
        suffix="_estabele" 
    )
    
    # 2.2 JOIN 2: JOIN 1 + Simples
    df_consolidado_lazy = df_consolidado_lazy.join(
        df_simples_lazy,
        on="cnpj_basico",
        how="left",
        suffix="_simples"
    )
    
    # 2.3 JOIN 3: JOIN 2 + Socios
    df_consolidado_lazy = df_consolidado_lazy.join(
        df_socios_lazy,
        on="cnpj_basico",
        how="left",
        suffix="_socios"
    )

    # 5. Executar a consulta e carregar o resultado consolidado na memória
    df_consolidado_final = df_consolidado_lazy.collect()

    print("\n--- JUNÇÃO CONCLUÍDA ---")
    print(f"Tabela Consolidada (Empresas + Estabelecimentos + Simples + Socios): {df_consolidado_final.shape[0]:,} linhas")
    
    # Tenta construir o CNPJ completo se as colunas existirem
    try:
        cnpj_completo = df_consolidado_final.head(1).select(pl.col("cnpj_basico") + pl.col("cnpj_ordem") + pl.col("cnpj_dv")).item()
        print("CNPJ completo:", cnpj_completo)
    except Exception:
        print("CNPJ completo: Não foi possível gerar a string de CNPJ completo (dados incompletos).")
    
    # 6. Exibir o cabeçalho
    print(df_consolidado_final.head())
    
    # 7. Salvar a tabela final consolidada
    # 7.1 PARQUET
    output_final_file_parquet = os.path.join(PASTA_PARQUET_FILTRADO, f'CONSOLIDADO_CNPJ_{dt_datetime}_COMPLETO.parquet')
    df_consolidado_final.write_parquet(output_final_file_parquet, compression='zstd')
    print(f"\nResultado final em Parquet salvo em: {output_final_file_parquet}")
    
    # 7.2 CSV
    output_final_file_csv = os.path.join(PASTA_PARQUET_FILTRADO, f'CONSOLIDADO_CNPJ_{dt_datetime}_COMPLETO.csv')
    df_consolidado_final.write_csv(output_final_file_csv, separator=';')
    print(f"\nResultado final em CSV salvo em: {output_final_file_csv}")
    
    # 8. Limpeza dos diretórios intermediários
    print("\n--- INICIANDO LIMPEZA DE DIRETÓRIOS ---")
    for d in DIRETORIOS_INTERMEDIARIOS:
        if os.path.exists(d):
            shutil.rmtree(d)
            print(f"Diretório removido: {os.path.basename(d)}")
    print("--- LIMPEZA CONCLUÍDA. ---")

except Exception as e:
    logging.error(f"ERRO NA JUNCAO/CONSOLIDACAO FINAL {e}")
    print(f"ERRO NA JUNÇÃO/CONSOLIDAÇÃO: {e}")
    print("Verifique se as pastas de Parquet foram criadas com sucesso e se contêm arquivos.")