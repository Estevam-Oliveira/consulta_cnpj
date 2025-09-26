## CHANGELOG
Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Unreleased]

[v1.0.0] (2025-09-25)
## Adicionado
- Primeira versão funcional.

- Processamento modular dos temas: Empresas, Estabelecimentos, Simples, Sócios.

- Filtro eficiente de CNPJs via Polars DataFrame.

- Junção dos dados em tabela consolidada.

- Salva resultados em Parquet e CSV.

- Uso de variáveis de ambiente para configuração de caminhos.

- Implementação de log detalhado das operações.

- Validação dos arquivos de entrada e da estrutura das pastas.

- Limpeza automática de diretórios intermediários após o fim do processamento.