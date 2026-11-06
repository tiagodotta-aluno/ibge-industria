Como executar o projeto

Na pasta principal execute:

Passo 1 - Construir as Imagens:

docker compose build

Passo 2 - Iniciar a infraestrutura:

docker compose up -d redis postgres

Passo 3 - Iniciar quatro Workers:

docker compose up -d --scale worker=4 worker

Passo 4 - Iniciar o Manager:

docker compose up manager

O fluxo esperado será:

-> Manager recebe a solicitação do processamento
-> Manager distribui as consultas das tabelas 8880 e 8888
-> Workers executam as consultas simultaneamente
-> Os dados originais são armazenados em JSON e PostgreSQL
-> Manager solicita a análise integrada 
-> Um Worker executa a análise
-> Os resultados são disponibilizados em CSV e JSON

Para acompanhar a execução:

docker compose logs -f manager

Para acompanhar os Workers:

docker compose logs -f worker

Para encerrar:

docker compose down

Para encerrar e também remover o banco persistido:

docker compose down -v

Resultados esperados:

O projeto produzirá estes arquivos:

-> sidra_8880.json           Dados originais da PMC
-> sidra_8888.json           Dados originais da produção industrial
-> dados_integrados.csv      Dados organizados por período e série
-> relatorio.json            Indicadores e relação exploratória









