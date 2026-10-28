# Versões vigentes e histórico

O núcleo de processamento corresponde às **versões executadas e validadas** abaixo.
As evidências e os documentos de origem incluem histórico identificado. A migração dos alertas
foi concluída e verificada em 28/10/2026, com revisões em relação às propostas iniciais.
Os templates efetivamente aplicados estão em `observabilidade/cloudformation/aplicado_20261028/`.
As propostas iniciais e os documentos anteriores foram preservados como histórico.
As versões deste README, VERSOES e manifesto antes da consolidação estão em
`docs/historico_pacote_antes_consolidacao_20261028/`.

| Componente | Arquivo | Versão | Execução de referência (us-east-2) |
|---|---|---|---|
| Bronze batch | `bronze/bronze_glue.py` | 1.0.0 | `jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700` (20/10/2026, SUCCEEDED; Glue: 146 s / 293 DPU-s; duração interna do manifesto: 94,407 s) |
| Silver batch | `silver/silver_glue.py` | porta do notebook `02_camada_silver.ipynb` | `jr_9c58c50a3e452ab6fff7f8560524b5235aa0d0c92036feccc6768ce2499b0288` (22/10/2026, SUCCEEDED, 219 s) |
| Gold batch | `gold/gold_glue.py` | **1.0.2** | `jr_2f49f1ef178c48d81fd1f61569da095c16d1e58d8f7a15ce11ae9c4f6b84cc29` (26/10/2026, SUCCEEDED, 174 s) |
| Streaming (consumidor Lambda) | `streaming/lambda_function.py` | pipeline 2.0.0 | stack `fiap-inep-streaming` UPDATE_COMPLETE 27/10/2026; run `sim_gold_20261027T155422Z_a52da0a7` |
| Finalizador Silver/Gold de simulação | `streaming/finalize_simulation.py` | 2.0.1 | tentativa `91276d3a-5b02-4c65-a84b-b41cc6601499` (positiva) e `495da030-c41b-4c8e-9b8c-a6101db08f77` (negativa) |
| Notificações | `observabilidade/scripts/configure_notifications.py` | plano `9fb8dd2a…304d9` | aplicado por SDK em 27/10/2026 |
| Modelo FinOps | `finops/modelo_finops.json` | revisão 27/10/2026 18:26 UTC | `PARTIAL_TWO_UNMEASURED_COST_COMPONENTS` |

## Versões superadas (evidências históricas identificadas)

- `gold_glue.py` **1.0.1** (22/10): mesma lógica analítica sem os oito campos de cobertura/comparabilidade da
  `validacao_cruzada_brasil`. Seus relatórios trazem números anteriores à 1.0.2.
- `gold_glue.py` 1.0.2 **tentativa 1** (26/10, run `jr_8544c209aa38c4721b3669728ad5c701a96e60e604c8e33ed536798061470c29`/`failed_attempt1.json`): falhou por `tuple` em
  `Column.isin` (Spark exige `list`). Corrigido antes da execução final. A evidência da falha está em
  `evidencias/gold/v1_0_2_2026_10_26/failed_attempt1.json`.
- Streaming `baseline_v1` (20/10): latência medida só até o PUT Bronze; substituída pela versão 2.0.0 que
  mede até a contribuição Gold.
- Notebooks originais Databricks (`docs/origem/`): usavam `dbutils`/widgets/Delta. Os diffs célula a célula
  contra os scripts Glue estão em `docs/origem/diffs_notebook_vs_glue/`.

## Recurso legado na conta

O job `fiap-ingestao-bigquery-uf` foi o piloto de conexão BigQuery (tabela `uf`). Não faz parte do pipeline.
Em 27/10 seus limites foram reduzidos de 10 G.1X/480 min para 2 G.1X/30 min (`evidencias/finops/controls_report.json`).
Atenção: `deploy/deploy_bronze.py` clona a role e a conexão BigQuery de um job existente (`--source-job`, default este piloto). Antes de removê-lo, aponte `--source-job` para `fiap-bronze-inep-batch`.

## O que precisa de autorização para mudar na nuvem

- Qualquer nova alteração das stacks. A migração autorizada dos alertas já foi concluída
  (`fiap-inep-observabilidade`: IMPORT_COMPLETE; `fiap-inep-streaming`: UPDATE_COMPLETE).
  Não reaplique os templates propostos antigos; consulte as evidências de 28/10.
- Limpar os Parquet *staged* da tentativa falha de 26/10 que permanecem sob `gold/inep/` (consumidores devem ler
  apenas os caminhos de manifesto `SUCCEEDED`; uma entrada do catálogo também exige validar seu
  `fiap_commit_manifest` e a correspondência ao dataset/run aprovado).
- Qualquer nova execução de job, replay ou extração do BigQuery (consome créditos; teto total do projeto US$ 140).

`jr_737aa335…` identifica a falha de join de 22/10, não o erro tuple/isin de 26/10.

O Budget financeiro permanece com contato sem verificação na última evidência; o SNS operacional
confirmado é outro canal. US$140 é teto total; FinOps continua parcial, com duas parcelas n.a.
