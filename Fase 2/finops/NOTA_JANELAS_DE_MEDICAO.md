# Nota sobre as janelas de medição do modelo FinOps

Esta nota acompanha `modelo_finops.json` (revisão de 27/10/2026, 18:26 UTC) e a planilha
`evidencias/finops/FinOps_FIAP_Medicoes_Atualizado.xlsx`. Ela existe para que ninguém leia
uma quantidade como "medida por ciclo" quando ela foi observada em outra base.

## Bases diferentes convivem na mesma tabela

| Base (`basis`) | O que significa | Parcelas |
|---|---|---|
| `per_cycle` | Medida em uma execução real do componente (DPU-segundos do job, GB-segundo do Lambda, bytes lógicos das tabelas) e multiplicada pela cadência hipotética de 4 ciclos/30 dias. | Glue Bronze/Silver/Gold, Lambda, métricas customizadas, leitura BigQuery |
| `observed_period` | Soma do Cost Explorer (usage types, `Estimated=true`) entre 21/10 e 27/10 (seis dias UTC completos) e projetada proporcionalmente para 30 dias. | S3 PUT/GET, SQS, Secrets API, catálogo, CloudWatch Logs ingestão, **S3 entrada** |
| `monthly` / `storage_growth` | Estoque no dia da coleta (objetos S3, grupos de log, alarmes, segredo). | S3 armazenamento, arquivo de logs, alarmes, segredo |
| `pending` | **Não medido.** Fica `n.a.`; nunca é imputado zero. | Egress GCP → AWS; crescimento comprimido do arquivo de logs |

## Limitação da janela `observed_period`

A janela de 21 a 27/10 **não contém nenhuma execução Bronze nem replay de streaming**.
Contém a execução Gold 1.0.2 (26/10), as coletas de FinOps e as sessões de verificação.
Consequências:

- As contagens de PUT/GET/SQS/catálogo **subestimam** a carga de um mês com quatro ciclos completos.
  São um piso observado, não a quantidade marginal por ciclo.
- A linha **"Amazon S3 – Entrada comum de dados" = 0 GB** é um zero *observado nessa janela*, não um
  zero medido do desenho. O manifesto Bronze aprovado registra **80.065.708 bytes Parquet** nas sete
  tabelas (soma de `table_metrics[].parquet_bytes`, em `evidencias/bronze/manifesto_bronze.json`).
  Esse valor mede a saída comprimida daquele snapshot: não mede entrada de rede, bytes BigQuery ou egress.
  O inventário geral do lake inclui outras camadas e não substitui essa medição. A tarifa de ingresso
  comum preservada no modelo é zero; a quantidade daquela linha continua restrita à janela CE observada.
- Uma atribuição representativa por ciclo precisaria de registros de todas as etapas com escopo
  correlacionado. Cost Explorer por dia, sozinho, não isola um job/replay quando há outras atividades.
  Não foi iniciado novo ciclo, replay ou coleta para esta correção: as lacunas continuam explícitas e
  o modelo financeiro permanece byte a byte como recebido, sem transformar estimativa em medição.

## Como ler o subtotal

`US$ 2,1507 / 30 dias` é o subtotal das 19 parcelas numéricas. Não é o custo completo do desenho
(duas parcelas pendentes) nem prova de aderência ao teto total de US$ 140 do projeto. Os créditos exibidos em
27/10 (US$ 136,59) não pagam o lado Google Cloud, cujo projeto consumidor `fiap1-512214` está sem conta de
faturamento na fotografia consultada (`evidencias/finops/gcp_resultado_console.png`). A investigação
não recuperou o volume faturável nem a rota/SKU de egress. Billing não vinculado no consumidor não
comprova ausência global de custos/registros em outros projetos. Egress permanece n.a., não zero.

O Budget AWS está configurado, mas o último contato financeiro observado era `0/1 verified; Inactive`;
a entrega não foi comprovada. Isso não altera a confirmação do SNS operacional, que é outro canal.
A correção local de 28/10 não atualizou faturamento, créditos ou preços.
