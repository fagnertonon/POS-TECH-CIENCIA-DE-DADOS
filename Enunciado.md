# Tech Challenge — Fase 2: Pipeline Híbrido para Análise da Alfabetização no Brasil

> Conversão do `POSTECH - IAST - Tech Challenge - Fase 2.pdf` (14 páginas) para Markdown, seguida da
> lista de tarefas derivada dele. O detalhamento passo a passo de cada tarefa está em
> [`Tarefas desafio.md`](Tarefas%20desafio.md).

Projeto integrador da fase: atividade **obrigatória**, preferencialmente em **grupo**, vale **90 % da nota final**.
Simula um problema real de equipes de engenharia e ciência de dados com dados públicos em larga escala.

---

## Contexto do problema

O Compromisso Nacional Criança Alfabetizada mobiliza União, estados, DF e municípios para garantir que as crianças
estejam alfabetizadas até o final do 2º ano do ensino fundamental. Em 2023 o Inep realizou a Pesquisa Alfabetiza
Brasil e definiu o **ponto de corte de 743 pontos** na escala de proficiência do Saeb. Com ele foi criado o
**Indicador Criança Alfabetizada**: percentual de estudantes que atingem esse patamar.

### Atenção à meta de 2030

- A aspiração do Compromisso é a **universalização até 2030**.
- As tabelas oficiais da Base dos Dados trazem uma **trajetória pactuada e mensurável, com valores anuais de 2024 a 2030**.
  Para a rede Pública, a coluna `meta_alfabetizacao_2030` traz o valor **80**.
- As duas leituras são legítimas e significam coisas diferentes. Se calcular distância até a meta, use a **meta
  pactuada da fonte**; a leitura de universalização deve ser identificada como **cenário aspiracional**.
  **Não misture as duas** em pontos diferentes do projeto sem dizer que trocou.

Integrar metas nacionais/estaduais/municipais, dados territoriais, microdados e indicadores permite analisar
desigualdades educacionais e embasar políticas públicas.

## Fonte de dados

Plataforma **Base dos Dados**, dataset `br_inep_avaliacao_alfabetizacao`.

| Entidade | Tabela oficial | Conteúdo |
|---|---|---|
| Dados de alunos | `alunos` | microdado individual: proficiência, presença, preenchimento de caderno, alfabetizado, peso amostral |
| Município | `municipio` | indicador por município, ano, série e rede |
| UF | `uf` | indicador por UF, ano, série e rede |
| Meta Alfabetização Brasil | `meta_alfabetizacao_brasil` | trajetória nacional 2024 a 2030 |
| Meta Alfabetização por UF | `meta_alfabetizacao_uf` | trajetória estadual |
| Meta Alfabetização por Município | `meta_alfabetizacao_municipio` | trajetória municipal |
| Dicionário | `dicionario` | tradução oficial dos códigos de rede, série, presença e demais colunas categóricas |

A tabela `dicionario` existe para **não inventar** a correspondência código ↔ descrição. Use-a.

### Forma de acesso

BigQuery nativo **ou** download direto dos arquivos — as duas são aceitas, sem desconto. Avalia-se se a escolha foi
**consciente e justificada** no README, considerando arquitetura, custo e atualização da fonte. Registrar:

- por que esse caminho foi escolhido;
- o que implica em custo (bytes escaneados, transferência entre nuvens, egress);
- o que acontece quando a fonte for atualizada.

---

## O desafio

Time de engenharia de dados de uma organização pública de análise educacional: construir uma **pipeline híbrida
(Batch + Streaming)** que integre as fontes do indicador de alfabetização, com qualidade, escalabilidade e eficiência
de custo em nuvem, seguindo a **Arquitetura Medalhão** (Bronze, Silver, Gold).

**Objetivo técnico** — pipeline escalável em nuvem que faça:
ingestão de diferentes fontes · tratamento e padronização · integração de bases heterogêneas · camada analítica
confiável · monitoramento operacional · controle de custos.

**Entidades a integrar:** UF, Meta Brasil, Meta UF, Meta Município, Município e Dados de alunos.
**Todas as seis devem ser efetivamente utilizadas.** Tabela ingerida, tratada e nunca usada é custo sem retorno e
será avaliada como tal.

## Arquitetura esperada

### Ingestão batch

Processamento periódico dos dados históricos: metas, municípios e UFs, microdados de alunos e agregados nacionais.

### Ingestão streaming

Simulação de eventos em tempo quase real (atualização de indicadores, novas medições, atualização de metas e
resultados). Simular o fluxo e levar o evento até a camada analítica final **têm o mesmo peso**. Precisa ficar claro:

- o **mecanismo de mensageria/recepção** de eventos, mesmo simulado;
- o **processamento incremental**, em micro-lotes ou contínuo;
- a **diferença de latência, frequência e garantias** em relação ao batch, explicada no README;
- o tratamento de **evento inválido ou duplicado** (mensageria costuma entregar *at-least-once*).

> Se o dado simulado se juntar ao oficial em qualquer camada, mantenha uma **coluna de origem**. Número de
> simulador não pode se misturar ao indicador oficial sem que se consiga separá-los depois.

### Arquitetura Medalhão

| Camada | Requisitos |
|---|---|
| **Bronze** (dados brutos) | dados brutos das fontes; sem transformações significativas; **histórico completo preservado** |
| **Silver** (tratados) | limpeza; tratamento de ausentes; padronização de nomes e tipos; validação de consistência; normalização de chaves; integração das bases |
| **Gold** (analítica) | datasets prontos para análise, p.ex. indicador por município, metas × resultados, evolução temporal; preparada para dashboards, estatística e ML |

- **Preservar histórico** = uma execução não apaga a anterior. Partição por data de ingestão **com sobrescrita do
  caminho inteiro a cada execução não preserva histórico.** Verifique o comportamento real da escrita (`overwrite`,
  `WRITE_TRUNCATE`, `CREATE OR REPLACE`, sobrescrita dinâmica de partição) e descreva a política no README.
- **Guardar o dataset da camada Gold** — será usado na **Fase 3** para treinar um modelo preditivo.

## Regras de qualidade de dados

Mecanismos obrigatórios: verificação de **duplicidade**, detecção de **ausentes**, validação de **chaves de
relacionamento**, **consistência entre tabelas**, **taxa de correspondência dos joins**.

Uma regra só cumpre sua função se **impedir que dado ruim avance**. O projeto deve:

- classificar as regras em **bloqueantes** e **informativas**;
- **interromper** a execução quando uma bloqueante falhar, **antes** de a camada seguinte ser publicada;
- registrar o resultado das validações em lugar que **sobreviva ao fim da execução**.

Cuidado com a exceção levantada corretamente e capturada por um `except` genérico que só registra e deixa o processo
terminar com sucesso. **Demonstre o pipeline falhando** — relatório que só mostra aprovação não prova nada.

**Taxa de correspondência de joins (obrigatória):** join sem par não gera erro, devolve nulo, que se propaga até virar
indicador errado. Após **cada** junção, meça quantas linhas encontraram par e reprove abaixo de um limiar definido e
justificado.

## Monitoramento da pipeline

Monitorar: **falhas de ingestão · latência do pipeline · volume processado · alertas de erro**.

*Logging não é observabilidade.* Para atender o critério:

- métricas de execução (duração por etapa, linhas de entrada e saída, status) **persistidas, acumulando execuções**;
- alguma **notificação** quando o pipeline falhar (alerta na nuvem, e-mail, webhook ou equivalente);
- **latência medida de fato**. No streaming, latência = momento da ingestão − momento em que o evento foi
  **produzido**; se o produtor gravar o mesmo carimbo nos dois campos, a diferença é sempre zero.

O README deve responder: **em produção, quem descobre que o pipeline quebrou, e em quanto tempo?**

## FinOps

Práticas esperadas: armazenamento eficiente (Parquet, compressão, particionamento); otimização de queries (seleção
de colunas, filtro na origem, evitar leitura repetida); controle de recursos computacionais; **estimativa de custo
da arquitetura com números medidos**.

Listar boas práticas não é estimar custo. Apresentar tabela:

| Serviço | Unidade medida | Quantidade | Preço unitário | Custo mensal projetado |
|---|---|---|---|---|

Obter quantidades reais com as ferramentas da nuvem (dry run no BigQuery, dados escaneados no Athena, duração de
job, tamanho dos objetos). Premissa inflada não é conservadorismo, é imprecisão. Se o ambiente for gratuito/sandbox,
informe e **projete o custo do mesmo desenho em produção**, com a cadência pretendida.

**Multi-cloud** é decisão, sem desconto; deve ser justificada, e o README deve discutir o custo: egress, duplicação de
armazenamento, duas contas de billing, dois modelos de IAM, time dominando dois ecossistemas.

## Implementação em cloud

Executar em **AWS, GCP ou Azure**, **de verdade, com evidência**. Script que só valida plano, ou infraestrutura
declarada e nunca provisionada, não conta. Incluir: identificadores de job, prints de console, logs, contagens de
linhas nas tabelas criadas.

## Reprodutibilidade

Um avaliador deve clonar e executar **apenas seguindo o README**. Teste em clone limpo:

- caminhos do README existem e batem com o código;
- arquivos com a extensão correta;
- dependências declaradas **com versão**;
- projeto, bucket e credencial **configuráveis** (variável de ambiente/arquivo), não fixos em vários arquivos;
- **nenhuma credencial versionada**;
- testes, se existirem, passam.

## Formato da entrega

### README deve conter

1. **Contexto do problema** — desafio educacional e uso do indicador.
2. **Arquitetura proposta** — diagrama e fluxo de dados; deve refletir o **implementado**, não o pretendido.
3. **Tecnologias utilizadas** — escolhas e justificativa; distinguir implementado de evolução futura.
4. **Decisões arquiteturais** — batch × streaming, data lake × data warehouse, custo × performance, forma de acesso à fonte.
5. **Monitoramento e FinOps** — como monitora e controla custos, com a tabela de estimativa.
6. **Aplicação em IA** — uso da Gold para predição de alfabetização, desigualdade educacional e políticas públicas.
   Atenção a **vazamento de alvo** em base de features (alvo e coluna derivada dele na mesma tabela).
7. **Limitações conhecidas** — o que ficou de fora, o que foi simulado, lacunas da fonte. Assumir vale mais que esconder.
8. **Link do vídeo executivo.**

**Documentação precisa descrever o projeto:** descrever processo que não existe no repositório (CI não configurado,
alertas não criados, fluxo de branches que o histórico não mostra) custa credibilidade. Prefira "planejado para a
próxima etapa".

### Estrutura mínima do repositório

Código-fonte dos pipelines · camadas bronze/silver/gold claras · scripts de validação e qualidade · documentação
técnica · README completo · evidências de execução.

### Vídeo executivo (até 5 min)

Pelo menos um integrante apresenta em linguagem executiva (liderança/stakeholders): **problema de negócio,
arquitetura, valor para análises educacionais, potencial para IA**. Link no README. Gráficos com eixos rotulados e
números da narração **idênticos** aos do repositório.

## Erros comuns em dados públicos (armadilhas)

- **Join que não casa gera nulo**, não erro → meça a taxa de correspondência após cada junção.
- **Chaves "falam a mesma língua"?** Categorias e tipos podem diferir entre tabelas; compare os valores dos dois lados.
- **Preencher nulo com zero** vira "é zero": distorce médias de proficiência e faz todos "atingirem" a meta. Preserve o nulo.
- **Ausência de meta ≠ não atingiu a meta.** Comparação com nulo é falsa e o `else` classifica errado → use um **terceiro estado**.
- **Média simples de municípios ≠ indicador nacional**, que é **ponderado por alunos**. Se usar média simples, nomeie a coluna assim.
- **Universo de rede e de série:** compare resultado e meta só no **mesmo recorte**.
- **Denominador:** taxa = alunos **avaliados** com **peso amostral**, não total de registros.
- **Fallback apaga informação:** `else` que classifica código desconhecido como categoria válida esconde valor novo; use categoria explícita "desconhecido" com check de contagem zero.
- **Extrapolar com poucos pontos é frágil:** projetar até 2030 com 2–3 anos é cenário, não previsão.

## Critérios de avaliação

| Critério | Pontos |
|---|---|
| Ingestão híbrida (batch + streaming) | 15 |
| Arquitetura Medalhão | 15 |
| Qualidade de dados | 10 |
| Monitoramento e observabilidade | 10 |
| FinOps | 10 |
| Cloud e organização do repositório | 10 |
| README e documentação | 5 |
| Vídeo executivo | 15 |
| **Total** | **90** |

**Diferenciais (não obrigatórios):** validação cruzada microdado × indicador oficial (recalcular a taxa ponderando
pelo peso amostral sobre os avaliados e confrontar com a publicada) · orquestração (dependência, retentativa,
agendamento) · infraestrutura como código · testes automatizados das transformações · investigação documentada de
anomalias no dado.

---

# Tarefas a executar

Numeração igual à de [`Tarefas desafio.md`](Tarefas%20desafio.md), onde cada uma tem *O que / Por que / Como / Pronto quando*.

## Base

- [ ] **1. Grupo, repositório e configuração** — estrutura de pastas, `requirements.txt` com versões, `.env.example`, nada de credencial versionada, IDs de projeto/bucket configuráveis.
- [ ] **2. Provisionar o ambiente em nuvem** — AWS/GCP/Azure de fato criado, com evidência (IDs, prints).
- [ ] **3. Explorar a fonte e decidir a forma de acesso** — BigQuery nativo × download; justificar custo, egress e atualização da fonte.

## Pipeline

- [ ] **4. Bronze (batch)** — ingerir as 6 entidades + `dicionario`; sem transformação significativa; **política de histórico** que não sobrescreve execuções anteriores.
- [ ] **5. Motor de qualidade** — duplicidade, ausentes, chaves, consistência, taxa de join; regras **bloqueantes × informativas**; falha interrompe antes de publicar; resultado persistido.
- [ ] **6. Monitoramento persistido e alertas** — duração por etapa, linhas in/out, status acumulados; notificação em falha.
- [ ] **7. Silver** — limpeza, tipos, nomes, chaves normalizadas, nulo preservado, `dicionario` em vez de códigos inventados, categoria "desconhecido" com check zero.
- [ ] **8. Gold** — indicador por município, metas × resultados (terceiro estado "sem meta"), evolução temporal; média **ponderada**; mesmo recorte rede/série; **guardar para a Fase 3**.
- [ ] **9. Streaming simulado** — mensageria/recepção, micro-lotes, deduplicação (*at-least-once*), evento inválido, **coluna de origem**, latência real (produzido ≠ ingerido) até a Gold.

## Diferenciais e provas

- [ ] **10. Validação cruzada microdado × indicador oficial** *(diferencial)*.
- [ ] **11. Demonstrar a pipeline falhando** — evidência de regra bloqueante interrompendo a execução e do alerta disparado.
- [ ] **12. FinOps** — Parquet/compressão/partição, queries otimizadas, **tabela de custo com números medidos**, projeção para produção.
- [ ] **13. Orquestração, testes e IaC** *(diferenciais)*.

## Entrega

- [ ] **14. Evidências de execução** — job IDs, prints, logs, contagem de linhas.
- [ ] **15. Reprodutibilidade** — teste em clone limpo seguindo só o README.
- [ ] **16. README completo** — as 8 seções acima; diagrama = o implementado; "planejado" ≠ "feito"; responder "quem descobre que quebrou e em quanto tempo?"; seção de IA com cuidado de vazamento de alvo; limitações.
- [ ] **17. Vídeo executivo (≤ 5 min)** — problema, arquitetura, valor, IA; números iguais aos do repositório; link no README.
