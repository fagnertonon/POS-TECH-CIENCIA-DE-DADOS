# Tech Challenge — Pós-Tech FIAP (AI Scientist, turma 1IAST)

Repositório do **Tech Challenge** da Pós-Tech da FIAP. O Tech Challenge é o projeto que reúne os
conhecimentos de todas as disciplinas da fase, desenvolvido em grupo, e corresponde a **90% da nota
final** de cada fase.

**Fase 1 — Case NPS Preditivo.**

---

## Autores

| Nome | E-mail |
|---|---|
| Fagner Carvalho Tonon | fagnertonon@hotmail.com |
| Beatriz Marciliano Sant Anna | bia.santanna13@outlook.com |
| Gustavo Mendes de Oliveira | guhmendes20@hotmail.com |
| Vanessa Silva Menezes | vssamenezes@gmail.com |
| Júlio Wehba Bruna | julio@web-assessoria.com |

---

## Objetivo do projeto

Uma empresa de e-commerce em crescimento acelerado passou a observar **alta variabilidade no Net
Promoter Score (NPS)** entre clientes com indicadores operacionais parecidos: alguns viram
promotores, outros viram detratores. Hoje o NPS só é coletado **depois** do fim da jornada de
compra, o que impede a empresa de antecipar problemas e agir de forma preventiva.

A pergunta central do case:

> **Quais fatores operacionais realmente influenciam a satisfação do cliente, e como a empresa pode
> agir proativamente antes mesmo da aplicação da pesquisa de NPS?**

O trabalho traduz dados de pedidos, logística e atendimento em **recomendações acionáveis** para as
áreas de logística, atendimento, produto e estratégia — com foco em entendimento do problema,
pensamento analítico e storytelling com dados.

---

## Descrição da base de dados

Base histórica com **2.500 pedidos** de e-commerce (um pedido por cliente), contendo dados
cadastrais, do pedido, logísticos e de atendimento.

| Coluna | Descrição |
|---|---|
| `customer_id` | Identificador único do cliente |
| `order_id` | Identificador único do pedido |
| `customer_age` | Idade do cliente |
| `customer_region` | Região geográfica do cliente |
| `customer_tenure_months` | Tempo de relacionamento com a empresa (meses) |
| `order_value` | Valor total do pedido |
| `items_quantity` | Quantidade de itens no pedido |
| `discount_value` | Valor de desconto aplicado |
| `payment_installments` | Número de parcelas do pagamento |
| `delivery_time_days` | Tempo total de entrega (dias) |
| `delivery_delay_days` | Dias de atraso em relação ao prazo prometido |
| `freight_value` | Valor do frete |
| `delivery_attempts` | Número de tentativas de entrega |
| `customer_service_contacts` | Número de contatos com o atendimento |
| `resolution_time_days` | Tempo para resolução de problemas (dias) |
| `complaints_count` | Número de reclamações registradas |
| `repeat_purchase_30d` | Recompra em até 30 dias (0 = não, 1 = sim) |
| `csat_internal_score` | Score interno de satisfação |
| **`nps_score`** | **Nota de satisfação (0 a 10), coletada após a compra — variável alvo** |

A base é **sintética, de estudo**: não possui valores nulos nem linhas duplicadas, e cada cliente
aparece uma única vez. O arquivo não é versionado neste repositório — o notebook o lê de
`data/raw/` ou o baixa automaticamente do repositório da disciplina.

---

## Metodologia

1. **Entendimento do negócio** — definição do problema, da relevância do NPS para um e-commerce e
   das áreas beneficiadas pelos insights (logística, atendimento, produto, pricing).
2. **Definição da target** — `nps_score` como variável de satisfação, com discussão sobre o momento
   de coleta e os riscos de uso inadequado.
3. **Qualidade dos dados** — verificação de nulos, duplicidades e cardinalidade.
4. **Análise exploratória (EDA) com foco em negócio** — a métrica de leitura escolhida foi
   **% de detratores por grupo** (mais direta para a operação do que a nota média), com:
   - classificação NPS (promotor ≥ 9, neutro 7–8, detrator < 7) e cálculo do NPS da empresa;
   - correlação de cada variável operacional com a nota;
   - análise do efeito marginal de atraso, reclamações e contatos com o SAC;
   - cruzamento entre falhas logísticas e de atendimento;
   - comparação entre perfis de cliente (região, idade, tempo de casa).
5. **Controle de vazamento de dados (*data leakage*)** — `csat_internal_score`,
   `repeat_purchase_30d` e `resolution_time_days` foram **excluídas** da análise de fatores por só
   existirem depois do momento em que a empresa precisaria agir. Um teste no notebook mostra que
   `repeat_purchase_30d` é, em 100% dos casos, a própria nota disfarçada (`nps_score >= 8`).
6. **Storytelling gerencial** — tradução dos achados em recomendações para público não técnico.

---

## Principais resultados

- **NPS da empresa: −80.** De cada 100 pedidos, 84 terminam com um detrator, 11 neutros e 4
  promotores — zona crítica de mercado.
- **Os únicos fatores relevantes são operacionais:** atraso na entrega (correlação −0,60),
  reclamações (−0,50) e contatos com o atendimento (−0,35).
- **O tempo total de entrega tem correlação 0,00 com a nota.** O cliente não se importa que a
  entrega demore — se importa que ela **atrase em relação ao prometido**.
- **O ponto de ruptura é o primeiro dia de atraso:** sem atraso, 52% viram detratores; com 1 dia de
  atraso, 74% (+22 p.p.). Os saltos seguintes são menores — a primeira falha é a mais cara.
- **As falhas se somam:** entrega no prazo e nenhum contato com o SAC → 22% de detratores;
  4+ dias de atraso com 3+ contatos → 100%.
- **Não existe "cliente detrator", existe "experiência detratora":** região e faixa etária separam
  os grupos em 2 e 4 p.p., enquanto os dias de atraso separam em 48 p.p.

**Recomendações principais:** rever o prazo prometido no site antes de acelerar a logística; atuar
no primeiro dia de atraso (onde está o volume recuperável) em vez dos casos graves já perdidos;
escalonar o cliente na segunda reclamação; cobrar logística e atendimento no mesmo painel; e não
segmentar ações por perfil de cliente.

**Limitações declaradas:** correlação não é causa; reclamações e contatos ao SAC medem efeitos
sobrepostos; a base não tem datas (sem leitura de sazonalidade); cada cliente aparece uma vez só
(não é possível medir recompra ao longo do tempo); e, por ser sintética, os valores absolutos não
devem ser comparados a benchmarks de mercado.

---

## Estrutura do repositório

```
.
├── 1IAST - Fase 1 - Tech Challenge.pdf     # enunciado do desafio
├── README.md
└── Fase 1/
    ├── 01-analise exploratoria.ipynb       # EDA completa e conclusões
    ├── NPS crítico_ Alavancas de Retenção.pdf   # apresentação executiva (storytelling)
    └── notebooks/
```

Pastas geradas na execução (não versionadas):

```
data/raw/          # base original (.csv)
data/processed/    # base tratada com as colunas derivadas
reports/figures/   # gráficos exportados em .png
```

> **Observação:** o notebook usa caminhos relativos no padrão `../data/...` e `../reports/...`,
> ou seja, espera ser executado a partir de uma subpasta `notebooks/`.

---

## Como reproduzir

Pré-requisitos: **Python 3.10+**.

```bash
# 1. Clonar o repositório
git clone <url-do-repositorio>
cd POS-TECH-CIENCIA-DE-DADOS

# 2. Criar e ativar um ambiente virtual
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 3. Instalar as dependências
pip install pandas matplotlib jupyter

# 4. Abrir o notebook
jupyter lab
```

Abra `Fase 1/01-analise exploratoria.ipynb` e execute **Run → Run All Cells**. Não é necessário
baixar a base manualmente: se o arquivo não estiver em `data/raw/desafio_nps_fase_1.csv`, o
notebook faz o download automaticamente. Ao final, a base tratada é salva em
`data/processed/nps_tratado_simples.csv` e os gráficos em `reports/figures/`.

---

## Entregáveis do desafio

- [x] Tratamento e preparação da base de dados
- [x] Análise exploratória (EDA) com foco em negócio
- [x] Material de apresentação para público não técnico (storytelling gerencial)
- [ ] Modelo preditivo de NPS *(desafio opcional — próxima etapa)*
- [ ] Vídeo executivo de até 5 minutos
