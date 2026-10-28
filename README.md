# Pós-Tech FIAP — AI Scientist (turma 1IAST)

Repositório central dos trabalhos da **Pós-Tech da FIAP em Ciência de Dados e Inteligência
Artificial (AI Scientist)**. Aqui ficam reunidas **todas as fases do curso**: cada fase tem sua
própria pasta, com o enunciado, os notebooks, os materiais de apresentação e um `README.md`
próprio descrevendo aquela entrega.

O curso é organizado em fases, e cada fase se encerra com um **Tech Challenge** — projeto em grupo
que integra os conteúdos de todas as disciplinas daquela fase e vale **90% da nota final**.

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

## Fases

| Fase | Tema | Status | Entrega |
|---|---|---|---|
| [Fase 1](Fase%201/README.md) | Case NPS Preditivo — análise exploratória e storytelling com dados | Concluída | EDA + apresentação executiva |
| [Fase 2](Fase%202/README.md) | Pipeline híbrida (batch + streaming) do Indicador Criança Alfabetizada | Em andamento | Arquitetura medalhão em AWS Glue + S3 |

Novas fases serão adicionadas como pastas irmãs (`Fase 2/`, `Fase 3/`, ...), seguindo o mesmo
padrão de organização.

---

## Estrutura do repositório

```
.
├── README.md            # este arquivo — visão geral do curso
├── .gitignore
├── Fase 1/              # Tech Challenge da Fase 1 (NPS)
│   ├── README.md        # descrição completa da entrega
│   ├── notebooks/       # notebooks da análise
│   └── *.pdf            # enunciado e materiais de apresentação
└── Fase 2/              # Tech Challenge da Fase 2 (pipeline híbrida em AWS)
    ├── README.md        # descrição completa da entrega
    ├── bronze/ silver/ gold/   # script da camada + notebook Glue + testes
    ├── streaming/       # produtor, consumidor Lambda, verificadores da simulação
    ├── observabilidade/ # alarmes, notificações e os templates CloudFormation aplicados
    ├── finops/          # modelo de custo, tarifas e scripts de coleta
    ├── deploy/          # publicadores dos jobs e verificadores pós-execução
    ├── evidencias/      # manifestos, runs, falhas preservadas, capturas de console
    ├── docs/            # enunciado, notebooks de origem + diffs, relatórios por etapa
    ├── tests/ tools/    # verificações locais e sincronia notebook↔script
    └── Rascunho/        # embalagens anteriores do pacote (fora do versionamento)
```

A Fase 2 não grava dados localmente: as camadas vivem no S3 e as instruções de execução estão
em [`Fase 2/README.md`](Fase%202/README.md).

Os notebooks ficam em `<Fase>/notebooks/` e gravam suas saídas um nível acima, dentro da própria
pasta da fase. Esses diretórios são criados na execução e **não são versionados** — `data/` e
`reports/` estão no `.gitignore`, já que podem ser regerados rodando o notebook:

```
<Fase>/data/raw/          # bases originais (.csv)
<Fase>/data/processed/    # bases tratadas
<Fase>/reports/figures/   # gráficos exportados em .png
```

---

## Como usar

Pré-requisitos: **Python 3.10+**.

```bash
git clone <url-do-repositorio>
cd POS-TECH-CIENCIA-DE-DADOS
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install pandas matplotlib jupyter
cd "Fase 1/notebooks" && jupyter lab
```

O ambiente virtual fica na raiz e é compartilhado por todas as fases; o Jupyter é aberto de
dentro da pasta do notebook que você quer executar.

Cada fase traz no seu próprio `README.md` as instruções específicas de reprodução — comece por
[`Fase 1/README.md`](Fase%201/README.md).
