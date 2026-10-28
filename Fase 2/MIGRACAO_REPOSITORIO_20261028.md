# Migração do pacote para o repositório do grupo — 28/10/2026

O pacote `Projeto_FIAP_Completo_20261028/fiap-tc-fase2-alfabetizacao` passou a ser **a entrega da
Fase 2**, na raiz desta pasta. Antes dele existia aqui um esqueleto paralelo que descrevia uma
arquitetura diferente e nunca executada; ele foi removido, porque duas descrições incompatíveis do
mesmo projeto custam mais credibilidade do que qualquer uma delas entrega.

Nenhum serviço de nuvem foi chamado nesta migração: nenhum job, replay, e-mail, extração ou recurso
novo. Os dados, manifestos, capturas e relatórios do pacote estão intactos.

## O que foi descartado

O esqueleto anterior propunha **Kinesis Data Streams + Glue Structured Streaming** para o streaming,
um template CloudFormation próprio para os alarmes, e uma tabela de FinOps com duas quantidades
medidas. Nada disso havia sido executado na AWS. O pacote traz, para os mesmos requisitos,
**SQS + Lambda executado em 27/10**, duas stacks CloudFormation efetivamente aplicadas em 28/10 e um
modelo de custo com 19 parcelas medidas. Entre a versão implementada e a versão executada, fica a
executada.

O código batch das duas versões era **idêntico byte a byte** — `bronze_glue.py`, `silver_glue.py` e
`gold_glue.py` não mudaram em nada nesta migração.

## O que mudou em relação ao `SHA256_Arquivos.json`

O manifesto de integridade **não foi regerado**: ele registra o pacote como foi entregue em 28/10.
Vinte e um dos 400 arquivos divergem dele hoje: os 17 com e-mail mascarado, descritos na seção
[E-mails institucionais mascarados](#e-mails-institucionais-mascarados), e estes quatro:

| Arquivo | O que mudou |
| --- | --- |
| `README.md` | seção 9 passou a citar a reexecução dos testes nesta migração e a ferramenta de sincronia; seção 12 ganhou `tools/` |
| `bronze/camada_bronze_glue.ipynb` | markdown apontava `bronze_glue.py` e `env.example` sem o caminho da pasta |
| `silver/02_camada_silver_glue.ipynb` | markdown apontava o notebook de origem em `original/02_camada_silver.ipynb`, caminho que não existe; o original está em `silver/02_camada_silver_original.ipynb` e em `docs/origem/02_camada_silver.ipynb` |
| `.gitignore` | passou a ignorar `Rascunho/` e os sete arquivos da seção "O que fica fora do repositório" |

Nos notebooks, apenas texto em célula markdown: **nenhuma célula de código foi tocada**, e as células
de código dos três continuam idênticas aos scripts das camadas.

Cinco arquivos existem fora do manifesto: este documento, o PDF do enunciado — que já estava
versionado na pasta antes do pacote — e os três utilitários descritos a seguir.

Para conferir tudo isso sem acreditar nesta lista:

```bash
python tools/verificar_integridade.py --detalhe
```

## O que foi acrescentado

- `tools/sync_notebooks.py` e `tests/test_sincronia_notebooks.py` — travam notebook e script da camada
  no mesmo código. O avaliador lê o notebook; notebook desatualizado é código errado na prática.
- `tools/verificar_integridade.py` — confere os 400 arquivos contra o manifesto.

## Testes depois da migração

As suítes documentadas na seção 9 do README foram reexecutadas num venv criado do zero a partir de
`requirements.txt`, com as versões fixadas resolvendo na instalação (boto3 1.43.115, PyArrow 26.0.0,
pandas 3.0.1):

| Suíte | Testes | Resultado |
| --- | ---: | --- |
| `silver` | 7 | OK |
| `gold` | 24 | OK |
| `streaming` | 96 | OK |
| `observabilidade/scripts` (só `test_configure_notifications.py`) | 5 | OK |
| `tests` (6 de IaC + 2 de sincronia) | 8 | OK |
| **Total** | **140** | **0 falhas** |

Rodados com credenciais AWS inválidas no ambiente, para que qualquer chamada real falhasse em vez de
passar despercebida. `observabilidade/scripts/test_alarm_delivery.py` foi deixado de fora, como o
README manda: ele força um alarme real e envia e-mail.

Isso **não** é o teste em clone limpo que o enunciado pede — é a mesma máquina, com o repositório já
presente. A pendência continua aberta.

## O que fica fora do repositório

Sete arquivos do manifesto continuam no disco, mas o `.gitignore` desta pasta os exclui do
versionamento. `tools/verificar_integridade.py` os reporta como **EXCLUIDO**, não como ausentes, e
continua saindo com 0 num clone — com ou sem git, inclusive numa cópia baixada como ZIP.

| Arquivo | Por que não sobe |
| --- | --- |
| `docs/contexto_historico/Contexto_Retomada_FIAP_20261028.txt` e `…_Migracao.txt` | Instruções para retomar uma sessão com assistente de IA ("guardar as informações para alterar somente quando o usuário pedir"). Não descrevem o projeto ao avaliador. |
| `docs/relatorios/historico/Compilado_Conversa_FIAP.pdf` | Compilação da conversa com o assistente (317 mensagens). Cita os e-mails institucionais de dois integrantes. |
| `evidencias/migracao_cloudformation_20261028/aws_evidence_original.zip` | Os 44 itens do ZIP estão extraídos em `aws_evidence/`, byte a byte. |
| `evidencias/migracao_cloudformation_20261028/migration_bundle.zip` e os dois de `attempt_01/` e `attempt_02/` | Cada item tem cópia idêntica na árvore. Os quatro ZIPs repetiam 64 arquivos e o e-mail do destinatário SNS sem máscara. |

As duplicatas foram conferidas por SHA256 de cada item contra todos os arquivos da árvore, não por
nome. O `LEIA-ME_Migracao.txt` cita o hash do `aws_evidence_original.zip` como âncora de que o export
não foi alterado; essa garantia continua de pé, porque o `SHA256_Arquivos.json` registra o hash de cada
um dos 44 arquivos extraídos.

Depois da exclusão, as cinco suítes foram rodadas de novo numa cópia contendo **só** os arquivos que
irão para o repositório: 140 testes, 0 falhas, e notebooks sincronizados. Nenhum teste dependia dos
arquivos excluídos.

O `.gitignore` da raiz também ganhou regras para credenciais e chaves (`.aws/`, `*.pem`, `*.key`,
`service-account*.json`, `client_secret*.json`, estado de Terraform) e para arquivos de sistema e de
editor. Nenhuma delas casa com arquivo do projeto: o conjunto a versionar mudou exatamente nos sete
arquivos acima.

## E-mails institucionais mascarados

Os e-mails institucionais de dois integrantes (`rm` + matrícula `@fiap.com.br`) apareciam sem máscara
em 17 arquivos versionáveis. Todos foram trocados por `r***@fiap.com.br`, a mesma máscara que o pacote
já usava em `evidencias/observabilidade/recipient_receipt.json`.

| Onde | Arquivos | Ocorrências | Como |
| --- | ---: | ---: | --- |
| JSON de evidência da migração CloudFormation e o template aplicado em `observabilidade/cloudformation/aplicado_20261028/` | 14 | 26 | troca literal do texto, em bytes; cada arquivo continua JSON válido |
| `evidencias/observabilidade/plano_notificacao.png` | 1 | 2 | caixa na cor do fundo do terminal com o texto mascarado; só a região (28,194)–(257,307) da imagem mudou |
| `docs/relatorios/Relatorio_Completo_Observabilidade_FIAP.pdf` | 1 | 3 no texto + 1 imagem | redação real: o texto original foi **removido** do PDF, não só coberto; a captura embutida na página 7 é a mesma de cima e recebeu a mesma máscara |
| `docs/relatorios/historico/Relatorio_Completo_Tech_Challenge_AWS_2026_10_22.pdf` | 1 | 1 | redação real, como acima |

Conferências feitas depois da troca:

- nenhum arquivo que vai para o repositório contém `rm` seguido de matrícula — texto, JSON e o texto
  extraído dos PDFs;
- nos dois PDFs, o texto extraído é idêntico ao original palavra por palavra, fora o e-mail, e o número
  de páginas não mudou;
- das 58 capturas de tela versionáveis, 25 foram abertas e lidas: todas as de terminal (CloudShell
  da AWS e do Google), assinatura SNS, orçamento, faturamento, créditos e identidade, além das três
  imagens embutidas no PDF de observabilidade. Só `plano_notificacao.png` exibia e-mail. As outras 33
  são telas de execução do Glue, do S3, da Lambda, do SQS e do CloudWatch e **não** foram abertas
  uma a uma;
- os 14 JSON mascarados voltam exatamente ao SHA256 do manifesto quando a máscara é trocada pelo
  endereço original — conferido arquivo por arquivo.

**Quem tem o original consegue conferir a máscara.** Nos JSON, a única diferença é o e-mail: trocando
`r***@fiap.com.br` pelo endereço real, o SHA256 volta a bater com o `SHA256_Arquivos.json`. O
`recipient_receipt.json` guarda o SHA256 do endereço do destinatário (`48c4f695…`), o que permite
conferir que se trata da mesma pessoa sem publicar o endereço.

**Consequência conhecida — uma das sete verificações da migração passa a reprovar.**
`evidencias/migracao_cloudformation_20261028/test_migration_guards.py` não faz parte das suítes do
README, mas é a guarda da migração: `verify_files()` recusa qualquer arquivo cujo hash difira do plano
revisado, e o `import_template.json` e o `before.json` dessa pasta foram mascarados. O resultado é
`test_file_drift_invalidates_the_reviewed_plan` em erro, com `Changed migration source: import_template.json`.

A guarda está fazendo exatamente o trabalho dela: os arquivos mascarados não são mais os que foram
revisados e **não devem ser usados para implantar nada**. Com os dois originais de volta, as sete
verificações passam — conferido numa cópia da pasta. Pelo mesmo motivo, os hashes desses 17 arquivos em
`evidencias/migracao_cloudformation_20261028/SHA256.json`, em `integridade_preservacao.json` e no
`migration_plan.json` registram as versões com o e-mail. O arquivo em
`observabilidade/cloudformation/aplicado_20261028/import_template.json` deixou de ser byte a byte o
template aplicado: a única diferença é o `Endpoint` da assinatura SNS.

Os 17 originais estão em `Rascunho/originais_antes_da_mascara_20261028/`, com a lista em `LISTA.txt`,
fora do versionamento.

**O que não foi mascarado**, por ser informação que a equipe já publica ou que a avaliação exige: o
nome de exibição da conta AWS nas capturas, que é de um integrante listado como autor; o número da
conta AWS, presente nos ARNs que servem de evidência; o nome de usuário do Cloud Shell do Google em
`evidencias/finops/gcp_resultado_console.png` e a foto de perfil no canto das capturas do GCP; e o
caminho local `/Users/…` numa saída de erro de `docs/origem/camada_bronze.ipynb`.

## Onde foi parar o resto da embalagem

`Rascunho/` guarda o que sobrou da embalagem original — o ZIP da versão anterior, o
`ABRA_PRIMEIRO.txt` (cópia idêntica de `LEIA-ME_PROJETO_ATUAL.txt`) e o `VERIFICACAO_EMBALAGEM.json`.
Está fora do versionamento pelo `.gitignore`: nada foi apagado do disco.
