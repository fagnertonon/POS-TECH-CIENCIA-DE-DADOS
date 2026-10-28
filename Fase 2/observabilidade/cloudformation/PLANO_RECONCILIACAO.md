# Reconciliação proposta de observabilidade - não executada

Revisão local: 28/10/2026. Base observada: 27/10/2026, conta `637246457028`,
Ohio (`us-east-2`). Não houve consulta AWS, criação de change set, importação,
alteração IAM/SCP, envio de e-mail ou execução de job para preparar este material.

## Objetivo e propriedade dos recursos

O tópico, a assinatura confirmada, a política SNS, a regra e os dois alarmes batch
existem fora de uma stack de observabilidade na evidência disponível. Os três
alarmes streaming já pertencem à stack `fiap-inep-streaming`; suas ações SNS foram
configuradas posteriormente por SDK. Não se deve importar os mesmos alarmes
streaming em uma segunda stack.

| Proposta / recursos | Destino futuro a avaliar |
|---|---|
| `observabilidade_stack.json`: `AlertTopic`, `AlertSubscription`, `AlertTopicPolicy`, `BatchFailureRule`, `SilverFailureAlarm`, `GoldFailureAlarm` | Adoção/reconciliação individual dos recursos existentes, após verificar suporte de importação e identificadores por tipo. |
| `streaming_stack_com_alarmactions_PROPOSTO.json`: três alarmes existentes | Update da stack streaming atual, sem criar/importar alarmes novos nem substituir fila, role ou função. |

O template de observabilidade possui `DeletionPolicy: Retain` e
`UpdateReplacePolicy: Retain`. Isso preserva recursos diante de remoção/substituição
planejada pelo CloudFormation, mas **não** torna qualquer tipo automaticamente
importável nem impede uma mudança indevida de propriedades.

Não existe nesta entrega um arquivo `resources-to-import` executável: ainda faltam
a confirmação de suporte/identificadores de cada tipo e a conferência atual de
propriedade/drift. Não se deve preencher esses dados por suposição nem resolver
conflitos apagando recursos ou assinando o destinatário novamente.

## O que foi preservado nas propostas locais

- Política SNS completa de `notifications_verified_04.json`, inclusive a declaração
  padrão do proprietário, com conta/ARN parametrizados sem ampliar o escopo.
- EventBridge: mesmo filtro dos três jobs/estados, target `OperationalSNS`,
  `InputTransformer` de metadados operacionais e `RetryPolicy` com
  `MaximumRetryAttempts=3` e `MaximumEventAgeInSeconds=3600`.
- Alarmes Silver/Gold: namespace, métrica, dimensões, limiar, período,
  `DatapointsToAlarm` e comportamento de dados ausentes preservados.
  Descrições antigas foram mantidas para fidelidade ao snapshot: referências
  históricas a "sem destinatários" não são o estado da assinatura após o SDK.
- Streaming: somente `AlertTopicArn`, `ActionsEnabled`, `AlarmActions` e `OKActions`
  nos três alarmes são acrescidos à base histórica. Código Lambda, permissões,
  filas, limites de computação e recursos oficiais não são alterados pela proposta.

As referências usadas estão em `evidencias/observabilidade/raw/`. Os arquivos
originais não foram editados. A validação local compara as propriedades propostas
com esses registros, não com o estado atual do serviço.

## Ordem de uma futura adoção deliberada

1. Obter autorização específica para a operação de nuvem, fora desta correção local.
2. Conferir identidade, região, recursos físicos, propriedade por stack e drift
   usando apenas as leituras necessárias. Conferir a assinatura confirmada pelo ARN
   existente em `notifications_verified_04.json`; não criar outra para importar.
3. Verificar suporte e identificador de importação de **cada tipo** no CloudFormation.
   Se um tipo não admitir importação, registrar a limitação e mantê-lo sob a gestão
   atual até existir estratégia revisada. Não misturar criação, exclusão ou mudança
   de propriedades na mesma operação de importação.
4. Preparar template/manifesto de importação apenas dos tipos confirmados,
   preservando política, target, assinatura e ações. Revisar o change set antes
   de executá-lo. Um template futuro deve abranger o conjunto exato da stack alvo.
5. Após eventual adoção e drift conferido, revisar separadamente um update da
   stack streaming com a proposta que inclui ações SNS. Conferir `AlertTopicArn`,
   propriedade dos três alarmes e ausência de substituições/mudanças em outros
   recursos. Não usar o atualizador histórico de role/função para essa migração.
6. Conferir a configuração final e comparar política/targets/alarmes. Uma nova
   notificação de teste ou confirmação só deve ocorrer mediante autorização
   específica; não é parte automática da importação nem desta entrega.

Este é um plano de preparação, não uma autorização, validação da API ou implantação.
O SDK aplicado em 27/10 continua sendo o último estado comprovado de notificações.

## Regeneração e testes locais

```bash
python streaming/build_proposed_alarm_actions.py --output <arquivo-novo.json>
python -m unittest discover -s tests -p "test_*.py"
```

O gerador recusa sobrescrita e não importa `boto3`. Ele preserva o builder e o
template históricos. O `deploy_simulation.py` continua intencionalmente limitado
à migração de role/função e não aceita modificações de alarmes.

## Referências oficiais usadas na revisão

- [Importação manual de recursos](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/import-resources-manually.html)
- [Retentativas de entrega EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-rule-retry-policy.html)

Essas referências não constituem confirmação de suporte atual de importação para
todos os recursos deste pacote. Nenhuma consulta ao serviço foi realizada.
