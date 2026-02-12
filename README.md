# Exemplos do Curso Apache Kafka

Repositório com os exemplos práticos do curso **Apache Kafka (~6h)**. Aqui você exercita produção e consumo de mensagens, particionamento, confiabilidade, serialização e integração com Schema Registry, seguindo a sequência das aulas.

## Sobre o curso
Neste curso, você aprende a trabalhar com o Apache Kafka para processar grandes volumes de dados em tempo real. Os exemplos mostram arquitetura, técnicas de produção/consumo e integração de dados.

Principais tópicos:
- Introdução ao Kafka e casos de uso
- Arquitetura: brokers, topics, partições e replicação
- Producers e consumers: configuração, particionamento e garantias de entrega
- Kafka Connect: conectores source/sink e transformações de dados
- Schema Registry: gestão e evolução de esquemas (Avro/Protobuf/JSON Schema)

Habilidades ao final: configurar Kafka para produção e consumo confiáveis, integrar sistemas com Connect e manter compatibilidade de dados via Schema Registry.

## Estrutura dos exemplos
Cada pasta corresponde a um capítulo/prática. Use o `Producer.csproj` ou `Consumer.csproj` dentro de cada módulo conforme indicado.

- `02-02-producer/`: produtor básico
- `02-03-consumer/`: consumidor básico
- `02-06-partitions/Producer` e `02-06-partitions/Consumer`: uso de partições
- `03-01-acks/Producer`: configurações de acknowledgments
- `03-02-buffer-linger/Producer`: buffer e linger
- `03-03-retries/Producer`: políticas de retry
- `03-04-max-in-flight/Producer`: controle de mensagens em voo
- `03-05-idempotencia/Producer`: idempotência
- `03-06-serializer/Producer`: serialização customizada
- `03-07-compression/Producer`: compressão
- `04-02-heartbeats/Producer` e `04-02-heartbeats/Consumer`: heartbeats do consumer group
- `04-03-commit/Producer` e `04-03-commit/Consumer`: commits de offset
- `04-04-store-offset/Producer` e `04-04-store-offset/Consumer`: armazenamento de offsets
- `04-05-auto-offset-reset/Consumer`: política de auto offset reset
- `04-06-deserializer/Producer` e `04-06-deserializer/Consumer`: desserialização customizada
- `05-04-avrogen/Producer` e `05-04-avrogen/Consumer`: geração de classes Avro
- `05-05-producer-avro/Producer`: producer com Avro + Schema Registry
- `05-06-consumer-avro/Consumer`: consumer com Avro + Schema Registry

## Pré-requisitos
- .NET SDK instalado (mesma versão usada nos exemplos)
- Cluster Kafka acessível (local ou remoto) com os tópicos esperados criados
- Acesso ao Schema Registry para os exemplos Avro

## Como executar um exemplo
1) Garanta que o cluster Kafka e o Schema Registry (quando necessário) estão rodando.
2) No diretório `src`, execute o projeto desejado:

```sh
cd /samples/Kafka/src
# Exemplo: producer básico
DOTNET_ENVIRONMENT=Development dotnet run --project 02-02-producer/Producer.csproj
# Exemplo: consumer básico
DOTNET_ENVIRONMENT=Development dotnet run --project 02-03-consumer/Consumer.csproj
```

Ajuste as configurações (bootstrap servers, tópicos, schemas) nos arquivos `Program.cs` correspondentes, conforme sua infraestrutura.

## Dicas rápidas
- Teste primeiro com um tópico simples e uma única partição para entender o fluxo.
- Para exemplos de confiabilidade (acks/retries/idempotência), monitore latência e duplicidade de mensagens.
- Nos módulos de Avro, publique e consuma após registrar o schema para validar compatibilidade.
- Nos módulos de offset/commit, observe o deslocamento no consumer group para entender commits manuais x automáticos.

