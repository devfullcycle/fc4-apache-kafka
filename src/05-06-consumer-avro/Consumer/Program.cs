using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Events.Orders;

const string topicName = "avro-test";

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "avro-consumer-group",
    EnableAutoOffsetStore = false
};

var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

using var consumer = new ConsumerBuilder<Ignore, OrderCreated>(config)
    .SetValueDeserializer(new AvroDeserializer<OrderCreated>(schemaRegistry).AsSyncOverAsync())
    .Build();
consumer.Subscribe(topicName);

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    while (!cts.Token.IsCancellationRequested)  // Poll Loop
    {
        try
        {
            var consumeResult = consumer.Consume(cts.Token); // Poll
            var order = consumeResult.Message.Value;
            Console.WriteLine(
                $"""
                  > Mensagem recebida
                  [OrderId: {order.orderId}, CustomerId: {order.customerId}, Amount: {order.amount}]
                  """
            );
            consumer.StoreOffset(consumeResult);
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Erro ao consumir mensagem: {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }
}
finally
{
    Console.WriteLine("Fechando consumidor...");
    consumer.Close();
}