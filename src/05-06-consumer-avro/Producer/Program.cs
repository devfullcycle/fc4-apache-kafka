using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Events.Orders;

const string topicName = "avro-test";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All,
    LingerMs = 250,
    EnableIdempotence = true
};
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

using var producer = new ProducerBuilder<Null, OrderCreated>(config)
    .SetValueSerializer(new AvroSerializer<OrderCreated>(schemaRegistry))
    .Build();

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
for (var i = 0; i < 1000; i++)
{
    try
    {
        var value = new OrderCreated
        {
            orderId = Guid.NewGuid().ToString(),
            customerId = $"customer-{i}",
            amount = 100 + i,
            notes = $"Order notes {i}",
            currency = "BRL"
        };
        var message = new Message<Null, OrderCreated>
        {
            Value = value
        };
        stopwatch.Start();
        var result = await producer.ProduceAsync(topicName, message);
        stopwatch.Stop();
        Console.WriteLine(
            $"""
              Mensagem enviada: {value}
              [OrderId: {value.orderId}, CustomerId: {value.customerId}, Amount: {value.amount}]
              [Partition: {result.Partition.Value}, Offset: {result.Offset.Value}]
              Em {stopwatch.ElapsedMilliseconds} ms
              """
        );
        stopwatch.Reset();
    }
    catch (ProduceException<Null, OrderCreated> ex)
    {
        Console.WriteLine($"Erro ao enviar mensagem: {ex.Error.Reason} em {stopwatch.ElapsedMilliseconds} ms");
        break;
    }
}

producer.Flush();