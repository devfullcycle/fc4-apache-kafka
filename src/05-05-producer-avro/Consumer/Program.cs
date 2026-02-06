using Confluent.Kafka;

const string topicName = "avro-test";

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "avro-consumer-group",
    EnableAutoOffsetStore = false
};

using var consumer = new ConsumerBuilder<Ignore, string>(config)
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
            Console.WriteLine(
                $"""
                 > Mensagem {consumeResult.Message.Value} recebida
                 """
            );
            consumer.StoreOffset(consumeResult);
            Console.WriteLine(
                $"""
                 > Mensagem {consumeResult.Message.Value} processada
                 [Partition: {consumeResult.Partition.Value}, Offset: {consumeResult.Offset.Value}]
                 """
            );
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