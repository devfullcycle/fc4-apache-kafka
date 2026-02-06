using Confluent.Kafka;

const string topicName = "commit-test";

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "commit-consumer-group",
    HeartbeatIntervalMs = 3000,
    SessionTimeoutMs = 10000,
    MaxPollIntervalMs = 10000,
    EnableAutoCommit = true,
    AutoCommitIntervalMs = 5000
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
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
                 > {consumeResult.Message.Value}
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