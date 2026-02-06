using Confluent.Kafka;

const string topicName = "commit-test";

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "commit-consumer-group",
    HeartbeatIntervalMs = 3000,
    SessionTimeoutMs = 10000,
    MaxPollIntervalMs = 10000,
    EnableAutoOffsetStore = false
    // EnableAutoCommit = false,
    // AutoCommitIntervalMs = 5000
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
            // Faz o commit dos offsets processados, se AutoCommitTimeoutMs expirou
            // Fetch das mensagens, somente quando o buffer estiver vazio
            // StoreOffset - Marca a mensagem como processada
            // Entrega a mensagem para a aplicação
            Console.WriteLine(
                $"""
                 > Mensagem {consumeResult.Message.Value} recebida
                 """
            );
            await Task.Delay(3000, cts.Token);
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