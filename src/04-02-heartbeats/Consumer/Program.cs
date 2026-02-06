using Confluent.Kafka;

const string topicName = "heartbeats-test";

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "heartbeats-consumer-group",
    HeartbeatIntervalMs = 3000,
    SessionTimeoutMs = 10000,
    MaxPollIntervalMs = 10000,
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(topicName);

try
{
    while (true)  // Poll Loop
    {
        try
        {
            var consumeResult = consumer.Consume(); // Poll
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

    }
}
finally
{
    Console.WriteLine("Fechando consumidor...");
    consumer.Close();
}