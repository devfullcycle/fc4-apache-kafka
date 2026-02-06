using Confluent.Kafka;

var consumerGroup = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? "group-0";

Console.WriteLine("=== Kafka Consumer ===");
Console.WriteLine($"Aguardando mensagens do tópico 'hello-world' [Consumer Group {consumerGroup}]");

const string topicName = "hello-world";

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = consumerGroup
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
                 Mensagem recebida do tópico '{topicName}'
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
    consumer.Close();
}