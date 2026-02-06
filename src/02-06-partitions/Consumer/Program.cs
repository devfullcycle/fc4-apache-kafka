using Confluent.Kafka;

var consumerGroup = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? "group-0";

const string topicName = "partitions-example";

Console.WriteLine("=== Kafka Consumer ===");
Console.WriteLine($"Aguardando mensagens do tópico '{topicName}' [Consumer Group {consumerGroup}]");


var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = consumerGroup
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(topicName);

try
{
    while (true)
    {
        try
        {
            var consumeResult = consumer.Consume();
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