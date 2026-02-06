using Confluent.Kafka;

const string topicName = "compression-test";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All, // In-Sync replicas
    LingerMs = 250,
    EnableIdempotence = true,
    CompressionType = CompressionType.Gzip
};
using var producer = new ProducerBuilder<Null, string>(config).Build();

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
for (var i = 0; i < 10000; i++)
{
    try
    {
        var value = i.ToString();
        var message = new Message<Null, string>
        {
            Value = value
        };
        producer.Produce(topicName, message,
            result =>
            {
                Console.WriteLine(
                    $"""
                     Mensagem enviada para o tópico '{topicName}' 
                     [Partition: {result.Partition.Value}, Offset: {result.Offset.Value}]
                     """
                );
            });
        await Task.Delay(10);
    }
    catch (ProduceException<Null, string> ex)
    {
        Console.WriteLine($"Erro ao enviar mensagem: {ex.Error.Reason} em {stopwatch.ElapsedMilliseconds} ms");
        break;
    }
}

producer.Flush();