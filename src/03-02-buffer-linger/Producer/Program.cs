using Confluent.Kafka;

const string topicName = "buffering-test";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All, // In-Sync replicas
    BatchNumMessages = 100,
    BatchSize = 50000,
    LingerMs = 500,
    QueueBufferingMaxKbytes = 500000,
    QueueBufferingMaxMessages = 1000
};
using var producer = new ProducerBuilder<Null, string>(config).Build();

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
for (var i = 0; i < 1000; i++)
{
    try
    {
        var value = i.ToString();
        var message = new Message<Null, string>
        {
            Value = value
        };
        stopwatch.Start();
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
        stopwatch.Stop();
        Console.WriteLine($"Tempo decorrido até o buffer: {stopwatch.ElapsedMilliseconds} ms");
        stopwatch.Reset();
    }
    catch (ProduceException<Null, string> ex)
    {
        Console.WriteLine($"Erro ao enviar mensagem: {ex.Error.Reason}");
    }
}

producer.Flush();