using Confluent.Kafka;

const string topicName = "auto-offset-reset-test";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All, // In-Sync replicas
    LingerMs = 500,
    EnableIdempotence = true
};
using var producer = new ProducerBuilder<string, string>(config).Build();

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
for (var i = 0; i < 10000; i++)
{
    try
    {
        var value = i.ToString();
        var message = new Message<string, string>
        {
            Key = value,
            Value = value
        };
        stopwatch.Start();
        var result = await producer.ProduceAsync(topicName, message);
        stopwatch.Stop();
        Console.WriteLine(
            $"""
             Mensagem enviada: {value}
             [Partition: {result.Partition.Value}, Offset: {result.Offset.Value}]
             Em {stopwatch.ElapsedMilliseconds} ms
             """
        );
        stopwatch.Reset();
    }
    catch (ProduceException<Null, string> ex)
    {
        Console.WriteLine($"Erro ao enviar mensagem: {ex.Error.Reason} em {stopwatch.ElapsedMilliseconds} ms");
        break;
    }
}

producer.Flush();