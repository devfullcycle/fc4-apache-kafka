using Confluent.Kafka;

const string topicName = "idempotence-test";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All, // In-Sync replicas
    LingerMs = 250,
    EnableIdempotence = true
};
using var producer = new ProducerBuilder<Null, string>(config).Build();

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
for (var i = 0; i < 1000; i++)
{
    try
    {
        var value = new string(i.ToString()[0], 2000);
        var message = new Message<Null, string>
        {
            Value = value
        };
        stopwatch.Start();
        var result = await producer.ProduceAsync(topicName, message);
        stopwatch.Stop();
        Console.WriteLine(
            $"""
             Mensagem enviada para o tópico '{topicName}' 
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