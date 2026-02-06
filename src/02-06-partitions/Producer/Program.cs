using Confluent.Kafka;

Console.WriteLine("=== Kafka Producer ===");
Console.WriteLine("Digite suas mensagens (ou 'sair' para encerrar):\n");

const string topicName = "partitions-example";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.Plaintext,
    ApiVersionRequest = false
};
using var producer = new ProducerBuilder<string, string>(config).Build();

while (true)
{
    Console.Write("> ");
    var input = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(input))
    {
        continue;
    }

    if (input == "sair")
    {
        Console.WriteLine("Encerrando o producer...");
        break;
    }
    
    try
    {
        var parts = input.Split(':', StringSplitOptions.TrimEntries);
        var key = parts[0];
        var value = parts[1];
        var result = await producer.ProduceAsync(topicName, new Message<string, string>
        {
            Key = key,
            Value = value
        });
        Console.WriteLine(
            $"""
             Mensagem enviada para o tópico '{topicName}' 
             [Partition: {result.Partition.Value}, Offset: {result.Offset.Value}]
             """
        );
    }
    catch (ProduceException<Null, string> ex)
    {
        Console.WriteLine($"Erro ao enviar mensagem: {ex.Error.Reason}");
    }
}
