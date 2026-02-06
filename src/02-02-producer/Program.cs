using Confluent.Kafka;

Console.WriteLine("=== Kafka Producer ===");
Console.WriteLine("Digite suas mensagens (ou 'sair' para encerrar):\n");

const string topicName = "hello-world";

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};
using var producer = new ProducerBuilder<Null, string>(config).Build();

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
        var result = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = input });
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
