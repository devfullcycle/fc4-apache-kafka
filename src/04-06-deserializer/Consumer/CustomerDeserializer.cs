using System.Text;
using Confluent.Kafka;

namespace Consumer;

/*
    | 16 bytes | 4 bytes        | N bytes        |
    | Guid Id  | Name length    | Name (UTF-8)   |
*/
public class CustomerDeserializer : IDeserializer<Customer>
{
    public Customer Deserialize(
        ReadOnlySpan<byte> data,
        bool isNull,
        SerializationContext context)
    {
        if (isNull || data.Length == 0)
            return null!;

        var idBytes = data[..16].ToArray();
        var id = new Guid(idBytes);

        var nameLength = BitConverter.ToInt32(data.Slice(16, 4));
        var name = Encoding.UTF8.GetString(data.Slice(20, nameLength));

        return new Customer
        {
            Id = id,
            Name = name
        };
    }
}