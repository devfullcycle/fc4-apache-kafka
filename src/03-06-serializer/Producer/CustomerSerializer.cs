using System.Text;
using Confluent.Kafka;

namespace Producer;

public class CustomerSerializer : ISerializer<Customer>
{
    /*
       | 16 bytes | 4 bytes        | N bytes        |
       | Guid Id  | Name length    | Name (UTF-8)   |
     */
    public byte[] Serialize(Customer? data, SerializationContext context)
    {
        if (data is null)
            return [];

        var idBytes = data.Id.ToByteArray();
        var nameBytes = Encoding.UTF8.GetBytes(data.Name);

        var result = new byte[16 + 4 + nameBytes.Length];

        Buffer.BlockCopy(idBytes, 0, result, 0, 16);
        BitConverter.GetBytes(nameBytes.Length).CopyTo(result, 16);
        Buffer.BlockCopy(nameBytes, 0, result, 20, nameBytes.Length);

        return result;
    }
}