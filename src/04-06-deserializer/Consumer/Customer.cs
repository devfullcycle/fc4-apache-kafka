namespace Consumer;

public class Customer
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    
    public override string ToString()
    {
        return $"Customer [Id={Id}, Name={Name}]";
    }
}