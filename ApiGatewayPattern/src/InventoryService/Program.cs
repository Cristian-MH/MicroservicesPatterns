using Confluent.Kafka;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);

var inventory = new ConcurrentDictionary<string, int>();
builder.Services.AddSingleton(inventory);

builder.Services.AddHostedService<KafkaOrderConsumer>();

var app = builder.Build();

app.MapGet("/inventory/{productId}", (string productId,
                                      ConcurrentDictionary<string, int> inv) =>
{
    inv.TryGetValue(productId, out var qty);
    return Results.Ok(new { ProductId = productId, Quantity = qty });
});

app.Run();

public class KafkaOrderConsumer : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly ConcurrentDictionary<string, int> _inventory;

    public KafkaOrderConsumer(IConfiguration config,
                              ConcurrentDictionary<string, int> inventory)
    {
        _config = config;
        _inventory = inventory;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            var conf = new ConsumerConfig
            {
                GroupId = "inventory-service",
                BootstrapServers = _config["Kafka:BootstrapServers"],
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<string, string>(conf).Build();

            var topic = _config["Kafka:OrderCreatedTopic"] ?? "order-created";
            consumer.Subscribe(topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(stoppingToken);
                var msg = cr.Message.Value;

                var order = System.Text.Json.JsonDocument.Parse(msg).RootElement;
                var productId = order.GetProperty("ProductId").GetString()!;
                var qty = order.GetProperty("Quantity").GetInt32();

                _inventory.AddOrUpdate(productId,
                    _ => 100 - qty,         // initial: 100 - qty
                    (_, current) => current - qty);

                Console.WriteLine($"[Inventory] Updated {productId}, -{qty}");
            }
        }, stoppingToken);
    }
}
