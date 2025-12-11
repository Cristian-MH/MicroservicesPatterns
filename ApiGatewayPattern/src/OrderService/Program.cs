using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = builder.Configuration["Kafka:BootstrapServers"]
    };
    return new ProducerBuilder<string, string>(config).Build();
});

var app = builder.Build();

app.MapPost("/orders", async (OrderRequest order,
                             IProducer<string, string> producer,
                             IConfiguration config) =>
{
    // TODO: save order in DB here

    // Create a simple event JSON
    var orderEvent = System.Text.Json.JsonSerializer.Serialize(new
    {
        OrderId = Guid.NewGuid().ToString(),
        order.ProductId,
        order.Quantity,
        CreatedAt = DateTime.UtcNow
    });

    var topic = config["Kafka:OrderCreatedTopic"] ?? "order-created";

    // Publish event to Kafka
    await producer.ProduceAsync(topic,
        new Message<string, string>
        {
            Key = order.ProductId,
            Value = orderEvent
        });

    return Results.Accepted("/orders", new { message = "Order created", eventSent = true });
});

app.Run();

public record OrderRequest(string ProductId, int Quantity);