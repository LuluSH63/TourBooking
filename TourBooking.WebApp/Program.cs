using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapPost("/api/tours", async (TourRequest request) =>
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    using var connection = await factory.CreateConnectionAsync();
    using var channel = await connection.CreateChannelAsync();

    // Erklær Topic Exchange
    await channel.ExchangeDeclareAsync(exchange: "tour_topic_exchange", type: ExchangeType.Topic);

    // Opret routing key dynamisk ud fra handling
    string actionKey = request.Action.ToLower() == "cancel" ? "cancel" : "book";
    string routingKey = $"tour.{actionKey}";

    var payload = new
    {
        Customer = request.CustomerName,
        Tour = request.TourName,
        Action = request.Action,
        Timestamp = DateTime.UtcNow
    };

    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));

    // Publicer til Topic Exchange med specifik routing key
    await channel.BasicPublishAsync(
        exchange: "tour_topic_exchange",
        routingKey: routingKey,
        body: body
    );

    Console.WriteLine($"[Web App] Publiceret til routing key '{routingKey}': {request.CustomerName} - {request.Action}");

    return Results.Ok(new { Message = "Sendt til RabbitMQ", RoutingKey = routingKey });
});

app.Run();

record TourRequest(string CustomerName, string TourName, string Action);
