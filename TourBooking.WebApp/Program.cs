using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapPost("/api/tour", async (TourRequest request) =>
{
    // Validering af input
    if (string.IsNullOrWhiteSpace(request.CustomerName) || string.IsNullOrWhiteSpace(request.Email))
    {
        return Results.BadRequest(new {Message = "Både navn og e-mail skal udfyldes."});
    }

    var factory = new ConnectionFactory {HostName = "localhost"};
    using var connection = await factory.CreateConnectionAsync();
    using var channel = await connection.CreateChannelAsync();

    // Erklær Topic Exchange som Durable (Guaranteed Delivery)
    await channel.ExchangeDeclareAsync(
        exchange: "tour_topic_exchange",
        type: ExchangeType.Topic,
        durable: true
    );

    // Routing Key baseret på handlingen (book eller cancel)
    string actionKey = request.Action.ToLower() == "cancel" ? "cancelled":"booked";
    string routingKey = $"tour.{actionKey}";

    // Serialiser payload med Email og handling
    var payload = new
    {
        Customer = request.CustomerName,
        Email = request.Email,
        Tour = request.TourName,
        Action = request.Action,
        Timestamp = DateTime.UtcNow
    };

    var jsonMessage = JsonSerializer.Serialize(payload);
    var body = Encoding.UTF8.GetBytes(jsonMessage);

    // Guaranteed Delivery: Markér beskeden som Persistent
    var properties = new BasicProperties
    {
        DeliveryMode = DeliveryModes.Persistent
    };

    // Publicer beskeden til Topic Exchange
    await channel.BasicPublishAsync(
        exchange: "tour_topic_exchange",
        routingKey: routingKey,
        mandatory: true,
        basicProperties: properties,
        body: body
    );

    Console.WriteLine($"[Web App] Publiceret til key '{routingKey}': {jsonMessage}");

    return Results.Ok(new {Message = $"Besked sendt til key '{routingKey}'."});

});

app.Run();

record TourRequest(string CustomerName, string Email, string TourName, string Action);
