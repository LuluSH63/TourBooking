using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// Erklær Dead Letter Exchange og Dead Letter Queue 
await channel.ExchangeDeclareAsync(exchange: "dead_letter_exchange", type: ExchangeType.Fanout, durable: true); 
await channel.QueueDeclareAsync(queue: "dead_letter_queue", durable: true, exclusive: false, autoDelete: false); 
await channel.QueueBindAsync(queue: "dead_letter_queue", exchange: "dead_letter_exchange", routingKey: "#");

// Erklær Back-Office kø med Dead Letter konfiguration 
var queueArgs = new Dictionary<string, object?>
{
    { "x-dead-letter-exchange", "dead_letter_exchange" }
};

await channel.ExchangeDeclareAsync(exchange: "tour_topic_exchange", type: ExchangeType.Topic, durable: true);
await channel.QueueDeclareAsync(queue: "back_office_queue", durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);

await channel.QueueBindAsync(queue: "back_office_queue", exchange: "tour_topic_exchange", routingKey: "tour.*");

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine("[BACK OFFICE SERVICE] Lytter på 'tour.*'");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    try
    {
        if (message.Contains("FAIL") || string.IsNullOrEmpty(message))
        {
            Console.WriteLine($"[BACK OFFICE ERROR] Ugyldig besked.");
            await channel.BasicRejectAsync(deliveryTag: ea.DeliveryTag, requeue: false);
            return;
        }

        Console.WriteLine($"[BACK OFFICE PROCESSERET] Key: '{ea.RoutingKey}' | Payload: {message}");
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[FEJL]  Kunne ikke behandle beskeden: {ex.Message}");
        await channel.BasicRejectAsync(deliveryTag: ea.DeliveryTag, requeue: false);
    }
};

await channel.BasicConsumeAsync("back_office_queue", autoAck: false, consumer: consumer);
await Task.Delay(-1);