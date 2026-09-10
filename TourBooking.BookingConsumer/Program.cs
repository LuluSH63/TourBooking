using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: "tour_topic_exchange", type: ExchangeType.Topic);

// Server-genereret anonym kø
QueueDeclareOk queueResult = await channel.QueueDeclareAsync();
string queueName = queueResult.QueueName;

// Bind kun til tour.book
string bindingKey = "tour.book";
await channel.QueueBindAsync(queue: queueName, exchange: "tour_topic_exchange", routingKey: bindingKey);

Console.WriteLine($"[BOOKING SERVICE] Lytter på binding key '{bindingKey}'...");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[NY BOOKING] Modtaget med key '{ea.RoutingKey}': {message}");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);
await Task.Delay(-1);