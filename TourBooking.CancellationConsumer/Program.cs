using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(exchange: "tour_topic_exchange", type: ExchangeType.Topic);

QueueDeclareOk queueResult = await channel.QueueDeclareAsync();
string queueName = queueResult.QueueName;

// Bind kun til tour.cancel
string bindingKey = "tour.cancel";
await channel.QueueBindAsync(queue: queueName, exchange: "tour_topic_exchange", routingKey: bindingKey);

Console.WriteLine($"[CANCELLATION SERVICE] Lytter på binding key '{bindingKey}'...");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[AFBESTILLING] Modtaget med key '{ea.RoutingKey}': {message}");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);
await Task.Delay(-1);