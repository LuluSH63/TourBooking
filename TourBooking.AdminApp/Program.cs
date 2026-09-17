using RabbitMQ.Client; 
using RabbitMQ.Client.Events; 
using System.Text;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: "dead_letter_queue", durable: true, exclusive: false, autoDelete: false);

Console.WriteLine("=================================================="); 
Console.WriteLine(" ADMIN APP - LOG OVER UGYLDIGE &amp; ULEVEREDE BESKEDER "); 
Console.WriteLine("==================================================");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] [DEAD LETTER LOG]");
    Console.WriteLine($" -&gt; Original Routing Key : {ea.RoutingKey}");
    Console.WriteLine($" -&gt; Besked indhold : {message}");
    Console.WriteLine("--------------------------------------------------");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue: "dead_letter_queue", autoAck: true, consumer: consumer);
await Task.Delay(-1);