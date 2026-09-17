using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// Erklær durable topic exchange 
await channel.ExchangeDeclareAsync(exchange: "tour_topic_exchange", type: ExchangeType.Topic, durable: true); 

// Erklær durable kø til e-mail servicen 
QueueDeclareOk queueResult = await channel.QueueDeclareAsync
( 
    queue: "email_service_queue", 
    durable: true, 
    exclusive: false, 
    autoDelete: false 
); 
string queueName = queueResult.QueueName; 

// Bind køen til kun at lytte på bookinger 
await channel.QueueBindAsync(queue: queueName, exchange: "tour_topic_exchange", routingKey: "tour.booked"); 
Console.WriteLine("[EMAIL SERVICE] Lytter på 'tour.book'...");

var consumer = new AsyncEventingBasicConsumer(channel); 
consumer.ReceivedAsync += (model, ea) => 
    { 
        var message = Encoding.UTF8.GetString(ea.Body.ToArray()); 
        Console.WriteLine($"[EMAIL SENDT] Sender bekræftelses-email for besked: {message}"); 
        return Task.CompletedTask; 
    }; 
    
await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer); await Task.Delay(-1);