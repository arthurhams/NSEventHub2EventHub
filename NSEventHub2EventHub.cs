using System.Text;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace NSEventHub2EventHub;

public class NSEventHub2EventHub
{
    private readonly ILogger<NSEventHub2EventHub> _logger;

    public NSEventHub2EventHub(ILogger<NSEventHub2EventHub> logger)
    {
        _logger = logger;
    }

    [Function("NSEventHub2EventHub")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");

        try
        {
            // Get EventHub connection string from environment variables
            var connectionString = Environment.GetEnvironmentVariable("EventHubConnectionString");
            var eventHubName = Environment.GetEnvironmentVariable("EventHubName");

            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(eventHubName))
            {
                return new BadRequestObjectResult("EventHub configuration is missing. Please set EventHubConnectionString and EventHubName in local.settings.json");
            }

            // Create a producer client to send events to EventHub
            await using var producerClient = new EventHubProducerClient(connectionString, eventHubName, new DefaultAzureCredential());


          // Create a batch of events
            int messageSize = 10; // in Kbytes
            try{
                if (req.Query.ContainsKey("messageSize"))
                    messageSize = int.Parse(req.Query["messageSize"]);

            }catch{
                _logger.LogWarning("Invalid messageSize parameter. Using default value of 10.");
            }

            // Read message from request body or use default
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var message = string.IsNullOrEmpty(requestBody) ? CreateMessageOfSize(messageSize) : requestBody;

            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            int numberOfEvents = 10; // Number of events to send
            try{
                if (req.Query.ContainsKey("numberOfEvents"))
                    numberOfEvents = int.Parse(req.Query["numberOfEvents"]);

            }catch{
                _logger.LogWarning("Invalid numberOfEvents parameter. Using default value of 10.");
            }


            for (int i = 1; i <= numberOfEvents; i++)
            {
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}")));
            }

            // Send the batch of events
            await producerClient.SendAsync(eventBatch);
 

            _logger.LogInformation($"Successfully sent message to EventHub: {message}");
            
            return new OkObjectResult(new { 
                status = "success", 
                message = "Event sent to EventHub successfully",
                eventData = message
            });
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error sending message to EventHub: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    //function that create a string with the size of the give parameter in kb
    private string CreateMessageOfSize(int sizeInKb)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < sizeInKb * 1024; i++)
        {
            sb.Append(string.Concat("Measurement", i, ":", Random.Shared.NextInt64(0, 100), ";"));
        }
        return sb.ToString();
    }
}
