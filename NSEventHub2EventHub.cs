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

    [Function("NSEventHub2EventHubFunction")]
    [FixedDelayRetry(5, "00:00:10")]
    [EventHubOutput("EventHubProcess", Connection = "EventHubProcessConnectionString")]
    public string EventHubFunction(
        [EventHubTrigger("EventHubIngest", Connection = "EventHubIngestConnectionString")] string[] input,
        FunctionContext context)
    {

        try 
        {
            // Get EventHub connection string from environment variables
            var connectionString = Environment.GetEnvironmentVariable("EventHubProcessConnectionString__fullyQualifiedNamespace");
            var eventHubName = Environment.GetEnvironmentVariable("EventHubProcessName");

            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(eventHubName))
            {
                return "EventHub configuration is missing. Please set EventHubConnectionString and EventHubName in local.settings.json";
            }

            // Create a producer client to send events to EventHub
            var producerClient = new EventHubProducerClient(connectionString, eventHubName, new DefaultAzureCredential());

            foreach (string s in input)
            {
                EventDataBatch eventBatch = producerClient.CreateBatchAsync().Result;
                foreach (var line in s.Split(';'))
                {
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(line)));

                }
                producerClient.SendAsync(eventBatch);
                _logger.LogInformation($"Successfully sent messages to EventHub: {s}");
            }


            return "Event sent to EventHub successfully";
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error sending message to EventHub: {ex.Message}");
            return StatusCodes.Status500InternalServerError.ToString();
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
