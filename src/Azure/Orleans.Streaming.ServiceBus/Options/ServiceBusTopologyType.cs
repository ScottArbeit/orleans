namespace Orleans.Configuration
{
    /// <summary>
    /// Specifies the topology type for Azure Service Bus streaming.
    /// </summary>
    public enum ServiceBusTopologyType
    {
        /// <summary>
        /// Queue topology where messages are sent to and received from queues.
        /// </summary>
        Queue,

        /// <summary>
        /// Topic/Subscription topology where messages are published to topics and consumed from subscriptions.
        /// </summary>
        Topic
    }
}