using Azure.Messaging.ServiceBus;
using Orleans.Configuration;
using Orleans.Runtime;
using System;
using Xunit;

namespace Orleans.Tests.ServiceBus
{
    /// <summary>
    /// Tests for <see cref="AzureServiceBusOptionsValidator"/>.
    /// </summary>
    public class AzureServiceBusOptionsValidatorTests
    {
        [Fact]
        public void ValidateConfiguration_WithValidQueueOptions_ShouldNotThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue"
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            validator.ValidateConfiguration(); // Should not throw
        }

        [Fact]
        public void ValidateConfiguration_WithValidTopicOptions_ShouldNotThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Topic,
                TopicName = "test-topic",
                SubscriptionName = "test-subscription"
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            validator.ValidateConfiguration(); // Should not throw
        }

        [Fact]
        public void ValidateConfiguration_WithoutServiceBusClient_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue"
            };
            // Note: Not configuring ServiceBusClient

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("No credentials specified", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_QueueTopologyWithoutQueueName_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue
                // Note: Missing QueueName
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("QueueName must be specified", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_TopicTopologyWithoutTopicName_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Topic,
                SubscriptionName = "test-subscription"
                // Note: Missing TopicName
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("TopicName must be specified", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_TopicTopologyWithoutSubscriptionName_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Topic,
                TopicName = "test-topic"
                // Note: Missing SubscriptionName
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("SubscriptionName must be specified", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_QueueTopologyWithTopicName_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue",
                TopicName = "test-topic" // Should not be specified for Queue topology
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("TopicName should not be specified when using Queue topology", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_TopicTopologyWithQueueName_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Topic,
                TopicName = "test-topic",
                SubscriptionName = "test-subscription",
                QueueName = "test-queue" // Should not be specified for Topic topology
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("QueueName should not be specified when using Topic topology", exception.Message);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ValidateConfiguration_WithInvalidMaxConcurrentCalls_ShouldThrow(int maxConcurrentCalls)
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue",
                MaxConcurrentCalls = maxConcurrentCalls
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("MaxConcurrentCalls must be greater than 0", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_WithInvalidMaxWaitTime_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue",
                MaxWaitTime = TimeSpan.Zero
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("MaxWaitTime must be greater than zero", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_WithNegativePrefetchCount_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue",
                PrefetchCount = -1
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("PrefetchCount must be greater than or equal to 0", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_WithInvalidMaxDeliveryCount_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue",
                MaxDeliveryCount = 0
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("MaxDeliveryCount must be greater than 0 when specified", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_WithInvalidLockDuration_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue",
                LockDuration = TimeSpan.Zero
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "test");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("LockDuration must be greater than zero when specified", exception.Message);
        }

        [Fact]
        public void ValidateConfiguration_WithWhiteSpaceName_ShouldThrow()
        {
            // Arrange
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "test-queue"
            };
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

            var validator = new AzureServiceBusOptionsValidator(options, "   ");

            // Act & Assert
            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Contains("Name cannot be empty or whitespace", exception.Message);
        }
    }
}