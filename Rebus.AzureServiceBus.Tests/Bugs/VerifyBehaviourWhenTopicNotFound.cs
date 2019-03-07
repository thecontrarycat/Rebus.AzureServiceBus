using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Management;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.AzureServiceBus.Tests.Bugs
{
    [TestFixture]
    public class VerifyBehaviourWhenTopicNotFound : FixtureBase
    {
        [Test]
        public async Task CanPublishToTopicThatDoesNotExist()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            Configure.With(activator)
                     .Transport(t => t.UseAzureServiceBus(AsbTestConfig.ConnectionString, TestConfig.GetName("topictest")))
                     .Start();

            await DeleteAllTopics();

            var topicName = Guid.NewGuid().ToString("N");

            // try to ensure we remove the topic afterwards
            Using(new TopicDeleter(topicName));

            var bus = activator.Bus;

            // must not throw!
            await bus.Advanced.Topics.Publish(topicName, "hej med dig min veeeeen!");
        }

        [Test]
        public async Task CanPublishOneWayToTopicThatDoesNotExist()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            Configure.With(activator)
                     .Transport(t => t.UseAzureServiceBusAsOneWayClient(AsbTestConfig.ConnectionString))
                     .Start();

            await DeleteAllTopics();

            var topicName = Guid.NewGuid().ToString("N");

            // try to ensure we remove the topic afterwards
            Using(new TopicDeleter(topicName));

            var bus = activator.Bus;

            // must not throw!
            await bus.Advanced.Topics.Publish(topicName, "hej med dig min veeeeen!");
        }

        [Test]
        public async Task CannotPublishToTopicThatDoesNotExist()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            Configure.With(activator)
                     .Transport(t => t.UseAzureServiceBus(AsbTestConfig.ConnectionString, TestConfig.GetName("topictest"))
                                      .DestinationTopicsMustExist())
                     .Start();

            await DeleteAllTopics();

            var topicName = Guid.NewGuid().ToString("N");

            // try to ensure we remove the topic afterwards
            Using(new TopicDeleter(topicName));

            var bus = activator.Bus;
           
            Assert.ThrowsAsync<RebusApplicationException>(async () => await bus.Advanced.Topics.Publish(topicName, "hej med dig min veeeeen!"));
        }

        [Test]
        public async Task CannotPublishOneWayToTopicThatDoesNotExist()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            Configure.With(activator)
                     .Transport(t => t.UseAzureServiceBusAsOneWayClient(AsbTestConfig.ConnectionString)
                                      .DestinationTopicsMustExist())
                     .Start();

            await DeleteAllTopics();

            var topicName = Guid.NewGuid().ToString("N");

            // try to ensure we remove the topic afterwards
            Using(new TopicDeleter(topicName));

            var bus = activator.Bus;

            Assert.ThrowsAsync<RebusApplicationException>(async () => await bus.Advanced.Topics.Publish(topicName, "hej med dig min veeeeen!"));
        }

        static async Task DeleteAllTopics()
        {
            var managementClient = new ManagementClient(AsbTestConfig.ConnectionString);

            while (true)
            {
                var topics = await managementClient.GetTopicsAsync();

                if (!topics.Any()) return;

                await Task.WhenAll(topics.Select(async topic =>
                {
                    Console.WriteLine($"Deleting topic '{topic.Path}'");
                    await managementClient.DeleteTopicAsync(topic.Path);
                }));
            }
        }
    }
}