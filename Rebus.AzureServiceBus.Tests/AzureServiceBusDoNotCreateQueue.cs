﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.AzureServiceBus.NameFormat;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;
#pragma warning disable 1998

namespace Rebus.AzureServiceBus.Tests
{
    [TestFixture]
    public class BasicAzureServiceBusBasicReceiveOnly : FixtureBase
    {
        static readonly string QueueName = TestConfig.GetName("input");

        [Test]
        [TestCase(5)]
        [TestCase(10)]
        [Ignore("Don't think this is relevant anymore, as it doesn't seem like the new client supports specifying a receive timeout in the connection string")]
        public async Task DoesntIgnoreDefinedTimeoutWhenReceiving(int operationTimeoutInSeconds)
        {
            var operationTimeout = TimeSpan.FromSeconds(operationTimeoutInSeconds);

            var connString = AsbTestConfig.ConnectionString;
            var builder = new ServiceBusConnectionStringBuilder(connString)
            {
                //    OperationTimeout = operationTimeout,
            };

            var newConnString = builder.ToString();

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var transport = new AzureServiceBusTransport(newConnString, QueueName, consoleLoggerFactory, new TplAsyncTaskFactory(consoleLoggerFactory), new DefaultNameFormatter());

            Using(transport);

            transport.Initialize();

            transport.PurgeInputQueue();
            //Create the queue for the receiver since it cannot create it self beacuse of lacking rights on the namespace
            transport.CreateQueue(QueueName);

            var senderActivator = new BuiltinHandlerActivator();

            var senderBus = Configure.With(senderActivator)
                .Transport(t => t.UseAzureServiceBus(newConnString, "sender"))
                .Start();

            Using(senderBus);

            // queue 3 messages
            await senderBus.Advanced.Routing.Send(QueueName, "message to receiver");
            await senderBus.Advanced.Routing.Send(QueueName, "message to receiver2");
            await senderBus.Advanced.Routing.Send(QueueName, "message to receiver3");

            //await Task.Delay(TimeSpan.FromSeconds(2)); // wait a bit to make sure the messages are queued.

            // receive 1
            using (var scope = new RebusTransactionScope())
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var msg = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                sw.Stop();
                await scope.CompleteAsync();

                Assert.That(msg, Is.Not.Null);
                Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(1500)));
            }

            // receive 2
            using (var scope = new RebusTransactionScope())
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var msg = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                sw.Stop();
                await scope.CompleteAsync();

                Assert.That(msg, Is.Not.Null);
                Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(1500)));
            }

            // receive 3
            using (var scope = new RebusTransactionScope())
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var msg = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                sw.Stop();
                await scope.CompleteAsync();

                Assert.That(msg, Is.Not.Null);
                Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(1500)));
            }

            // receive 4 - NOTHING
            using (var scope = new RebusTransactionScope())
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var msg = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                sw.Stop();
                await scope.CompleteAsync();

                Assert.That(msg, Is.Null);
                Assert.That(sw.Elapsed, Is.LessThan(operationTimeout.Add(TimeSpan.FromSeconds(2))).And.GreaterThan(operationTimeout.Subtract(TimeSpan.FromSeconds(2))));
            }

            // put 1 more message 
            await senderBus.Advanced.Routing.Send(QueueName, "message to receiver5");

            await Task.Delay(TimeSpan.FromSeconds(2)); // wait a bit to make sure the messages are queued.

            // receive 5
            using (var scope = new RebusTransactionScope())
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var msg = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                sw.Stop();
                await scope.CompleteAsync();

                Assert.That(msg, Is.Not.Null);
                Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(1500)));
            }

            // receive 6 - NOTHING
            using (var scope = new RebusTransactionScope())
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var msg = await transport.Receive(scope.TransactionContext, CancellationToken.None);
                sw.Stop();
                await scope.CompleteAsync();

                Assert.That(msg, Is.Null);
                Assert.That(sw.Elapsed, Is.LessThan(operationTimeout.Add(TimeSpan.FromSeconds(2))).And.GreaterThan(operationTimeout.Subtract(TimeSpan.FromSeconds(2))));
            }
        }

        [Test]
        public async Task ShouldBeAbleToRecieveEvenWhenNotCreatingQueue()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var transport = new AzureServiceBusTransport(AsbTestConfig.ConnectionString, QueueName, consoleLoggerFactory, new TplAsyncTaskFactory(consoleLoggerFactory), new DefaultNameFormatter());
            transport.PurgeInputQueue();
            //Create the queue for the receiver since it cannot create it self beacuse of lacking rights on the namespace
            transport.CreateQueue(QueueName);

            var recieverActivator = new BuiltinHandlerActivator();
            var senderActivator = new BuiltinHandlerActivator();

            var receiverBus = Configure.With(recieverActivator)
                .Logging(l => l.ColoredConsole())
                .Transport(t =>
                    t.UseAzureServiceBus(AsbTestConfig.ConnectionString, QueueName)
                        .DoNotCreateQueues())
                .Start();

            var senderBus = Configure.With(senderActivator)
                .Transport(t => t.UseAzureServiceBus(AsbTestConfig.ConnectionString, "sender"))
                .Start();

            Using(receiverBus);
            Using(senderBus);

            var gotMessage = new ManualResetEvent(false);

            recieverActivator.Handle<string>(async (bus, context, message) =>
            {
                gotMessage.Set();
                Console.WriteLine("got message in readonly mode");
            });
            await senderBus.Advanced.Routing.Send(QueueName, "message to receiver");

            gotMessage.WaitOrDie(TimeSpan.FromSeconds(10));
        }
    }
}
