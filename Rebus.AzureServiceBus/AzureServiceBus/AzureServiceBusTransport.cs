﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using Rebus.AzureServiceBus.NameFormat;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Internals;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Transport;
using Message = Microsoft.Azure.ServiceBus.Message;
// ReSharper disable RedundantArgumentDefaultValue
// ReSharper disable ArgumentsStyleNamedExpression
// ReSharper disable ArgumentsStyleOther
// ReSharper disable ArgumentsStyleLiteral
#pragma warning disable 1998

namespace Rebus.AzureServiceBus
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses Azure Service Bus queues to send/receive messages.
    /// </summary>
    public class AzureServiceBusTransport : ITransport, IInitializable, IDisposable, ISubscriptionStorage
    {
        /// <summary>
        /// Outgoing messages are stashed in a concurrent queue under this key
        /// </summary>
        const string OutgoingMessagesKey = "new-azure-service-bus-transport";

        /// <summary>
        /// Subscriber "addresses" are prefixed with this bad boy so we can recognize them and publish to a topic client instead
        /// </summary>
        const string MagicSubscriptionPrefix = "***Topic***: ";

        /// <summary>
        /// Defines the maximum number of outgoing messages to batch together when sending/publishing
        /// </summary>
        const int DefaultOutgoingBatchSize = 50;

        static readonly RetryExponential DefaultRetryStrategy = new RetryExponential(
            minimumBackoff: TimeSpan.FromMilliseconds(100),
            maximumBackoff: TimeSpan.FromSeconds(10),
            maximumRetryCount: 10
        );

        readonly ExceptionIgnorant _subscriptionExceptionIgnorant = new ExceptionIgnorant(maxAttemps: 10).Ignore<ServiceBusException>(ex => ex.IsTransient);
        readonly INameFormatter _nameFormatter;
        readonly ConcurrentStack<IDisposable> _disposables = new ConcurrentStack<IDisposable>();
        readonly ConcurrentDictionary<string, MessageSender> _messageSenders = new ConcurrentDictionary<string, MessageSender>();
        readonly ConcurrentDictionary<string, TopicClient> _topicClients = new ConcurrentDictionary<string, TopicClient>();
        readonly ConcurrentDictionary<string, string[]> _cachedSubscriberAddresses = new ConcurrentDictionary<string, string[]>();
        readonly IAsyncTaskFactory _asyncTaskFactory;
        readonly CancellationToken _cancellationToken;
        readonly ManagementClient _managementClient;
        readonly string _connectionString;
        readonly TimeSpan? _receiveTimeout;
        readonly ILog _log;
        readonly string _subscriptionName;

        bool _prefetchingEnabled;
        int _prefetchCount;

        MessageReceiver _messageReceiver;

        /// <summary>
        /// Constructs the transport, connecting to the service bus pointed to by the connection string.
        /// </summary>
        public AzureServiceBusTransport(string connectionString, string queueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, INameFormatter nameFormatter, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _nameFormatter = nameFormatter;

            if (queueName != null)
            {
                // this never happens
                if (queueName.StartsWith(MagicSubscriptionPrefix))
                {
                    throw new ArgumentException($"Sorry, but the queue name '{queueName}' cannot be used because it conflicts with Rebus' internally used 'magic subscription prefix': '{MagicSubscriptionPrefix}'. ");
                }

                Address = _nameFormatter.FormatQueueName(queueName);
                _subscriptionName = _nameFormatter.FormatSubscriptionName(queueName);
            }

            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
            _cancellationToken = cancellationToken;
            _log = rebusLoggerFactory.GetLogger<AzureServiceBusTransport>();
            _managementClient = new ManagementClient(connectionString);

            _receiveTimeout = _connectionString.Contains("OperationTimeout")
                ? default(TimeSpan?)
                : TimeSpan.FromSeconds(5);
        }

        /// <summary>
        /// Gets "subscriber addresses" by getting one single magic "queue name", which is then
        /// interpreted as a publish operation to a topic when the time comes to send to that "queue"
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            return _cachedSubscriberAddresses.GetOrAdd(topic, _ => new[] { $"{MagicSubscriptionPrefix}{topic}" });
        }

        /// <summary>
        /// Registers this endpoint as a subscriber by creating a subscription for the given topic, setting up
        /// auto-forwarding from that subscription to this endpoint's input queue
        /// </summary>
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            VerifyIsOwnInputQueueAddress(subscriberAddress);

            topic = _nameFormatter.FormatTopicName(topic);

            _log.Debug("Registering subscription for topic {topicName}", topic);

            await _subscriptionExceptionIgnorant.Execute(async () =>
            {
                var topicDescription = await EnsureTopicExists(topic).ConfigureAwait(false);
                var messageSender = GetMessageSender(Address);

                var inputQueuePath = messageSender.Path;
                var topicPath = topicDescription.Path;

                var subscription = await GetOrCreateSubscription(topicPath, _subscriptionName).ConfigureAwait(false);

                // if it looks fine, just skip it
                if (subscription.ForwardTo == inputQueuePath) return;

                subscription.ForwardTo = inputQueuePath;

                await _managementClient.UpdateSubscriptionAsync(subscription, _cancellationToken).ConfigureAwait(false);

                _log.Info("Subscription {subscriptionName} for topic {topicName} successfully registered", _subscriptionName, topic);
            }, _cancellationToken);
        }

        /// <summary>
        /// Unregisters this endpoint as a subscriber by deleting the subscription for the given topic
        /// </summary>
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            VerifyIsOwnInputQueueAddress(subscriberAddress);

            topic = _nameFormatter.FormatTopicName(topic);

            _log.Debug("Unregistering subscription for topic {topicName}", topic);

            await _subscriptionExceptionIgnorant.Execute(async () =>
            {
                var topicDescription = await EnsureTopicExists(topic).ConfigureAwait(false);
                var topicPath = topicDescription.Path;

                try
                {
                    await _managementClient.DeleteSubscriptionAsync(topicPath, _subscriptionName, _cancellationToken).ConfigureAwait(false);

                    _log.Info("Subscription {subscriptionName} for topic {topicName} successfully unregistered",
                        _subscriptionName, topic);
                }
                catch (MessagingEntityNotFoundException)
                {
                    // it's alright man
                }
            }, _cancellationToken);
        }

        async Task<SubscriptionDescription> GetOrCreateSubscription(string topicPath, string subscriptionName)
        {
            try
            {
                return await _managementClient.CreateSubscriptionAsync(topicPath, subscriptionName, _cancellationToken).ConfigureAwait(false);
            }
            catch (MessagingEntityAlreadyExistsException)
            {
                return await _managementClient.GetSubscriptionAsync(topicPath, subscriptionName, _cancellationToken).ConfigureAwait(false);
            }
        }

        void VerifyIsOwnInputQueueAddress(string subscriberAddress)
        {
            if (subscriberAddress == Address) return;

            var message = $"Cannot register subscriptions endpoint with input queue '{subscriberAddress}' in endpoint with input" +
                          $" queue '{Address}'! The Azure Service Bus transport functions as a centralized subscription" +
                          " storage, which means that all subscribers are capable of managing their own subscriptions";

            throw new ArgumentException(message);
        }

        async Task<TopicDescription> EnsureTopicExists(string normalizedTopic)
        {
            try
            {
                return await _managementClient.GetTopicAsync(normalizedTopic, _cancellationToken).ConfigureAwait(false);
            }
            catch (MessagingEntityNotFoundException)
            {
                // it's OK... try and create it instead
            }

            try
            {
                return await _managementClient.CreateTopicAsync(normalizedTopic, _cancellationToken).ConfigureAwait(false);
            }
            catch (MessagingEntityAlreadyExistsException)
            {
                return await _managementClient.GetTopicAsync(normalizedTopic, _cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                throw new ArgumentException($"Could not create topic '{normalizedTopic}'", exception);
            }
        }

        /// <summary>
        /// Creates a queue with the given address
        /// </summary>
        public void CreateQueue(string address)
        {
            address = _nameFormatter.FormatQueueName(address);

            InnerCreateQueue(address);
        }

        void InnerCreateQueue(string normalizedAddress)
        {
            QueueDescription GetInputQueueDescription()
            {
                var queueDescription = new QueueDescription(normalizedAddress);

                // if it's the input queue, do this:
                if (normalizedAddress == Address)
                {
                    // must be set when the queue is first created
                    queueDescription.EnablePartitioning = PartitioningEnabled;

                    if (LockDuration.HasValue)
                    {
                        queueDescription.LockDuration = LockDuration.Value;
                    }

                    if (DefaultMessageTimeToLive.HasValue)
                    {
                        queueDescription.DefaultMessageTimeToLive = DefaultMessageTimeToLive.Value;
                    }

                    if (DuplicateDetectionHistoryTimeWindow.HasValue)
                    {
                        queueDescription.RequiresDuplicateDetection = true;
                        queueDescription.DuplicateDetectionHistoryTimeWindow = DuplicateDetectionHistoryTimeWindow.Value;
                    }

                    if (AutoDeleteOnIdle.HasValue)
                    {
                        queueDescription.AutoDeleteOnIdle = AutoDeleteOnIdle.Value;
                    }
                }

                return queueDescription;
            }

            // one-way client does not create any queues
            if (Address == null)
            {
                return;
            }

            if (DoNotCreateQueuesEnabled)
            {
                _log.Info("Transport configured to not create queue - skipping existence check and potential creation for {queueName}", normalizedAddress);
                return;
            }

            AsyncHelpers.RunSync(async () =>
            {
                if (await _managementClient.QueueExistsAsync(normalizedAddress, _cancellationToken).ConfigureAwait(false)) return;

                try
                {
                    _log.Info("Creating ASB queue {queueName}", normalizedAddress);

                    var queueDescription = GetInputQueueDescription();

                    await _managementClient.CreateQueueAsync(queueDescription, _cancellationToken).ConfigureAwait(false);
                }
                catch (MessagingEntityAlreadyExistsException)
                {
                    // it's alright man
                }
                catch (Exception exception)
                {
                    throw new ArgumentException($"Could not create Azure Service Bus queue '{normalizedAddress}'", exception);
                }
            });
        }

        void CheckInputQueueConfiguration(string address)
        {
            if (DoNotCheckQueueConfigurationEnabled)
            {
                _log.Info("Transport configured to not check queue configuration - skipping existence check for {queueName}", address);
                return;
            }

            AsyncHelpers.RunSync(async () =>
            {
                var queueDescription = await GetQueueDescription(address).ConfigureAwait(false);

                if (queueDescription.EnablePartitioning != PartitioningEnabled)
                {
                    _log.Warn("The queue {queueName} has EnablePartitioning={enablePartitioning}, but the transport has PartitioningEnabled={partitioningEnabled}. As this setting cannot be changed after the queue is created, please either make sure the Rebus transport settings are consistent with the queue settings, or delete the queue and let Rebus create it again with the new settings.",
                        address, queueDescription.EnablePartitioning, PartitioningEnabled);
                }

                if (DuplicateDetectionHistoryTimeWindow.HasValue)
                {
                    var duplicateDetectionHistoryTimeWindow = DuplicateDetectionHistoryTimeWindow.Value;

                    if (!queueDescription.RequiresDuplicateDetection ||
                        queueDescription.DuplicateDetectionHistoryTimeWindow != duplicateDetectionHistoryTimeWindow)
                    {
                        _log.Warn("The queue {queueName} has RequiresDuplicateDetection={requiresDuplicateDetection}, but the transport has DuplicateDetectionHistoryTimeWindow={duplicateDetectionHistoryTimeWindow}. As this setting cannot be changed after the queue is created, please either make sure the Rebus transport settings are consistent with the queue settings, or delete the queue and let Rebus create it again with the new settings.",
                            address, queueDescription.RequiresDuplicateDetection, PartitioningEnabled);
                    }
                }
                else
                {
                    if (queueDescription.RequiresDuplicateDetection)
                    {
                        _log.Warn("The queue {queueName} has RequiresDuplicateDetection={requiresDuplicateDetection}, but the transport has DuplicateDetectionHistoryTimeWindow={duplicateDetectionHistoryTimeWindow}. As this setting cannot be changed after the queue is created, please either make sure the Rebus transport settings are consistent with the queue settings, or delete the queue and let Rebus create it again with the new settings.",
                            address, queueDescription.RequiresDuplicateDetection, PartitioningEnabled);
                    }
                }

                var updates = new List<string>();

                if (DefaultMessageTimeToLive.HasValue)
                {
                    var defaultMessageTimeToLive = DefaultMessageTimeToLive.Value;
                    if (queueDescription.DefaultMessageTimeToLive != defaultMessageTimeToLive)
                    {
                        queueDescription.DefaultMessageTimeToLive = defaultMessageTimeToLive;
                        updates.Add($"DefaultMessageTimeToLive = {defaultMessageTimeToLive}");
                    }
                }

                if (LockDuration.HasValue)
                {
                    var lockDuration = LockDuration.Value;
                    if (queueDescription.LockDuration != lockDuration)
                    {
                        queueDescription.LockDuration = lockDuration;
                        updates.Add($"LockDuration = {lockDuration}");
                    }
                }

                if (AutoDeleteOnIdle.HasValue)
                {
                    var autoDeleteOnIdle = AutoDeleteOnIdle.Value;
                    if (queueDescription.AutoDeleteOnIdle != autoDeleteOnIdle)
                    {
                        queueDescription.AutoDeleteOnIdle = autoDeleteOnIdle;
                        updates.Add($"AutoDeleteOnIdle = {autoDeleteOnIdle}");
                    }
                }

                if (!updates.Any()) return;

                if (DoNotCreateQueuesEnabled)
                {
                    _log.Warn("Detected changes in the settings for the queue {queueName}: {updates} - but the transport is configured to NOT create queues, so no settings will be changed", address, updates);
                    return;
                }

                _log.Info("Updating ASB queue {queueName}: {updates}", address, updates);
                await _managementClient.UpdateQueueAsync(queueDescription, _cancellationToken);
            });
        }

        async Task<QueueDescription> GetQueueDescription(string address)
        {
            try
            {
                return await _managementClient.GetQueueAsync(address, _cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not get queue description for queue {address}");
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Sends the given message to the queue with the given <paramref name="destinationAddress" />
        /// </summary>
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (!destinationAddress.StartsWith(MagicSubscriptionPrefix))
            {
                destinationAddress = _nameFormatter.FormatQueueName(destinationAddress);
            }

            var outgoingMessages = GetOutgoingMessages(context);

            outgoingMessages.Enqueue(new OutgoingMessage(destinationAddress, message));
        }

        static Message GetMessage(OutgoingMessage outgoingMessage)
        {
            var transportMessage = outgoingMessage.TransportMessage;
            var message = new Message(transportMessage.Body);
            var headers = transportMessage.Headers.Clone();

            if (headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedStr))
            {
                timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
                var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);
                message.TimeToLive = timeToBeReceived;
                headers.Remove(Headers.TimeToBeReceived);
            }

            if (headers.TryGetValue(Headers.DeferredUntil, out var deferUntilTime))
            {
                var deferUntilDateTimeOffset = deferUntilTime.ToDateTimeOffset();
                message.ScheduledEnqueueTimeUtc = deferUntilDateTimeOffset.UtcDateTime;
                headers.Remove(Headers.DeferredUntil);
            }

            if (headers.TryGetValue(Headers.ContentType, out var contentType))
            {
                message.ContentType = contentType;
            }

            if (headers.TryGetValue(Headers.CorrelationId, out var correlationId))
            {
                message.CorrelationId = correlationId;
            }

            if (headers.TryGetValue(Headers.MessageId, out var messageId))
            {
                message.MessageId = messageId;
            }

            message.Label = transportMessage.GetMessageLabel();

            foreach (var kvp in headers)
            {
                message.UserProperties[kvp.Key] = kvp.Value;
            }

            return message;
        }

        ConcurrentQueue<OutgoingMessage> GetOutgoingMessages(ITransactionContext context)
        {
            return context.GetOrAdd(OutgoingMessagesKey, () =>
            {
                var messagesToSend = new ConcurrentQueue<OutgoingMessage>();

                context.OnCommitted(async () =>
                {
                    var messagesByDestinationQueue = messagesToSend.GroupBy(m => m.DestinationAddress);

                    await Task.WhenAll(messagesByDestinationQueue.Select(async group =>
                    {
                        var destinationQueue = group.Key;
                        var messages = group;

                        if (destinationQueue.StartsWith(MagicSubscriptionPrefix))
                        {
                            var topicName = _nameFormatter.FormatTopicName(destinationQueue.Substring(MagicSubscriptionPrefix.Length));

                            foreach (var batch in messages.Batch(DefaultOutgoingBatchSize))
                            {
                                var list = batch.Select(GetMessage).ToList();

                                try
                                {
                                    await GetTopicClient(topicName).SendAsync(list).ConfigureAwait(false);
                                }
                                catch (MessagingEntityNotFoundException mex)
                                {
                                    if (DestinationTopicsMustExist)
                                    {
                                        throw new RebusApplicationException(mex, $"Could not publish to topic '{topicName}' as it does not exist in the Azure Service Bus");
                                    }
                                    else
                                    {
                                        // if the topic does not exist, it's allright
                                    }
                                }
                                catch (Exception exception)
                                {
                                    throw new RebusApplicationException(exception, $"Could not publish to topic '{topicName}'");
                                }
                            }
                        }
                        else
                        {
                            foreach (var batch in messages.Batch(DefaultOutgoingBatchSize))
                            {
                                var list = batch.Select(GetMessage).ToList();

                                try
                                {
                                    await GetMessageSender(destinationQueue).SendAsync(list).ConfigureAwait(false);
                                }
                                catch (Exception exception)
                                {
                                    throw new RebusApplicationException(exception, $"Could not send to queue '{destinationQueue}'");
                                }
                            }
                        }

                    })).ConfigureAwait(false);
                });

                return messagesToSend;
            });
        }

        /// <summary>
        /// Receives the next message from the input queue. Returns null if no message was available
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            var receivedMessage = await ReceiveInternal().ConfigureAwait(false);

            if (receivedMessage == null) return null;

            var message = receivedMessage.Message;
            var messageReceiver = receivedMessage.MessageReceiver;

            if (!message.SystemProperties.IsLockTokenSet)
            {
                throw new RebusApplicationException($"OMG that's weird - message with ID {message.MessageId} does not have a lock token!");
            }

            var lockToken = message.SystemProperties.LockToken;
            var messageId = message.MessageId;

            if (AutomaticallyRenewPeekLock && !_prefetchingEnabled)
            {
                var now = DateTime.UtcNow;
                var leaseDuration = message.SystemProperties.LockedUntilUtc - now;
                var lockRenewalInterval = TimeSpan.FromMinutes(0.7 * leaseDuration.TotalMinutes);

                var renewalTask = _asyncTaskFactory
                    .Create($"RenewPeekLock-{messageId}",
                        () => RenewPeekLock(messageReceiver, messageId, lockToken),
                        intervalSeconds: (int)lockRenewalInterval.TotalSeconds,
                        prettyInsignificant: true);

                // be sure to stop the renewal task regardless of whether we're committing or aborting
                context.OnCommitted(async () => renewalTask.Dispose());
                context.OnAborted(() => renewalTask.Dispose());
                context.OnDisposed(() => renewalTask.Dispose());

                renewalTask.Start();
            }

            context.OnCompleted(async () =>
            {
                try
                {
                    await messageReceiver.CompleteAsync(lockToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception,
                        $"Could not complete message with ID {message.MessageId} and lock token {lockToken}");
                }
            });

            context.OnAborted(() =>
            {
                try
                {
                    AsyncHelpers.RunSync(async () => await messageReceiver.AbandonAsync(lockToken).ConfigureAwait(false));
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception,
                        $"Could not abandon message with ID {message.MessageId} and lock token {lockToken}");
                }
            });

            var userProperties = message.UserProperties;
            var headers = userProperties.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToString());
            var body = message.Body;

            return new TransportMessage(headers, body);
        }

        async Task<ReceivedMessage> ReceiveInternal()
        {
            try
            {
                var messageReceiver = _messageReceiver;

                var message = _receiveTimeout.HasValue
                    ? await messageReceiver.ReceiveAsync(_receiveTimeout.Value).ConfigureAwait(false)
                    : await messageReceiver.ReceiveAsync().ConfigureAwait(false);

                return message == null
                    ? null
                    : new ReceivedMessage(message, messageReceiver);
            }
            catch (MessagingEntityNotFoundException exception)
            {
                throw new RebusApplicationException(exception, $"Could not receive next message from Azure Service Bus queue '{Address}'");
            }
        }

        async Task RenewPeekLock(IMessageReceiver messageReceiver, string messageId, string lockToken)
        {
            _log.Info("Renewing peek lock for message with ID {messageId}", messageId);

            try
            {
                await messageReceiver.RenewLockAsync(lockToken).ConfigureAwait(false);
            }
            catch (MessageLockLostException exception)
            {
                // if we get this, it is probably because the message has been handled
                _log.Error(exception, "Could not renew lock for message with ID {messageId} and lock token {lockToken}", messageId, lockToken);
            }
        }

        /// <summary>
        /// Gets the input queue name for this transport
        /// </summary>
        public string Address { get; }

        /// <summary>
        /// Initializes the transport by ensuring that the input queue has been created
        /// </summary>
        /// <inheritdoc />
        public void Initialize()
        {
            if (Address != null)
            {
                _log.Info("Initializing Azure Service Bus transport with queue {queueName}", Address);

                InnerCreateQueue(Address);

                CheckInputQueueConfiguration(Address);

                _messageReceiver = new MessageReceiver(
                    _connectionString,
                    Address,
                    receiveMode: ReceiveMode.PeekLock,
                    retryPolicy: DefaultRetryStrategy,
                    prefetchCount: _prefetchCount
                );

                _disposables.Push(_messageReceiver.AsDisposable(m => AsyncHelpers.RunSync(async () => await m.CloseAsync().ConfigureAwait(false))));

                return;
            }

            _log.Info("Initializing one-way Azure Service Bus transport");
        }

        /// <summary>
        /// Always returns true because Azure Service Bus topics and subscriptions are global
        /// </summary>
        public bool IsCentralized => true;

        /// <summary>
        /// Enables automatic peek lock renewal - only recommended if you truly need to handle messages for a very long time
        /// </summary>
        public bool AutomaticallyRenewPeekLock { get; set; }

        /// <summary>
        /// Gets/sets whether partitioning should be enabled on new queues. Only takes effect for queues created
        /// after the property has been enabled
        /// </summary>
        public bool PartitioningEnabled { get; set; }

        /// <summary>
        /// Gets/sets whether to skip creating queues
        /// </summary>
        public bool DoNotCreateQueuesEnabled { get; set; }

        /// <summary>
        /// Gets/sets whether to skip checking queues configuration
        /// </summary>
        public bool DoNotCheckQueueConfigurationEnabled { get; set; }

        /// <summary>
        /// Gets/sets the default message TTL. Must be set before calling <see cref="Initialize"/>, because that is the time when the queue is (re)configured
        /// </summary>
        public TimeSpan? DefaultMessageTimeToLive { get; set; }

        /// <summary>
        /// Gets/sets message peek lock duration
        /// </summary>
        public TimeSpan? LockDuration { get; set; }

        /// <summary>
        /// Gets/sets auto-delete-on-idle duration
        /// </summary>
        public TimeSpan? AutoDeleteOnIdle { get; set; }

        /// <summary>
        /// Gets/sets the duplicate detection window
        /// </summary>
        public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }

        /// <summary>
        /// Determines whether to allow sending to topics that have not been created 
        /// </summary>
        public bool DestinationTopicsMustExist { get; set; }

        /// <summary>
        /// Purges the input queue by receiving all messages as quickly as possible
        /// </summary>
        public void PurgeInputQueue()
        {
            var queueName = Address;

            if (string.IsNullOrWhiteSpace(queueName))
            {
                throw new InvalidOperationException("Cannot 'purge input queue' because there's no input queue name – it's most likely because this is a one-way client, and hence there is no input queue");
            }

            PurgeQueue(queueName);
        }

        /// <summary>
        /// Configures the transport to prefetch the specified number of messages into an in-mem queue for processing, disabling automatic peek lock renewal
        /// </summary>
        public void PrefetchMessages(int prefetchCount)
        {
            if (prefetchCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(prefetchCount), prefetchCount, "Must prefetch zero or more messages");
            }

            _prefetchingEnabled = prefetchCount > 0;
            _prefetchCount = prefetchCount;
        }

        /// <summary>
        /// Disposes all resources associated with this particular transport instance
        /// </summary>
        public void Dispose()
        {
            while (_disposables.TryPop(out var disposable))
            {
                disposable.Dispose();
            }
        }

        void PurgeQueue(string queueName)
        {
            try
            {
                AsyncHelpers.RunSync(async () =>
                    await ManagementExtensions.PurgeQueue(_connectionString, queueName, _cancellationToken).ConfigureAwait(false));
            }
            catch (Exception exception)
            {
                throw new ArgumentException($"Could not purge queue '{queueName}'", exception);
            }
        }

        IMessageSender GetMessageSender(string queue)
        {
            return _messageSenders.GetOrAdd(queue, _ =>
            {
                var connectionStringParser = new ConnectionStringParser(_connectionString);
                var connectionString = connectionStringParser.GetConnectionStringWithoutEntityPath();

                var messageSender = new MessageSender(
                    connectionString,
                    queue,
                    retryPolicy: DefaultRetryStrategy
                );

                _disposables.Push(messageSender.AsDisposable(t => AsyncHelpers.RunSync(async () => await t.CloseAsync().ConfigureAwait(false))));

                return messageSender;
            });
        }

        ITopicClient GetTopicClient(string topic)
        {
            return _topicClients.GetOrAdd(topic, _ =>
            {
                var topicClient = new TopicClient(
                    _connectionString,
                    topic,
                    retryPolicy: DefaultRetryStrategy
                );
                _disposables.Push(topicClient.AsDisposable(t => AsyncHelpers.RunSync(async () => await t.CloseAsync().ConfigureAwait(false))));
                return topicClient;
            });
        }
    }
}
