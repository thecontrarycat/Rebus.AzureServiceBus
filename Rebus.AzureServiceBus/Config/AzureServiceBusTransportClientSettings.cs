namespace Rebus.Config
{
    /// <summary>
    /// Allows for configuring additional options for the Azure Service Bus transport (when running in one-way client mode)
    /// </summary>
    public class AzureServiceBusTransportClientSettings
    {
        internal bool LegacyNamingEnabled { get; set; }
        internal bool DestinationTopicsMustExistEnabled { get; set; }

        /// <summary>
        /// Enables "legacy naming", which means that queue names are lowercased, and topic names are "normalized" to be in accordance
        /// with how v6 of the transport did it.
        /// </summary>
        public AzureServiceBusTransportClientSettings UseLegacyNaming()
        {
            LegacyNamingEnabled = true;
            return this;
        }

        /// <summary>
        /// Configures Rebus to throw an exception if a message is published to a topic that does not exist.
        /// Note that by default, the AzureServiceBusTransport does not complain if the topic has not been created, causing published messages to be lost.
        /// </summary>
        public AzureServiceBusTransportClientSettings DestinationTopicsMustExist()
        {
            DestinationTopicsMustExistEnabled = true;

            return this;
        }
    }
}