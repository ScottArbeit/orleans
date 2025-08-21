using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    /// <summary>
    /// <see cref="IStreamFailureHandler"/> which does nothing in response to failures.
    /// </summary>
    public class NoOpStreamDeliveryFailureHandler : IStreamFailureHandler
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NoOpStreamDeliveryFailureHandler"/> class.
        /// </summary>
        public NoOpStreamDeliveryFailureHandler()
            : this(false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NoOpStreamDeliveryFailureHandler"/> class.
        /// </summary>
        /// <param name="faultOnError">The value used for <see cref="ShouldFaultSubscriptionOnError"/>.</param>
        public NoOpStreamDeliveryFailureHandler(bool faultOnError)
        {
            ShouldFaultSubscriptionOnError = faultOnError;
        }

        /// <inheritdoc/>
        public bool ShouldFaultSubscriptionOnError { get; }

        /// <inheritdoc/>
        public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamId,
            StreamSequenceToken sequenceToken)
        {
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamId,
            StreamSequenceToken sequenceToken)
        {
            return Task.CompletedTask;
        }
    }
}
