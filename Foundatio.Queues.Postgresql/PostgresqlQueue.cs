using Foundatio.Serializer;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.Queues
{
    public class PostgresqlQueue<T> : QueueBase<T> where T : class
    {

        public PostgresqlQueue(NpgsqlConnection connection, ISerializer serializer = null, string queueName = null, int retries = 2, TimeSpan? retryDelay = null, int[] retryMultipliers = null,
            TimeSpan? workItemTimeout = null, TimeSpan? deadLetterTimeToLive = null, int deadLetterMaxItems = 100, bool runMaintenanceTasks = true, IEnumerable<IQueueBehavior<T>> behaviors = null)
            : base(serializer, behaviors)
        {
        }

        public override Task AbandonAsync(string id)
        {
            throw new NotImplementedException();
        }

        public override Task CompleteAsync(string id)
        {
            throw new NotImplementedException();
        }

        public override Task DeleteQueueAsync()
        {
            throw new NotImplementedException();
        }

        public override Task<QueueEntry<T>> DequeueAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public override Task<string> EnqueueAsync(T data)
        {
            throw new NotImplementedException();
        }

        public override Task<IEnumerable<T>> GetDeadletterItemsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public override Task<QueueStats> GetQueueStatsAsync()
        {
            throw new NotImplementedException();
        }

        public override void StartWorking(Func<QueueEntry<T>, CancellationToken, Task> handler, bool autoComplete = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }
    }
}
