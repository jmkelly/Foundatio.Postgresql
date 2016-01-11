using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Foundatio.Tests.Utility;
using Npgsql;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Postgresql.Tests.Queue
{
    public class PostgresqlQueueTests : QueueTestBase
    {
        public PostgresqlQueueTests(CaptureFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

        protected override IQueue<SimpleWorkItem> GetQueue(int retries = 1, TimeSpan? workItemTimeout = null, TimeSpan? retryDelay = null, int deadLetterMaxItems = 100, bool runQueueMaintenance = true)
        {
            const string TestDbConnectionString = "Server=127.0.0.1;Port=5432;User id=store_user;password=my super secret password;database=store;";
            var connection = new NpgsqlConnection(TestDbConnectionString);
            var queue = new PostgresqlQueue<SimpleWorkItem>(connection, workItemTimeout: workItemTimeout, retries: retries, retryDelay: retryDelay, deadLetterMaxItems: deadLetterMaxItems, runMaintenanceTasks: runQueueMaintenance);
            Logger.Debug().Message($"Queue Id: {queue.QueueId}").Write();
            return queue;
        }


        [Fact]
        public override Task CanQueueAndDequeueWorkItem()
        {
            return base.CanQueueAndDequeueWorkItem();
        }

        [Fact]
        public override Task CanDequeueWithCancelledToken()
        {
            return base.CanDequeueWithCancelledToken();
        }
    }
}
