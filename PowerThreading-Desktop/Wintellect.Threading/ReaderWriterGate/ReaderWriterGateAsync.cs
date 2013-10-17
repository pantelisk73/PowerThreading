namespace Wintellect.Threading.ReaderWriterGate
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Wintellect.Threading.ResourceLocks;

    public sealed class ReaderWriterGateAsync : IDisposable
    {
        private ResourceLock syncLock = new MonitorResourceLock();

        private ReaderWriterGate.ReaderWriterGateStates gateState = ReaderWriterGate.ReaderWriterGateStates.Free;
        private int readerCount;

        private readonly Queue<Task> writerTasks = new Queue<Task>();
        private readonly Queue<Task> readerTasks = new Queue<Task>();

        public ReaderWriterGateAsync()
            : this(false)
        {

        }

        public ReaderWriterGateAsync(bool blockReadersUntilFirstWriteCompletes)
        {
            this.gateState = blockReadersUntilFirstWriteCompletes
                          ? ReaderWriterGate.ReaderWriterGateStates.ReservedForWriter
                          : ReaderWriterGate.ReaderWriterGateStates.Free;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (syncLock != null)
            {
                syncLock.Dispose();
                syncLock = null;
            }
        }

        public Task<T> WriteAsync<T>(Func<object,T> protectedWrite, object state)
        {
            var runNow = false;
            Task<T> result = null;

            syncLock.Enter(true);
            switch (this.gateState)
            {
                case ReaderWriterGate.ReaderWriterGateStates.Free:             // If Free "RFW -> OBW, invoke, return
                case ReaderWriterGate.ReaderWriterGateStates.ReservedForWriter:
                    this.gateState = ReaderWriterGate.ReaderWriterGateStates.OwnedByWriter;
                    runNow = true;
                    break;

                case ReaderWriterGate.ReaderWriterGateStates.OwnedByReaders:   // If OBR | OBRAWP -> OBRAWP, queue, return
                case ReaderWriterGate.ReaderWriterGateStates.OwnedByReadersAndWriterPending:
                    this.gateState = ReaderWriterGate.ReaderWriterGateStates.OwnedByReadersAndWriterPending;
                    result = new Task<T>(protectedWrite, state);
                    writerTasks.Enqueue(result);
                    break;

                case ReaderWriterGate.ReaderWriterGateStates.OwnedByWriter:   // If OBW, queue, return
                    result = new Task<T>(protectedWrite, state);
                    writerTasks.Enqueue(result);
                    break;
            }

            syncLock.Leave();

            if (runNow)
            {
                result = Task.FromResult(protectedWrite(state));
                this.Complete(false);
            }
            else
            {
                Task.Factory.ContinueWhenAll(new[] { result }, tasks => this.Complete(false));
            }

            return result;
        }

        public Task<T> ReadAsync<T>(Func<object, T> protectedRead, object state)
        {
            Task<T> result = null;
            var runNow = false;

            syncLock.Enter(true);
            switch (this.gateState)
            {
                case ReaderWriterGate.ReaderWriterGateStates.Free:   // If Free | OBR -> OBR, NR++, invoke, return
                case ReaderWriterGate.ReaderWriterGateStates.OwnedByReaders:
                    this.gateState = ReaderWriterGate.ReaderWriterGateStates.OwnedByReaders;
                    readerCount++;
                    runNow = true;
                    break;

                case ReaderWriterGate.ReaderWriterGateStates.OwnedByWriter:   // If OBW | OBRAWP | RFW, queue, return
                case ReaderWriterGate.ReaderWriterGateStates.OwnedByReadersAndWriterPending:
                case ReaderWriterGate.ReaderWriterGateStates.ReservedForWriter:
                    result = new Task<T>(protectedRead, state);
                    readerTasks.Enqueue(result);
                    break;
            }

            syncLock.Leave();

            if (runNow)
            {
                result = Task.FromResult(protectedRead(state));
                this.Complete(true);
            }
            else
            {
                Task.Factory.ContinueWhenAll(new[] { result }, tasks => this.Complete(true));
                Console.WriteLine("Task.Factory.ContinueWhenAll " + Thread.CurrentThread.ManagedThreadId.ToString() + " ");
            }

            return result;
        }

        private void Complete(bool reader)
        {
            Console.WriteLine("Completed a " + (reader ? "reader" : "writer") + " task " + Thread.CurrentThread.ManagedThreadId.ToString() + " ");

            syncLock.Enter(true);

            // If writer or last reader, the lock is being freed
            var freeing = !reader || (--this.readerCount == 0);
            if (freeing)
            {
                // Wake up a writer, or all readers, or set to free
                if (writerTasks.Count > 0)
                {
                    // A writer is queued, invoke it
                    this.gateState = ReaderWriterGate.ReaderWriterGateStates.OwnedByWriter;
                    var nextWriterTask = writerTasks.Dequeue();
                    nextWriterTask.Start();
                }
                else if (readerTasks.Count > 0)
                {
                    // Reader(s) are queued, invoke all of them
                    this.gateState = ReaderWriterGate.ReaderWriterGateStates.OwnedByReaders;
                    readerCount = readerTasks.Count;
                    while (readerTasks.Count > 0)
                    {
                        var readerTask = readerTasks.Dequeue();
                        readerTask.Start();
                    }
                }
                else
                {
                    // No writers or readers, free the gate
                    this.gateState = ReaderWriterGate.ReaderWriterGateStates.Free;
                }
            }

            syncLock.Leave();
        }
    }
}
