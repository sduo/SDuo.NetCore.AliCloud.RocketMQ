using Aliyun.MQ;
using Aliyun.MQ.Model;
using Aliyun.MQ.Model.Exp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SDuo.NetCore.AliCloud.RocketMQ
{
    public class Consumer
    {
        public event ReceiveMessageHandler ReceiveMessage;
        public event ConsumeMessageExceptionHandler ConsumeMessageException;

        private MQConsumer Instance { get; set; }
        private CancellationTokenSource CancellationTokenSource { get; set; }

        public void DoWork(uint batch, uint polling, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                List<Message> messages = new List<Message>();
                Dictionary<string, Message> receipts = new Dictionary<string, Message>();
                try
                {
                    messages.AddRange(Orderly ? Instance.ConsumeMessageOrderly(batch, polling) : Instance.ConsumeMessage(batch, polling));
                    if (messages.Count > 0)
                    {
                        foreach (Message message in messages)
                        {
                            ReceiveMessage?.Invoke(ReceiveMessageEventArgs.Create(message));
                            receipts.Add(message.ReceiptHandle, message);
                        }
                        if (receipts.Count > 0)
                        {
                            AckMessageResponse response = Instance.AckMessage(receipts.Keys.ToList());
                        }
                    }
                }
                catch (MessageNotExistException ex)
                {
                    Trace.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]{nameof(MessageNotExistException)}({ex.Message})");
                    //ConsumeMessageException?.Invoke(ConsumeMessageExceptionArgs.Create(ex));
                }
                catch (AckMessageException ex)
                {
                    Trace.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]{nameof(AckMessageException)}({ex.Message})");
                    ConsumeMessageException?.Invoke(ConsumeMessageExceptionArgs.Create(ex));
                }
                catch (Exception ex)
                {
                    Trace.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]{nameof(Exception)}({ex.Message})");
                    ConsumeMessageException?.Invoke(ConsumeMessageExceptionArgs.Create(ex));
                }
            }
        }

        public bool Orderly { get; private set; }

        public Consumer(MQConsumer consumer, bool orderly)
        {
            Orderly = orderly;
            Instance = consumer;
        }

        public void ConsumeMessage()
        {
            ConsumeMessage(Service.DEFAULT_BATCH, Service.DEFAULT_POLLING);
        }

        public void ConsumeMessage(uint batch, uint polling)
        {
            CancellationTokenSource = new CancellationTokenSource();
            Task task = new Task(() => DoWork(batch, polling, CancellationTokenSource.Token), TaskCreationOptions.LongRunning);
            //task.ContinueWith(x =>
            //{
            //    Trace.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]{nameof(Task)}({x.Exception.Message})");
            //}, TaskContinuationOptions.OnlyOnFaulted);
            task.Start();
        }

        public void Abort()
        {
            CancellationTokenSource?.Cancel();
        }
    }
}