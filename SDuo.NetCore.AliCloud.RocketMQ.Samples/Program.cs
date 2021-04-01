using System;
using System.Threading.Tasks;

namespace SDuo.NetCore.AliCloud.RocketMQ.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("普通消息");
            Task.Run(() => SendMessage(5, 500));
            MQ.Consumer.ReceiveMessage += new ReceiveMessageHandler(ReceiveMessageA);
            MQ.Consumer.ReceiveMessage += new ReceiveMessageHandler(ReceiveMessageB);
            MQ.Consumer.ConsumeMessage();
            Console.ReadKey();
            MQ.Consumer.Abort();
            Console.WriteLine("顺序消息");
            Task.Run(() => SendMessageOrderly(5, 10));
            MQ.OrderlyConsumer.ReceiveMessage += new ReceiveMessageHandler(ReceiveMessageA);
            MQ.OrderlyConsumer.ConsumeMessageException += new ConsumeMessageExceptionHandler(ConsumeMessageException);
            MQ.OrderlyConsumer.ConsumeMessage();
            Console.ReadKey();
            MQ.OrderlyConsumer.Abort();
            Console.WriteLine("完成");
            Console.ReadKey();
        }

        static void ConsumeMessageException(ConsumeMessageExceptionArgs e)
        {
            throw e.Exception;
        }

        static void ReceiveMessageA(ReceiveMessageEventArgs e)
        {
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}]{nameof(ReceiveMessageA)}[{e.Id}]{e.Body}({e.MD5})");
        }

        static void ReceiveMessageB(ReceiveMessageEventArgs e)
        {
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}]{nameof(ReceiveMessageB)}[{e.Id}]{e.Body}({e.MD5})");
        }

        static void SendMessage(int count, int sleep)
        {
            while (--count >= 0)
            {
                string body = $"<{count}>{Guid.NewGuid():N}";
                SendResponse response = MQ.Producer.Send(new SendRequest(body));
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}]{nameof(SendMessage)}[{response.Id}]{body}({response.MD5})");
                Task.Delay(sleep).Wait();
            }
        }

        static void SendMessageOrderly(int count, int sleep)
        {
            while (--count >= 0)
            {
                string body = $"<{count}>{Guid.NewGuid():N}";
                SendResponse response = MQ.OrderlyProducer.Send(new SendRequest(body));
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}]{nameof(SendMessageOrderly)}[{response.Id}]{body}({response.MD5})");
                Task.Delay(sleep).Wait();
            }
        }
    }

    public static class MQ
    {        
        private const string ENDPOINT = "{ HTTP 协议客户端接入点 }";        

        private const string INSTANCE = "{ 实例 ID }";

        private readonly static Service Service;

        public readonly static Producer Producer;

        public readonly static Consumer Consumer;

        public readonly static Producer OrderlyProducer;

        public readonly static Consumer OrderlyConsumer;

        static MQ()
        {
            Service = new Service("{ AK }", "{ SK }", ENDPOINT);

            Producer = Service.CreateProducer(INSTANCE, "{ Topic 名称 }");

            Consumer = Service.CreateConsumer(INSTANCE, "{ Topic 名称 }", "{ Group ID }");

            OrderlyProducer = Service.CreateProducer(INSTANCE, "{ Topic 名称 }");

            OrderlyConsumer = Service.CreateConsumer(INSTANCE, "{ Topic 名称 }", "{ Group ID }", true);

        }
    }
}