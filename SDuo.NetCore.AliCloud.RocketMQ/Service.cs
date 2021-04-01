using Aliyun.MQ;
using Aliyun.MQ.Util;
using System;
using System.Text;

namespace SDuo.NetCore.AliCloud.RocketMQ
{
    public class Service
    {
        public const string MESSAGE_KEY = Constants.MESSAGE_PROPERTIES_TIMER_KEY;
        //接收消息轮询一次最多消费数(最多可设置为16条)
        public const uint MAX_BATCH = 16;
        public const uint DEFAULT_BATCH = 1;
        //长轮询时间（最多可设置为30秒）
        //长轮询表示如果topic没有消息则请求会在服务端挂住，长轮询时间内如果有消息可以消费则立即返回
        public const uint MAX_POLLING = 30;
        public const uint DEFAULT_POLLING = 10;

        public static readonly Func<string, string> UTF8_BASE64_ENCODER = (x) => Convert.ToBase64String(Encoding.UTF8.GetBytes(x));
        public static readonly Func<string, string> UTF8_BASE64_DECODER = (x) => Encoding.UTF8.GetString(Convert.FromBase64String(x));
        public static readonly string SDK_VERSION = $"{AliyunSDKUtils.SDKVersionNumber}({Constants.X_MQ_VERSION})";

        public string Namespace { get; protected set; } = "AliCloud:RocketMQ";

        public string EndPoint { get { return Client.Config.RegionEndpoint.AbsoluteUri; } }

        public string InstanceId { get; protected set; }

        protected MQClient Client { get; set; }

        public Service(string ak,string sk,string endpoint)
        {
            Client = new MQClient(ak, sk, endpoint, null);
        }

        public Producer CreateProducer(string instance,string topic)
        {
            return new Producer(Client.GetProducer(instance, topic));
        }

        public Consumer CreateConsumer(string instance, string topic, string group)
        {
            return CreateConsumer(instance, topic, group, false);
        }

        public Consumer CreateConsumer(string instance, string topic, string group, string tag)
        {
            return CreateConsumer(instance, topic, group, tag, false);
        }

        public Consumer CreateConsumer(string instance,string topic, string group, bool orderly)
        {
            return CreateConsumer(instance,topic, group, null, orderly);
        }

        public Consumer CreateConsumer(string instance,string topic, string group, string tag, bool orderly)
        {            
            return new Consumer(Client.GetConsumer(instance, topic, group, tag), orderly) ;
        }
    }
}
