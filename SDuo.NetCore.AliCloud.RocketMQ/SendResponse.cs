using Aliyun.MQ.Model;

namespace SDuo.NetCore.AliCloud.RocketMQ
{
    public class SendResponse
    {
        public string Id { get;private set; }

        public string MD5 { get; private set; }

        public string ReceiptHandle { get; private set; }

        private SendResponse() { }

        internal static SendResponse Create(TopicMessage message)
        {
            return new SendResponse() { 
                Id = message.Id,
                MD5 = message.BodyMD5,
                ReceiptHandle = message.ReceiptHandle
            };
        }
    }
}
