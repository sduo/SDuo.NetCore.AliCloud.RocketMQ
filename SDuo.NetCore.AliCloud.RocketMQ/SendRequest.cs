using Aliyun.MQ.Model;
using System;

namespace SDuo.NetCore.AliCloud.RocketMQ
{
    public class SendRequest
    {
        internal TopicMessage TopicMessage { get; set; }               

        public long Deliver { get; set; }

        public string Key { get; set; }

        public SendRequest(string body)
        {
            TopicMessage = new TopicMessage(body);
        }

        public SendRequest(string body, string tag)
        {
            TopicMessage = new TopicMessage(body, tag);
        }

        public void SetProperty(string key, string value)
        {
            SetProperty(key, value, Service.UTF8_BASE64_ENCODER);
        }

        public void SetProperty(string key,string value,Func<string,string> encoder)
        {
            if (encoder != null)
            {
                value = encoder.Invoke(value);
            }
            TopicMessage.PutProperty(key, value);
        }
    }
}
