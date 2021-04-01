using Aliyun.MQ.Model;
using System;

namespace SDuo.NetCore.AliCloud.RocketMQ
{
    public delegate void ReceiveMessageHandler(ReceiveMessageEventArgs e);
    public class ReceiveMessageEventArgs
    {
        internal Message Message { get; set; }

        public string Id { get;private set; }
        public string Key { get; private set; }
        public string ReceiptHandle { get; private set; }
        public string Body { get; private set; }
        public string MD5 { get; private set; }
        public string Tag { get; private set; }
        public DateTimeOffset PublishAt { get; private set; }
        public DateTimeOffset NextConsumeAt { get; private set; }
        public DateTimeOffset FirstConsumeAt { get; private set; }
        public uint TotalConsumed { get; private set; }        

        internal static ReceiveMessageEventArgs Create(Message message)
        {
            return new ReceiveMessageEventArgs()
            {
                Id = message.Id,
                Key = message.MessageKey,
                ReceiptHandle = message.ReceiptHandle,
                Body = message.Body,
                MD5 = message.BodyMD5,
                Tag = message.MessageTag,
                PublishAt = DateTimeOffset.FromFileTime(message.PublishTime),
                NextConsumeAt = DateTimeOffset.FromFileTime(message.NextConsumeTime),
                FirstConsumeAt = DateTimeOffset.FromFileTime(message.FirstConsumeTime),
                TotalConsumed = message.ConsumedTimes,
                Message = message
            };
        }

        public string GetProperty(string key)
        {
            return GetProperty(key, Service.UTF8_BASE64_DECODER);
        }

        public string GetProperty(string key,Func<string,string> decoder)
        {
            string value = Message.GetProperty(key);
            if (decoder != null)
            {
                value = decoder.Invoke(value);                
            }
            return value;
        }

        /// <summary>
        /// TODO:待测试机制
        /// </summary>
        /// <param name="key"></param>
        /// <param name="deliver"></param>
        /// <returns></returns>
        public SendRequest ReBuild(string key,long deliver)
        {
            SendRequest request = new SendRequest(Message.Body, Message.MessageTag);
            if (deliver > 0)
            {
                request.Deliver = deliver;
            }
            if (string.IsNullOrEmpty(key))
            {
                request.Key = key;
            }            
            foreach(string property in Message.Properties.Keys)
            {
                if(Message.Properties.TryGetValue(property, out string value))
                {
                    request.SetProperty(property, value, null);
                }               
            }
            return request;
        }
    }

    public delegate void ConsumeMessageExceptionHandler(ConsumeMessageExceptionArgs e);
    public class ConsumeMessageExceptionArgs
    {
        public Exception Exception { get;private set; }

        private ConsumeMessageExceptionArgs()
        {

        }

        public static ConsumeMessageExceptionArgs Create(Exception ex)
        {
            return new ConsumeMessageExceptionArgs()
            {
                Exception = ex
            };
        }
    }
}
