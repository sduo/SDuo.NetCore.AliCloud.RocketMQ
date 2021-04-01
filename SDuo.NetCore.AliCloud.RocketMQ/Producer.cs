using Aliyun.MQ;
using Aliyun.MQ.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace SDuo.NetCore.AliCloud.RocketMQ
{
    public class Producer
    {
        private MQProducer Instance { get; set; }

        public Producer(MQProducer producer)
        {
            Instance = producer;
        }

        public string Id { get { return Instance.IntanceId; } }
        public string Topic { get { return Instance.TopicName; } }

        /// <summary>
        /// TODO:Key处理机制
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public SendResponse Send(SendRequest message)
        {
            if (message.Deliver > 0)
            {
                message.TopicMessage.StartDeliverTime = message.Deliver;
            }
            
            //if (!string.IsNullOrEmpty(message.Key))
            //{
            //    msg.MessageKey = message.Key;
            //}
            TopicMessage result = Instance.PublishMessage(message.TopicMessage);
            return SendResponse.Create(result);
        }
    }
}
