using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using InsertDB_App;
using System.Runtime.Caching;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ZstdSharp.Unsafe;

namespace saveDB
{
    // MQ 기능 구현
    public class rabbitMQ 
    {
        // 각 스레드로 구현
        ConnectionFactory factory;
        IConnection connection;
        IChannel channel;
        private List<string[]> _arr;
        private readonly object _lock = new object();
        private string _exchange = "";
        public rabbitMQ(string name)
        {
            this._exchange = name;
            _arr = new List<string[]>();
        }
        // 캐시, 3분 동안 동일 알람 받지 않기로 하기 기능 추가
        private static MemoryCache _cache = MemoryCache.Default;
        public async void ReadRabbit()
        {
            // 구독 스레드 만들기
            try
            {
                // exchange 구독
                await this.RabbitmqConnect();
                
                QueueDeclareOk queueDeclareResult = await this.channel.QueueDeclareAsync();
                string queueName = queueDeclareResult.QueueName;
                await this.channel.QueueBindAsync(queue: queueName, exchange: _exchange, routingKey: string.Empty);
                var consumer = new AsyncEventingBasicConsumer(this.channel);
                // 따로 스레드를 만들기 때문에 성립되지 않았던 것..
                
                consumer.ReceivedAsync += (model, ea) =>
                {
                    byte[] body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    string[] tmp = message.Split(", ");
                    //arr.Add(tmp); // 무지성 추가 x
                    
                    if(_exchange == "alarm")
                    {
                        string cacheKey = $"alarm: {tmp[1]}:{tmp[4]}";
                        if (!_cache.Contains(cacheKey))
                        {
                            lock (_lock){
                                _cache.Add(cacheKey, true, DateTimeOffset.Now.AddMinutes(3));
                                _arr.Add(tmp);
                            }
                        }
                    }
                    else
                    {
                        lock (_lock)
                        {
                            _arr.Add(tmp); // 모든 센서 데이터는 저장을 지속한다.
                        }
                        Console.WriteLine($"[x] : {message}");
                    }
                    return Task.CompletedTask;
                };
                await this.channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);
            }
            catch(Exception e)
            {
                Console.WriteLine($"{e.ToString()}");
            }
            
        }
        public List<string[]> GetAndClearData()
        {
            lock (_lock)
            {
                List<string[]> copy = new List<string[]>(_arr);
                _arr.Clear();
                return copy;
            }
        }
        public async Task RabbitmqConnect() // rabbit mq 서버 커넥트
        {
            try
            {
                // rabbit mq 연결 하기
                this.factory = new ConnectionFactory
                {
                    HostName = "211.187.0.113",
                    UserName = "guest",
                    Password = "guest",
                    Port = 5672
                };
                this.connection = await this.factory.CreateConnectionAsync();
                this.channel = await this.connection.CreateChannelAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"연결 실패: {e.ToString()}");
            }
        }
    }
}
