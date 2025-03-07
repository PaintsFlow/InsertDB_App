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
        DataBase DB = DataBase.Instance(); // DB 연결
        private static ThreadLocal<List<string[]>> tlv = new ThreadLocal<List<string[]>>(() => new List<string[]>()); // 스레드 로컬 변수 선언
        private static Timer timer; // 스레드 타이머 선언
        ConnectionFactory factory;
        IConnection connection;
        IChannel channel;

        private static MemoryCache _cache = MemoryCache.Default;
        public async void ReadRabbit(object exchangeName)
        {
            // 구독 스레드 만들기
            try
            {
                // exchange 구독
                QueueDeclareOk queueDeclareResult = await this.channel.QueueDeclareAsync();
                string queueName = queueDeclareResult.QueueName;
                await this.channel.QueueBindAsync(queue: queueName, exchange: (string)exchangeName, routingKey: string.Empty);
                var consumer = new AsyncEventingBasicConsumer(this.channel);

                List<string[]> arr = tlv.Value; // 스레드 변수 할당
                timer = new Timer(saveDBAndClearDict, arr, 0, 300000); // 스레드 타이머 생성 5 분
                
                consumer.ReceivedAsync += (model, ea) =>
                {
                    byte[] body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    string[] tmp = message.Split(", ");
                    //arr.Add(tmp); // 무지성 추가 x

                    if((string)exchangeName == "alarm")
                    {
                        string cacheKey = $"alarm: {tmp[1]}:{tmp[4]}";
                        if (!_cache.Contains(cacheKey))
                        {
                            //Console.WriteLine($" [x] {cacheKey}");
                            _cache.Add(cacheKey, true, DateTimeOffset.Now.AddMinutes(3));
                            arr.Add(tmp);
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[x] : {message}");
                        arr.Add(tmp); // 모든 센서 데이터는 저장을 지속한다.
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
        // callback 함수는 obj가 필요함
        // timer로 5분에 한 번 수행할 콜백함수 선언
        private async void saveDBAndClearDict(object obj)
        {
            // Bulk insert 
            Console.WriteLine("save!");
            List<string[]> bufferOBJ = ((List<string[]>)obj);
            
            // 어떤 데이터인지에 따라서 처리 프로세스가 다름
            if (bufferOBJ.Count > 0 && bufferOBJ[0].Length == 5)
                await DB.AlarmBulkToMySQL(bufferOBJ);
            else if(bufferOBJ.Count > 0 && bufferOBJ[0].Length == 10)
                await DB.SensorBulkToMySQL(bufferOBJ);

            ((List<string[]>)obj).Clear(); //obj로 처리하면 됨
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
