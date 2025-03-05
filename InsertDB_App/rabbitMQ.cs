using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using InsertDB_App;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

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
        public async void ReadRabbit(object exchangeName)
        {
            // 구독 스레드 만들기
            try
            {
                QueueDeclareOk queueDeclareResult = await this.channel.QueueDeclareAsync();
                string queueName = queueDeclareResult.QueueName;
                await this.channel.QueueBindAsync(queue: queueName, exchange: (string)exchangeName, routingKey: string.Empty);
                timer = new Timer(saveDBAndClearDict);
                timer.Change(0, 5000);
                var consumer = new AsyncEventingBasicConsumer(this.channel);
                List<string[]> arr = tlv.Value; // 스레드 변수 할당
                consumer.ReceivedAsync += (model, ea) =>
                {
                    byte[] body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($" [x] {message}");

                    string[] tmp = message.Split(", ");
                    arr.Add(tmp);

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
        private void saveDBAndClearDict(object obj)
        {
            // Bulk insert 
            Console.WriteLine("save!");
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
