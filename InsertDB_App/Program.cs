using System;
using InsertDB_App;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace saveDB
{
    // rabbit mq 정보를 -> DB에 저장하는 프로그램
    // 1. rabbitmq server연결 -> 데이터 받아서 임시 Buffer에 저장
    // 2. 5분간의 정보 저장 및 리스트 clear

    internal class saveDB
    {
        static async Task Main(string[] args)
        {
            rabbitMQ rMQ = new rabbitMQ(); // rabbitMQ 연결
            await rMQ.RabbitmqConnect();

            // 비동기 프로그래밍 
            Thread readSensor = new Thread(new ParameterizedThreadStart(rMQ.ReadRabbit));
            Thread readalarm = new Thread(new ParameterizedThreadStart(rMQ.ReadRabbit));

            readSensor.Start("logs"); // 센서 데이터 받는 스레드 생성
            readalarm.Start("alarm"); // 알람 데이터 받는 데이터 생성
            Thread.Sleep(-1); // Main함수 무한 대기 -> rMQ.ReadRabbit은 무한 roof가 아니라 대기 상태임
            // 즉, Main Thread를 무한대기 시키지 않으면 통신 대기 상태를 Main은 종료로 인식함
        }
    }
}