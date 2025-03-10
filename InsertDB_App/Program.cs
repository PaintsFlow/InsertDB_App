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
        static DataBase DB = DataBase.Instance(); // DB 연결
        static async Task Main(string[] args)
        {
            rabbitMQ readSensor = new rabbitMQ("logs"); // rabbitMQ 연결
            rabbitMQ readAlarm = new rabbitMQ("alarm");
            //await readSensor.RabbitmqConnect();
            
            // 비동기 프로그래밍 
            Thread readSensorThread = new Thread(new ThreadStart(readSensor.ReadRabbit));
            Thread readalarmThread = new Thread(new ThreadStart(readAlarm.ReadRabbit));

            readSensorThread.Start(); // 센서 데이터 받는 스레드 생성
            readalarmThread.Start(); // 알람 데이터 받는 데이터 생성

            Timer timer = new Timer(saveDataBase, new object[] { readSensor, readAlarm }, 0, 300000);
            Thread.Sleep(-1); // Main함수 무한 대기 -> rMQ.ReadRabbit은 무한 roof가 아니라 대기 상태임
            // 즉, Main Thread를 무한대기 시키지 않으면 통신 대기 상태를 Main은 종료로 인식함
        }

        private static void saveDataBase(object state)
        {
            object[] threads = (object[])state;

            rabbitMQ T1 = (rabbitMQ)threads[0];
            rabbitMQ T2 = (rabbitMQ)threads[1];

            List<string[]> arr1 = T1.GetAndClearData();
            List<string[]> arr2 = T2.GetAndClearData();

            Console.WriteLine("arr1 DB 저장---------------");
            if (arr1.Count > 0)
                DB.SensorBulkToMySQL(arr1);
            Console.WriteLine("arr2 DB 저장---------------");
            if (arr2.Count > 0)
                DB.AlarmBulkToMySQL(arr2);
        }
    }
}