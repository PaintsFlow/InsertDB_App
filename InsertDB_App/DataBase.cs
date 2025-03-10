using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Org.BouncyCastle.Security;

namespace InsertDB_App
{ 
    internal class DataBase
    {
        /// <summary>
        /// DataBase 연결하는 객체 생성
        /// 저장하는 기능만 생성
        /// </summary>
        static public DataBase staticDataBase;
        MySqlConnection connection;
        static private object _lock = new object();
        private string _server = "hy.shogle.net", _port = "3306", _dbName="PaintFlowDB", _dbId = "root", _dbPw="9671";
        // 생성자 -> DB Connect
        private DataBase()
        {
            string myConnection = "Server=" + _server + ";Port=" + _port + ";Database=" + _dbName + ";User Id = " + _dbId + ";Password = " + _dbPw + ";CharSet=utf8;";
            try
            {
                connection = new MySqlConnection(myConnection);
                connection.Open(); // DB 오픈
                Console.WriteLine("연결됨");
            }
            catch (Exception E)
            {
                Console.WriteLine("연결실패");
            }
        }
        static public DataBase Instance()
        { 
            // 싱글톤
            if (staticDataBase == null)
                staticDataBase = new DataBase();
            return staticDataBase;
        }
        // 알람 데이터 bulk insert
        public void AlarmBulkToMySQL(List<string[]> arr)
        {
            // buffer 데이터를 받아와서 정리 후 삽입하는 기능
            StringBuilder sCommand = new StringBuilder("INSERT INTO alarm (time, sensor, data, message) VALUES ");
            try
            {
                List<string> Rows = new List<string>();
                for (int i = 0; i < arr.Count; i++)
                {
                        Rows.Add(string.Format("('{0}','{1}', {2}, '{3}')",
                            MySqlHelper.EscapeString(arr[i][0]), MySqlHelper.EscapeString(arr[i][1])
                            , MySqlHelper.EscapeString(arr[i][2]), MySqlHelper.EscapeString(arr[i][4])));
                }
                sCommand.Append(string.Join(",", Rows));
                sCommand.Append(";");
                lock (_lock)
                {
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.ToString()}");
            }
        }
        public void SensorBulkToMySQL(List<string[]> arr)
        {
            // buffer 데이터를 받아와서 정리 후 삽입하는 기능
            StringBuilder sCommandelectro = new StringBuilder("INSERT INTO electroDeposition (time, waterLevel, viscosity, ph, current, voltage) VALUES ");
            StringBuilder sCommandDry = new StringBuilder("INSERT INTO dry (time, temperature, humidity) VALUES ");
            StringBuilder sCommandpaint = new StringBuilder("INSERT INTO paint (time, paintAmount, pressure) VALUES ");

            try
            {
                List<string> Rowselectro = new List<string>();
                List<string> Rowspaint = new List<string>();
                List<string> RowsDry = new List<string>();
                for (int i = 0; i < arr.Count; i++)
                {
                    //날짜 { "수위", "점도", "PH", "전압", "전류", "온도", "습도", "스프레이 건 공압", "페인트 유량" }; 
                    Rowselectro.Add(string.Format("('{0}', {1}, {2}, {3}, {4}, {5})", 
                        MySqlHelper.EscapeString(arr[i][0]), MySqlHelper.EscapeString(arr[i][1]),
                        MySqlHelper.EscapeString(arr[i][2]), MySqlHelper.EscapeString(arr[i][3]),
                        MySqlHelper.EscapeString(arr[i][5]), MySqlHelper.EscapeString(arr[i][4])));

                    RowsDry.Add(string.Format("('{0}', {1}, {2})",
                        MySqlHelper.EscapeString(arr[i][0]), MySqlHelper.EscapeString(arr[i][6]),
                        MySqlHelper.EscapeString(arr[i][7])));

                    Rowspaint.Add(string.Format("('{0}', {1}, {2})",
                        MySqlHelper.EscapeString(arr[i][0]), MySqlHelper.EscapeString(arr[i][9]),
                        MySqlHelper.EscapeString(arr[i][8])));
                }
                sCommandelectro.Append(string.Join(",", Rowselectro));
                sCommandelectro.Append(";"); 
                
                sCommandDry.Append(string.Join(",", RowsDry));
                sCommandDry.Append(";");

                sCommandpaint.Append(string.Join(",", Rowspaint));
                sCommandpaint.Append(";");
                lock (_lock)
                {
                    using (MySqlCommand myCmd = new MySqlCommand(sCommandelectro.ToString(), connection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery();
                    }
                    using (MySqlCommand myCmd = new MySqlCommand(sCommandDry.ToString(), connection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery();
                    }
                    using (MySqlCommand myCmd = new MySqlCommand(sCommandpaint.ToString(), connection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.ToString()}");
            }
        }
    }
}
