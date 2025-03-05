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
        public void BulkToMySQL()
        {
            // buffer 데이터를 받아와서 정리 후 삽입하는 기능
            StringBuilder sCommand = new StringBuilder("INSERT INTO User (FirstName, LastName) VALUES ");
            try
            {
                List<string> Rows = new List<string>();
                for (int i = 0; i < 100000; i++)
                {
                    Rows.Add(string.Format("('{0}','{1}')", MySqlHelper.EscapeString("test"), MySqlHelper.EscapeString("test")));
                }
                sCommand.Append(string.Join(",", Rows));
                sCommand.Append(";");
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                {
                    myCmd.CommandType = CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
            }
            catch(Exception e)
            {
                Console.WriteLine($"{e.ToString()}");
            }
        }

    }
}
