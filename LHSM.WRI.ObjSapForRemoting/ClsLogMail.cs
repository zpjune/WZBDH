using System;
using System.Collections.Generic;
using System.Web;
using System.Text;
using System.Data;
using System.IO;
using System.Xml;
using System.Collections.ObjectModel;
using System.Net.Mail;
using System.Net;
using System.Net.Sockets;
using LHSM.DataAccess;
namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    ///LHSM.BaseServiceA8
    ///ClsLogMail
    ///邮件验证
    /// </summary>
    public class ClsLogMail
    {
        #region === 属性

        /// <summary>
        /// 公式编号
        /// </summary>
        public string FLA_CODE { get; set; }

        /// <summary>
        /// 公式名称
        /// </summary>
        public string FLA_NAME { get; set; }

        /// <summary>
        /// 所属计划编号
        /// </summary>
        public string PLAN_CODE { get; set; }

        /// <summary>
        /// 所属计划名称
        /// </summary>
        public string PLAN_NAME { get; set; }

        /// <summary>
        /// 公式类型
        /// </summary>
        public string FLA_TYPE { get; set; }

        /// <summary>
        /// 时间类型
        /// </summary>
        public string DATE_TYPE { get; set; }

        /// <summary>
        /// 本地表名称
        /// </summary>
        public string DB_TABLE { get; set; }

        /// <summary>
        /// 数据源列信息
        /// </summary>
        public string DB_COLUMN { get; set; }

        /// <summary>
        /// 本地条件信息，用于删除当前数据
        /// </summary>
        public string DB_WHERE { get; set; }

        /// <summary>
        /// 初始时间
        /// </summary>
        public string INIT_DATE { get; set; }

        /// <summary>
        /// 执行时间
        /// </summary>
        public string EXE_DATE { get; set; }

        /// <summary>
        /// 执行时间辅助，如：旬，上半年、下半年等
        /// </summary>
        public string EXE_DATE_ASS { get; set; }

        /// <summary>
        /// 执行单位
        /// </summary>
        public string EXE_UNIT { get; set; }

        /// <summary>
        /// 失败信息
        /// </summary>
        public string FAIL_INFO { get; set; }

        /// <summary>
        /// 数据源
        /// </summary>
        public string DB_CODE { get; set; }


        /// <summary>
        /// 执行SQL
        /// </summary>
        public string m_ExeSql { get; set; }

        //自定义变量表
        private DataTable m_dtVaBase = null;

        #endregion

        private MailMessage mm;

        public TcpClient Server;
        public NetworkStream NetStrm;
        public StreamReader RdStrm;
        public string Data;
        public byte[] szData;
        public string CRLF = "\r\n";


        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="p_PLAN_CODE">方案名称</param>
        public ClsLogMail()
        {

        }

        public bool MailConnect(string p_User, string p_Pass)
        {
            //判断系统是否注册此用户
            ClsDBConnection m_oDb = new ClsDBConnection();
            m_oDb.Open();
            string strUserCountSql = "SELECT COUNT(*) FROM TBREGUSERINFO WHERE  USER_CODE = '" + p_User + "'";
            string strCount = m_oDb.GetSqlResultToStr(strUserCountSql);
            if (strCount == "0" || strCount == "")
            { 
                return false;
            }
            
            //注册验证邮件
            bool Result = true;

            //中油信箱
            string strMailSql = "SELECT SYS_VALUE FROM SAP_SYSCONFIG WHERE SYS_CODE = 'MailAddress'";
            string strPopServer = m_oDb.GetSqlResultToStr(strMailSql);
            string strUser = p_User;
            string strPass = p_Pass;
           
            try
            {
                //用110端口新建POP3服务器连接
                Server = new TcpClient(strPopServer, 110);

                //初始化
                NetStrm = Server.GetStream();
                RdStrm = new StreamReader(Server.GetStream());
                string strMegage = RdStrm.ReadLine();
                Result = CheckMailResult(strMegage);

                //登录服务器过程
                Data = "USER " + strUser + CRLF;
                szData = System.Text.Encoding.ASCII.GetBytes(Data.ToCharArray());
                NetStrm.Write(szData, 0, szData.Length);
                strMegage = RdStrm.ReadLine();
                Result = CheckMailResult(strMegage);

                Data = "PASS " + strPass + CRLF;
                szData = System.Text.Encoding.ASCII.GetBytes(Data.ToCharArray());
                NetStrm.Write(szData, 0, szData.Length);
                strMegage = RdStrm.ReadLine();
                Result = CheckMailResult(strMegage);
            }
            catch (InvalidOperationException err)
            {
                Result = false;
            }

            m_oDb.Dispose();
            return Result;
        }

        private bool CheckMailResult(string Message)
        {
            bool Result = true;

            if (Message.Trim() == "")
            {
                return false;
            }

            try
            {
                if (Message.IndexOf("+OK") == -1)
                {
                    Result = false;
                }
            }
            catch
            {
                Result = false;
            }

            return Result;
        }
    }
}