using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using LHSM.DataAccess;
using System.Data;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：数据准备信息
    /// 编写时间：20141014
    /// 编写人：孙冰
    /// </summary>
    public class ClsUtility
    {
        //数据库连接
        private static ClsDBConnection m_Conn = null;

        /// <summary>
        /// 返回接口实例
        /// </summary>
        /// <param name="className">接口实现类名</param>
        /// <returns></returns>
        public static ISAPInterface GetInter(string p_SapName)
        {
            string strClassName = GetClassName(p_SapName);
            string path = "LHSM.HB.ObjSapForRemoting.";
            Assembly assembly = Assembly.Load("LHSM.HB.ObjSapForRemoting");
            return (ISAPInterface)assembly.CreateInstance(path + strClassName);
        }

        /// <summary>
        /// 返回数据加载实例
        /// </summary>
        /// <param name="className">接口实现类名</param>
        /// <returns></returns>
        public static ISAPLoadInterface GetLoadInter(string p_SapName)
        {
            string strClassName = GetLoadClassName(p_SapName);
            string path = "LHSM.HB.ObjSapForRemoting.";
            Assembly assembly = Assembly.Load("LHSM.HB.ObjSapForRemoting");
            return (ISAPLoadInterface)assembly.CreateInstance(path + strClassName);
        }

        /// <summary>
        /// 获取类的名称
        /// </summary>
        /// <param name="p_SapName">传给接口名称</param>
        public static string GetClassName(string p_SapName )
        {
            string strResult = "";

            switch(p_SapName)
            {
                case "ZP10PSIF013_PO":
                    strResult = "ClsZP10PSIF013_PO";
                break;
                case "ZP10PSIF013_PRPS":
                    strResult = "ClsZP10PSIF013_PRPS";
                break;

                case "ZP10PSIF013_PSDG079":
                     strResult = "ClsZP10PSIF013_PSDG079";
                break;

                case "ZP10PSIF013_MARA":
                    strResult = "ClsZP10PSIF013_MARA";
                break;

                case "ZP10PSIF013_LFA1":
                    strResult = "ClsZP10PSIF013_LFA1";
                break;

                case "ZP10PSIF013_QTZSL":
                    strResult = "ClsZP10PSIF013_QTZSL";
                break;

                case "ZP10PSIF013_COVP":
                strResult = "ClsZP10PSIF013_COVP";
                break;
            }

            return strResult;
        }

        /// <summary>
        /// 获取类的名称
        /// </summary>
        /// <param name="p_SapName">传给接口名称</param>
        public static string GetLoadClassName(string p_SapName)
        {
            string strResult = "";

            switch (p_SapName)
            {
                case "ZWKC":
                    strResult = "ClsDataLoadZWKC";
                    break;
                case "SWKC":
                    strResult = "ClsDataLoadSWKC";
                    break;

                case "XMZJ":
                    strResult = "ClsDataLoadXMZJ";
                    break;
                case "RK":
                    strResult = "ClsDataLoadRK";
                    break;
                case "CK":
                    strResult = "ClsDataLoadCK";
                    break;
            }

            return strResult;
        }

        /// <summary>
        /// 获取SAP的参数数据信息
        /// </summary>
        /// <returns> 返回SAP参数类 </returns>
        public static ClsSAPDataParameter GetSapConParameter(string[] p_SapPara)
        {
            //数据库连接
            //ClsDBConnection Conn = GetConn();
            //string strSaoConn = Conn.GetSqlResultToStr(" SELECT T.SYS_VALUE FROM SAP_SYSCONFIG T WHERE T.SYS_CODE = 'ServicesSoap' ");
            //Conn.Dispose();

            //参数数据
            ClsSAPDataParameter m_conParamet = new ClsSAPDataParameter();
            //m_conParamet.Sap_Conn = strSaoConn;
            m_conParamet.Sap_AEDAT = p_SapPara.Length == 0 ? "" : p_SapPara[0];

            //op接口添加单位代码参数
            return m_conParamet;
        }

        /// <summary>
        /// 创建数据库实例
        /// </summary>
        /// <returns></returns>
        public static ClsDBConnection GetConn()
        {
            ClsDBConnection Result = (m_Conn == null) ? new ClsDBConnection() : m_Conn;
            if (Result.ConnState == System.Data.ConnectionState.Closed)
            {
                Result.Open();
            }
            return Result;
        }

        /// <summary>
        /// 执行语句
        /// </summary>
        /// <param name="p_Sql">SQL语句</param>
        /// <returns>是否执行成功true执行成功，false失败</returns>
        public static bool ExecuteSqlToDb(string p_Sql)
        {
            bool Result = true;

            try
            {
                //执行更新SQL
                ClsDBConnection Conn = GetConn();
                Conn.ExecuteSql(p_Sql);
                Conn.Dispose();

                
            }
            catch (Exception exception)
            {
                ClsErrorLogInfo.WriteSqlLog("2", "执行sql发生异常:" + exception, p_Sql);
                Result = false;
            }

            return Result;
        }


        /// <summary>
        /// 获取表数据
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public static DataTable GetSelectTable(string strSql)
        {
            //数据库连接
            ClsDBConnection Conn = GetConn();
            DataTable dtResult = Conn.GetSqlResultToDt(strSql);
            Conn.Dispose();

            return dtResult;
        }
    }
}
