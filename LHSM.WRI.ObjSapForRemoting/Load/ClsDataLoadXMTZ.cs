using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadXMTZ : ISAPLoadInterface
    {
        //数据库连接
        private static ClsDBConnection m_Conn = null;

        /// <summary>
        /// 存储sql字符串变量
        /// </summary>
        private StringBuilder strBuilder = new StringBuilder();

        /// <summary>
        /// 表EKKO结构体用于存取表数据
        /// </summary>
        private struct IMPR
        {
            public string strPRNAM; //投资程序名称 
            public string strPOSID; //定位标识 
            public string strGJAHR; //批注年度  主键
            public string strOBJNR; //投资程序对象号 
        }

        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;

            m_Conn = ClsUtility.GetConn();

            DataTable dtIMPRDate = new DataTable();
            try
            {
                //查询IMPR的数据               
                string strSqlIMPR = "select distinct GJAHR from IMPR t where dldate='" + p_para.Sap_AEDAT + "'";
                dtIMPRDate = m_Conn.GetSqlResultToDt(strSqlIMPR);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表过程中查询IMPR表发生异常:\t\n" + exception);
                return Result;
            }

            foreach (DataRow subRowIMPRdate in dtIMPRDate.Rows)
            {
                string strDate = subRowIMPRdate["GJAHR"].ToString();
                if (string.IsNullOrEmpty(strDate))
                {
                    continue;
                }

                IMPR strIMPR = new IMPR();
                DataTable dtIMPR = new DataTable();
                try
                {
                    //查询IMPR的数据               
                    string strSqlIMPR = "select * from IMPR t where t.gjahr='" + strDate + "'";
                    dtIMPR = m_Conn.GetSqlResultToDt(strSqlIMPR);

                }
                catch (Exception exception)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表过程中查询IMPR表发生异常:\t\n" + exception);
                    return Result;
                }

                strBuilder.Clear();
                strBuilder.Append(" Begin "); //开始执行SQL
                strBuilder.Append(" DELETE FROM HB_XMTZ WHERE XMTZ_YEAR='" + strDate + "';");

                foreach (DataRow subRowIMPR in dtIMPR.Rows)
                {
                    string strPOST1 = string.Empty;  //投资节点名称
                    string strWTGES = string.Empty;  //投资节点金额
                    int intJC = 0;
                    strIMPR.strPRNAM = subRowIMPR["PRNAM"].ToString();
                    strIMPR.strPOSID = subRowIMPR["POSID"].ToString();
                    strIMPR.strGJAHR = subRowIMPR["GJAHR"].ToString();
                    strIMPR.strOBJNR = subRowIMPR["OBJNR"].ToString();

                    try
                    {
                        //投资节点名称
                        strPOST1 = m_Conn.GetSqlResultToStr("select distinct t.post1 from IMPU t where Trim(t.posnr)='" + strIMPR.strPOSID + "' and t.gjahr='" + strDate + "'");

                    }
                    catch (Exception exception1)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表过程中查询IMPU表发生异常:\t\n" + exception1);
                        return Result;
                    }

                    try
                    {
                        //投资节点金额
                        strWTGES = m_Conn.GetSqlResultToStr("select sum(WTGES) from BPGE t where t.OBJNR='" + strIMPR.strOBJNR + "' and t.WRTTP='47' ");
                    }
                    catch (Exception exception1)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表过程中查询BPGE表发生异常:\t\n" + exception1);
                        return Result;
                    }

                    //string[] strjc = strIMPR.strPOSID.Split('-');
                    //intJC = strjc.Length;
                    intJC = strIMPR.strPOSID.Length / 2;
                    //添加数据
                    strBuilder.Append(" INSERT INTO HB_XMTZ");
                    strBuilder.Append("(XMTZ_ID,XMTZ_TZCXMC,XMTZ_DWBS,XMTZ_YEAR,XMTZ_TZCXDXH,XMTZ_TZJDMC,XMTZ_TZJDJE,XMTZ_CCDJ)");
                    strBuilder.Append(" VALUES(");
                    strBuilder.Append("SQ_XMTZ.NEXTVAL,");
                    strBuilder.Append("'" + strIMPR.strPRNAM + "',");
                    strBuilder.Append("'" + strIMPR.strPOSID + "',");
                    strBuilder.Append("'" + strIMPR.strGJAHR + "',");
                    strBuilder.Append("'" + strIMPR.strOBJNR + "',");
                    strBuilder.Append("'" + strPOST1 + "',");
                    strBuilder.Append("'" + (string.IsNullOrEmpty(strWTGES) ? "0.00" : ((Convert.ToDecimal(strWTGES) / 10000).ToString("F2"))) + "',");
                    strBuilder.Append("'" + intJC.ToString() + "'");
                    strBuilder.Append(");");


                }

                strBuilder.Append(" End;");  //SQL完成
                try
                {
                   
                    //数据提交
                    Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                   
                }
                catch (Exception exception5)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表发生异常:" + exception5);
                }
            }

            return Result;
        }
    }
}
