using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadXMZJ : ISAPLoadInterface
    {
        //数据库连接
        private static ClsDBConnection m_Conn = null;

        /// <summary>
        /// 存储sql字符串变量
        /// </summary>
        private StringBuilder strBuilder = new StringBuilder();

        /// <summary>
        /// 表PRPS结构体用于存取表数据
        /// </summary>
        private struct XMZJ
        {
            public string strOBJNR;     //对象号
            public string strPOST1;     //WBS描述 
            public string strPOSID;     //WBS元素
            public string strPRART;     //项目类型 
            public string strSTUFE;     //项目层次 
            public string strPBUKR;     //公司代码 
            public string strPBUKRMC;   //公司名称
            public string strPBUKRSX;   //公司属性
            public string strTXT04;     //项目状态


            public string strDATE;     //成本发生日期 

            public string strYEAR;     //投资年


            public string strTZ;        //投资
            public string strCB;        //成本
            public string strHTCB;      //合同成本
            public string strHTJE;      //合同金额
            public string strCN;        //承诺=合同金额-合同成本

        }
        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;

            m_Conn = ClsUtility.GetConn();

           
            string m_dat = p_para.Sap_AEDAT.Substring(0, 6);
            string m_year = p_para.Sap_AEDAT.Substring(0, 4);

            string strCovpSQL = "select b.OBJNR, POST1, POSID, PRART, STUFE, PBUKR, b.WTGBTR from (select T.OBJNR, sum(T.WTGBTR) as WTGBTR  from COVP T  WHERE (T.WRTTP = '04' OR T.WRTTP = '11' )AND T.KSTAR NOT IN (SELECT KSTAR FROM TBPFK) and substr(BUDAT, 0, 6) = '" + m_dat + "' group by T.OBJNR) b , PRPS p where b.OBJNR=p.OBJNR";
            

            DataTable dtCOVP = m_Conn.GetSqlResultToDt(strCovpSQL);


            

            int commitcount = 0;
            strBuilder.Length = 0;
            strBuilder.Append(" Begin "); //开始执行SQL

            foreach (DataRow _cdr in dtCOVP.Rows)
            {
                XMZJ strXMZJ = new XMZJ();


                strXMZJ.strOBJNR = _cdr["OBJNR"].ToString();
                strXMZJ.strCB = _cdr["WTGBTR"].ToString();

                strXMZJ.strDATE = m_dat;

                #region PRPS 项目基础信息



                strXMZJ.strOBJNR = _cdr["OBJNR"].ToString();
                strXMZJ.strPOSID = _cdr["POSID"].ToString();
                strXMZJ.strPOST1 = _cdr["POST1"].ToString();
                strXMZJ.strPRART = _cdr["PRART"].ToString();
                strXMZJ.strSTUFE = _cdr["STUFE"].ToString();
                strXMZJ.strPBUKR = _cdr["PBUKR"].ToString();
                try
                {
                    //所属单位
                    strXMZJ.strPBUKRMC = m_Conn.GetSqlResultToStr("select BUTXT from T001 where bukrs='" + strXMZJ.strPBUKR + "'");
                    if (strXMZJ.strPBUKR.Substring(0, 2) == "16")
                    {
                        strXMZJ.strPBUKRSX = "上市";
                    }
                    else if (strXMZJ.strPBUKR.Substring(0, 2) == "C3")
                    {
                        strXMZJ.strPBUKRSX = "未上市";
                    }
                    else if (strXMZJ.strPBUKR.Substring(0, 2) == "M4")
                    {
                        strXMZJ.strPBUKRSX = "矿区";
                    }
                }
                catch (Exception exception1)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmzj", "ALL", p_para.Sap_AEDAT, "插入hb_xmzj表过程中查询T001表发生异常:\t\n" + exception1);
                    return Result;
                }
                try
                {
                    //状态
                    string strSTAT = m_Conn.GetSqlResultToStr("select STAT from JEST t where STAT like 'E%' and (INACT='' or INACT is null) and OBJNR='" + strXMZJ.strOBJNR + "'");

                    strXMZJ.strTXT04 = m_Conn.GetSqlResultToStr("select TXT04 from TJ30T t where STSMA like 'ZPSHB%' and t.estat = '" + strSTAT + "'");
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmzj", "ALL", p_para.Sap_AEDAT, "插入hb_xmzj表过程中查询JEST或TJ30T表发生异常:\t\n" + exception2);
                    return Result;
                }





                #endregion


                #region  cpov70 合同成本
                //杜杨ERP2.0
                string strCOVPsql70 = "select sum(T.WTGBTR)  as WTGBTR  from COVP T  WHERE (T.WRTTP = '04' OR T.WRTTP = '11' ) AND T.EBELN LIKE '73%' AND T.KSTAR NOT IN (SELECT KSTAR FROM TBPFK) and substr(BUDAT, 0, 6) = '" + m_dat + "' and T.OBJNR='" + strXMZJ.strOBJNR + "'";

                strXMZJ.strHTCB = m_Conn.GetSqlResultToStr(strCOVPsql70);

                #endregion


                #region  ekkn 合同金额
                string strEKKNsql = "select  sum(netwr) from ekkn where  replace(replace(replace(ps_psp_pnr,'-',''),'.',''),' ','' )='" + strXMZJ.strPOSID + "' and substr(aedat, 0, 6)='" + m_dat + "'";

                strXMZJ.strHTJE = m_Conn.GetSqlResultToStr(strEKKNsql);

                #endregion

                #region  承诺
                if (string.IsNullOrEmpty(strXMZJ.strHTJE))
                {
                    strXMZJ.strHTJE = "0.00";
                }
                if (string.IsNullOrEmpty(strXMZJ.strHTCB))
                {
                    strXMZJ.strHTCB = "0.00";
                }
                //承诺=合同金额-合同成本
                strXMZJ.strCN = (Convert.ToDecimal(strXMZJ.strHTJE) - Convert.ToDecimal(strXMZJ.strHTCB)).ToString();

                #endregion


                //添加数据
                strBuilder.Append(" INSERT INTO HB_XMZJ");
                strBuilder.Append("(XMZJ_ID,ZMZJ_YEAR,XMZJ_DATE,XMZJ_WBSBM,XMZJ_WBSMC,XMZJ_LX,XMZJ_CC,XMZJ_GSBM,");
                strBuilder.Append("XMZJ_GSMC,XMZJ_GSSX,XMZJ_ZT,XMZJ_TZ,XMZJ_CB,XMZJ_HTCB,XMZJ_HTJE,XMZJ_CN)");
                strBuilder.Append(" VALUES(");
                strBuilder.Append("SQ_XMZJ.NEXTVAL,");
                strBuilder.Append("'" + "" + "',");
                strBuilder.Append("'" + m_dat + "',");
                strBuilder.Append("'" + strXMZJ.strPOSID + "',");
                strBuilder.Append("'" + strXMZJ.strPOST1 + "',");
                strBuilder.Append("'" + strXMZJ.strPRART + "',");
                strBuilder.Append("'" + strXMZJ.strSTUFE + "',");
                strBuilder.Append("'" + strXMZJ.strPBUKR + "',");
                strBuilder.Append("'" + strXMZJ.strPBUKRMC + "',");
                strBuilder.Append("'" + strXMZJ.strPBUKRSX + "',");
                strBuilder.Append("'" + strXMZJ.strTXT04 + "',");
                strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strTZ) ? "0.00" : (Convert.ToDecimal(strXMZJ.strTZ) / 10000).ToString("F2")) + "',");
                strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strCB) ? "0.00" : (Convert.ToDecimal(strXMZJ.strCB) / 10000).ToString("F2")) + "',");
                strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strHTCB) ? "0.00" : (Convert.ToDecimal(strXMZJ.strHTCB) / 10000).ToString("F2")) + "',");
                strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strHTJE) ? "0.00" : (Convert.ToDecimal(strXMZJ.strHTJE) / 10000).ToString("F2")) + "',");
                strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strCN) ? "0.00" : (Convert.ToDecimal(strXMZJ.strCN) / 10000).ToString("F2")) + "'");
                strBuilder.Append(");");

                if (commitcount == 0)
                {
                    strBuilder.Insert(8, " DELETE FROM HB_XMZJ WHERE  xmzj_date='" + m_dat + "' ;");
                }


                commitcount++;
                if (commitcount % 2000 == 0)
                {
                    strBuilder.Append(" End;");  //SQL完成
                    try
                    {

                        //数据提交
                        Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                        strBuilder.Clear();
                        strBuilder.Append(" Begin "); //开始执行SQL
                    }
                    catch (Exception exception5)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表发生异常:" + exception5);
                    }
                }


            }

            if (strBuilder.ToString().Length >9)
            {
                strBuilder.Append(" End;");  //SQL完成
                Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                strBuilder.Clear();

            }




            string strBPJASQL = "select b.OBJNR, POST1, POSID, PRART, STUFE, PBUKR, b.WTJHR from (select T.OBJNR, sum(T.WTJHR) as WTJHR from BPJA T where T.WRTTP = '41'and T.GJAHR = '" + m_year + "' group by T.OBJNR) b,PRPS p where p.OBJNR = b.OBJNR";


            DataTable dtBPJA = m_Conn.GetSqlResultToDt(strBPJASQL);

            strBuilder.Clear();
            commitcount = 0;
            strBuilder.Append(" Begin "); //开始执行SQL
            foreach (DataRow _cdr in dtBPJA.Rows)
            {
                

                string strOBJNR = _cdr["OBJNR"].ToString();


              
                    XMZJ strXMZJ = new XMZJ();

                    strXMZJ.strTZ = _cdr["WTJHR"].ToString();

                    strXMZJ.strOBJNR = _cdr["OBJNR"].ToString();
                    strXMZJ.strPOSID = _cdr["POSID"].ToString();
                    strXMZJ.strPOST1 = _cdr["POST1"].ToString();
                    strXMZJ.strPRART = _cdr["PRART"].ToString();
                    strXMZJ.strSTUFE = _cdr["STUFE"].ToString();
                    strXMZJ.strPBUKR = _cdr["PBUKR"].ToString();

                    try
                    {
                        //所属单位
                        strXMZJ.strPBUKRMC = m_Conn.GetSqlResultToStr("select BUTXT from T001 where bukrs='" + strXMZJ.strPBUKR + "'");
                        if (strXMZJ.strPBUKR.Substring(0, 2) == "16")
                        {
                            strXMZJ.strPBUKRSX = "上市";
                        }
                        else if (strXMZJ.strPBUKR.Substring(0, 2) == "C3")
                        {
                            strXMZJ.strPBUKRSX = "未上市";
                        }
                        else if (strXMZJ.strPBUKR.Substring(0, 2) == "M4")
                        {
                            strXMZJ.strPBUKRSX = "矿区";
                        }
                    }
                    catch (Exception exception1)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "xmzj", "ALL", p_para.Sap_AEDAT, "插入hb_xmzj表过程中查询T001表发生异常:\t\n" + exception1);
                        return Result;
                    }
                    try
                    {
                        //状态
                        string strSTAT = m_Conn.GetSqlResultToStr("select STAT from JEST t where STAT like 'E%' and (INACT='' or INACT is null) and OBJNR='" + strXMZJ.strOBJNR + "'");

                        strXMZJ.strTXT04 = m_Conn.GetSqlResultToStr("select TXT04 from TJ30T t where STSMA like 'ZPSHB%' and t.estat = '" + strSTAT + "'");
                    }
                    catch (Exception exception2)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "xmzj", "ALL", p_para.Sap_AEDAT, "插入hb_xmzj表过程中查询JEST或TJ30T表发生异常:\t\n" + exception2);
                        return Result;
                    }



                    //添加数据
                    strBuilder.Append(" INSERT INTO HB_XMZJ_TZ");
                    strBuilder.Append("(XMZJ_ID,ZMZJ_YEAR,XMZJ_DATE,XMZJ_WBSBM,XMZJ_WBSMC,XMZJ_LX,XMZJ_CC,XMZJ_GSBM,");
                    strBuilder.Append("XMZJ_GSMC,XMZJ_GSSX,XMZJ_ZT,XMZJ_TZ,XMZJ_CB,XMZJ_HTCB,XMZJ_HTJE,XMZJ_CN)");
                    strBuilder.Append(" VALUES(");
                    strBuilder.Append("SQ_XMZJ.NEXTVAL,");
                    strBuilder.Append("'" + m_year + "',");
                    strBuilder.Append("'" + m_dat + "',");
                    strBuilder.Append("'" + strXMZJ.strPOSID + "',");
                    strBuilder.Append("'" + strXMZJ.strPOST1 + "',");
                    strBuilder.Append("'" + strXMZJ.strPRART + "',");
                    strBuilder.Append("'" + strXMZJ.strSTUFE + "',");
                    strBuilder.Append("'" + strXMZJ.strPBUKR + "',");
                    strBuilder.Append("'" + strXMZJ.strPBUKRMC + "',");
                    strBuilder.Append("'" + strXMZJ.strPBUKRSX + "',");
                    strBuilder.Append("'" + strXMZJ.strTXT04 + "',");
                    strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strTZ) ? "0.00" : (Convert.ToDecimal(strXMZJ.strTZ) / 10000).ToString("F2")) + "',");
                    strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strCB) ? "0.00" : (Convert.ToDecimal(strXMZJ.strCB) / 10000).ToString("F2")) + "',");
                    strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strHTCB) ? "0.00" : (Convert.ToDecimal(strXMZJ.strHTCB) / 10000).ToString("F2")) + "',");
                    strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strHTJE) ? "0.00" : (Convert.ToDecimal(strXMZJ.strHTJE) / 10000).ToString("F2")) + "',");
                    strBuilder.Append("'" + (string.IsNullOrEmpty(strXMZJ.strCN) ? "0.00" : (Convert.ToDecimal(strXMZJ.strCN) / 10000).ToString("F2")) + "'");
                    strBuilder.Append(");");

                    if (commitcount == 0)
                    {
                        strBuilder.Insert(8, " DELETE FROM HB_XMZJ_TZ WHERE  zmzj_year='" + m_year + "' ;");
                    }


                    commitcount++;
                    if (commitcount % 2000 == 0)
                    {
                        strBuilder.Append(" End;");  //SQL完成
                        try
                        {

                            //数据提交
                            Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                            strBuilder.Clear();
                            strBuilder.Append(" Begin "); //开始执行SQL
                        }
                        catch (Exception exception5)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "xmtz", "ALL", p_para.Sap_AEDAT, "插入hb_xmtz表发生异常:" + exception5);
                        }
                    }

                }


            if (strBuilder.ToString().Length > 9)
            {
                strBuilder.Append(" End;");  //SQL完成
                Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                strBuilder.Clear();

            }
            
            return Result;
        }
    }
}
