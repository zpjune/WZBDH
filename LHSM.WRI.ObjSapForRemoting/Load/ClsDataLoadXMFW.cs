using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：加载项目服务采购
    /// 编写时间：20161130
    /// 编写人：machunliang
    /// </summary>
    public class ClsDataLoadXMFW : ISAPLoadInterface
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
        private struct EKKO
        {
            public string strLOEKZ; //删除标识 
            public string strAEDAT; //创建日期 
            public string strEBELN; //采购凭证  主键
            public string strFRGKE; //批准标识 

            public string strBUKRS;   //甲方单位编码 
            public string strBUKRSMC; //甲方单位名称

            public string strLIFNR;   //服务商编码
            public string strLIFNRMC; //服务商名称

            public string strERNAM;   //采购订单创建人员账号
            public string strERNAMMC; //采购订单创建人员名称

            public string strUSERNAME;   //采购订单创建人员账号
            public string strUSERNAMEMC; //采购订单创建人员名称
        }

        /// <summary>
        /// 表EKPO结构体用于存取表数据
        /// </summary>
        private struct EKPO
        {
            public string strEBELP; //项目  
            public string strTXZ01; //短文本
            public string strBRTWR; //总价值
            public string strBUKRS; //公司代码 
            public string strPACKNO;//软件包编号
            public string strEBELN; //采购订单编号  
        }

        /// <summary>
        /// 表ESLL结构体用于存取表数据
        /// </summary>
        private struct ESLL
        {
            public string strPACKNO; //软件包编号
            public string strINTROW; //行号
            public string strASNUM;  //作业编号
            public string strKTEXT1; //短文本  
            public string strBRTWR;  //总价
        }


        private struct OTHER
        {
            public string strZEKKN;     //账户分配顺序号
            public string strPSPSPPNR;  //工作分解结构
            public string strPOSID;     //WBS元素  
            public string strPOST1;     //WBS描述
        }
        bool ISAPLoadInterface.SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;

            m_Conn = ClsUtility.GetConn();

            //暂时注销，正式时根据时间查出数据
            //DataTable dtEkkoDate = new DataTable();
            //try
            //{          
            //    string strSqlEkko = "SELECT AEDAT,EBELN FROM EKKO WHERE DLDATE='"+p_para.Sap_AEDAT+"'AND AEDAT like '" ;
            //    dtEkkoDate = m_Conn.GetSqlResultToDt(strSqlEkko);
            //}
            //catch (Exception exception)
            //{
            //    Result = false;
            //    ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询EKKO表发生异常:\t\n" + exception);
            //    return Result;
            //}

            EKKO structEKKO = new EKKO();
            DataTable dtEkko = new DataTable();

            try
            {
                //杜杨ERP2.0
                string stedate = p_para.Sap_AEDAT.Substring(0, 6);
                //查询EKKO数据               
                //string strSqlEkko = "SELECT * FROM EKKO WHERE BSART='Z012'AND AEDAT like '" + stedate + "%' and EBELN='7000231271' order by AEDAT asc"//单条测试
                string strSqlEkko = "SELECT * FROM EKKO WHERE BSART='X009'AND AEDAT like '" + stedate + "%'  order by AEDAT asc";
                dtEkko = m_Conn.GetSqlResultToDt(strSqlEkko);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询EKKO表发生异常:\t\n" + exception);
                return Result;
            }

            strBuilder.Length = 0;
            strBuilder.Append(" Begin "); //开始执行SQL

            //开始执行SQL
            foreach (DataRow subRowEkko in dtEkko.Rows)
            {
                //赋值EKKO表值  
                structEKKO.strLOEKZ = subRowEkko["LOEKZ"].ToString();
                structEKKO.strAEDAT = subRowEkko["AEDAT"].ToString();
                structEKKO.strEBELN = subRowEkko["EBELN"].ToString();
                structEKKO.strFRGKE = subRowEkko["FRGKE"].ToString();
                structEKKO.strFRGKE = string.IsNullOrEmpty(structEKKO.strFRGKE) ? "0" : structEKKO.strFRGKE;

                structEKKO.strBUKRS = subRowEkko["BUKRS"].ToString();
                structEKKO.strLIFNR = subRowEkko["LIFNR"].ToString();
                structEKKO.strERNAM = subRowEkko["ERNAM"].ToString();

                //服务商名称
                try
                {
                    structEKKO.strLIFNRMC = m_Conn.GetSqlResultToStr("SELECT NAME1 FROM LFA1 WHERE LIFNR='" + structEKKO.strLIFNR + "'");
                }
                catch (Exception exception1)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询LFA1表发生异常:\t\n" + exception1);
                    return Result;
                }

                //甲方单位名称
                try
                {
                    structEKKO.strBUKRSMC = m_Conn.GetSqlResultToStr("SELECT T.BUTXT FROM T001 T WHERE T.BUKRS='" + structEKKO.strBUKRS + "'");
                }
                catch (Exception exception1)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询T001表发生异常:\t\n" + exception1);
                    return Result;
                }

                if (string.IsNullOrEmpty(structEKKO.strBUKRSMC))
                {
                    continue;
                }
                //采购订单创建人员名称
                try
                {
                    structEKKO.strERNAMMC = m_Conn.GetSqlResultToStr("SELECT NAME_LAST FROM USR02 WHERE BNAME='" + structEKKO.strERNAM + "'");
                }
                catch (Exception exception1)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询USR02表发生异常:\t\n" + exception1);
                    return Result;
                }

                //采购订单创建人员账号
                //EKKO字段FRGKE值为1，获取审批人，为0不获取
                if (structEKKO.strFRGKE == "1")
                {
                    //当查询结果存在多行时，取最近日期的USERNAME
                    //当查询结果存在多行时，取最近时间的USERNAME
                    string strSqlCDHDR = "SELECT T.USERNAME,MAX(T.UDATE) AS UDATE,T.UTIME FROM CDHDR T WHERE T.TCODE='ME29N' AND T.OBJECTID='" + structEKKO.strEBELN + "' GROUP BY T.USERNAME,T.UTIME";
                    DataTable dtCDHDR = m_Conn.GetSqlResultToDt(strSqlCDHDR);
                    if (dtCDHDR != null && dtCDHDR.Rows.Count > 0)
                    {
                        string str_MaxTime = string.Empty;
                        for (int i = 0; i < dtCDHDR.Rows.Count; i++)
                        {
                            if (i == 0)
                            {
                                structEKKO.strUSERNAME = dtCDHDR.Rows[i]["USERNAME"].ToString();
                                str_MaxTime = dtCDHDR.Rows[i]["UTIME"].ToString();
                                continue;
                            }

                            string str_time = dtCDHDR.Rows[i]["UTIME"].ToString();
                            string[] str_times = str_time.Split(':');
                            if (str_times.Length < 2)
                            {
                                str_time = "0" + str_time;
                            }

                            if (String.Compare(str_MaxTime, str_time) < 0)
                            {
                                str_MaxTime = str_time;
                                structEKKO.strUSERNAME = dtCDHDR.Rows[i]["USERNAME"].ToString();
                            }
                        }
                    }

                    //采购订单创建人员名称
                    if (!string.IsNullOrEmpty(structEKKO.strUSERNAME))
                    {
                        try
                        {


                            structEKKO.strUSERNAMEMC = m_Conn.GetSqlResultToStr("SELECT NAME_LAST FROM USR02 WHERE BNAME='" + structEKKO.strUSERNAME + "'");
                        }
                        catch (Exception exception1)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询USR02表发生异常:\t\n" + exception1);
                            return Result;
                        }
                    }
                }

                EKPO structEKPO = new EKPO();
                DataTable dtEKPO = new DataTable();

                try
                {
                    //查询EKPO未删除的数据                    
                    string strSqlEKPO = " SELECT * FROM EKPO WHERE EBELN = '" + structEKKO.strEBELN + "' AND (LOEKZ <>'X' OR LOEKZ IS NULL)";
                    dtEKPO = m_Conn.GetSqlResultToDt(strSqlEKPO);
                }
                catch (Exception exception1)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询EKPO或LFA1表发生异常:\t\n" + exception1);
                    return Result;
                }

                foreach (DataRow subRowEKPO in dtEKPO.Rows)
                {
                    //赋值EKPO表值
                    structEKPO.strEBELP = subRowEKPO["EBELP"].ToString();
                    structEKPO.strTXZ01 = subRowEKPO["TXZ01"].ToString().Replace("'", "‘");
                    structEKPO.strEBELN = subRowEKPO["EBELN"].ToString();

                    structEKPO.strBRTWR = subRowEKPO["BRTWR"].ToString();
                    structEKPO.strBRTWR = string.IsNullOrEmpty(structEKPO.strBRTWR) ? "0" : structEKPO.strBRTWR;
                    structEKPO.strBRTWR = (Convert.ToDecimal(structEKPO.strBRTWR) / 10000).ToString("0.00");

                    structEKPO.strBUKRS = subRowEKPO["BUKRS"].ToString();
                    structEKPO.strPACKNO = subRowEKPO["PACKNO"].ToString();

                    //变量                   
                    string strSUB_PACKNO;
                    try
                    {

                        //取出 ESLL-SUB_PACKNO
                        strSUB_PACKNO = m_Conn.GetSqlResultToStr("SELECT SUB_PACKNO FROM ESLL WHERE PACKNO='" + structEKPO.strPACKNO + "' AND DEL IS NULL ");
                    }
                    catch (Exception exception2)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询ESLL表发生异常:\t\n" + exception2);
                        return Result;
                    }
                    OTHER structOther = new OTHER();
                    ESLL structESll = new ESLL();
                    if (!string.IsNullOrEmpty(strSUB_PACKNO))
                    {
                        //ESLL structESll = new ESLL();
                        DataTable dtESLL = new DataTable();
                        //变量                   
                        try
                        {
                            dtESLL = m_Conn.GetSqlResultToDt("SELECT * FROM ESLL WHERE PACKNO='" + strSUB_PACKNO + "' AND DEL IS NULL ");
                        }
                        catch (Exception exception2)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询ESLL表发生异常:\t\n" + exception2);
                            return Result;
                        }

                        if (dtESLL != null && dtESLL.Rows.Count > 0)
                        {

                            foreach (DataRow drESLL in dtESLL.Rows)
                            {

                                structESll.strPACKNO = drESLL["PACKNO"].ToString();
                                structESll.strINTROW = drESLL["INTROW"].ToString();
                                structESll.strASNUM = drESLL["SRVPOS"].ToString();
                                structESll.strKTEXT1 = drESLL["KTEXT1"].ToString();
                                structESll.strPACKNO = drESLL["PACKNO"].ToString();
                                structESll.strBRTWR = drESLL["BRTWR"].ToString();

                                //OTHER structOther = new OTHER();

                                try
                                {
                                    structOther.strZEKKN = m_Conn.GetSqlResultToStr("select t.ZEKKN  from ESKL t where t.PACKNO='" + structESll.strPACKNO + "' and t.introw='" + structESll.strINTROW + "'");
                                }
                                catch (Exception exception2)
                                {
                                    Result = false;
                                    ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询ESKL表发生异常:\t\n" + exception2);
                                    return Result;
                                }

                                if (!string.IsNullOrEmpty(structOther.strZEKKN))
                                {
                                    try
                                    {
                                        structOther.strPSPSPPNR = m_Conn.GetSqlResultToStr("select t.ps_psp_pnr  from EKKN t where t.ebeln='" + structEKPO.strEBELN + "' and t.ebelp='" + structEKPO.strEBELP + "'  and t.ZEKKN='" + structOther.strZEKKN + "'");
                                    }
                                    catch (Exception exception2)
                                    {
                                        Result = false;
                                        ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询EKKN表发生异常:\t\n" + exception2);
                                        return Result;
                                    }

                                    if (!string.IsNullOrEmpty(structOther.strPSPSPPNR))
                                    {
                                        try
                                        {
                                            //structOther.strPOSID = m_Conn.GetSqlResultToStr("select t.POSID from PRPS t,EKKN a where trim(t.poski) =trim(a.'" + structOther.strPSPSPPNR + "')");
                                            //structOther.strPOST1 = m_Conn.GetSqlResultToStr("select t.POST1 from PRPS t,EKKN a where trim(t.poski) =trim(a.'" + structOther.strPSPSPPNR + "')");
                                            structOther.strPOSID = m_Conn.GetSqlResultToStr("select t.POSID from PRPS t where POSID ='" + structOther.strPSPSPPNR.Replace("-", "").Replace(".", "").Trim() + "'");
                                            structOther.strPOST1 = m_Conn.GetSqlResultToStr("select t.POST1 from PRPS t where POSID ='" + structOther.strPSPSPPNR.Replace("-", "").Replace(".", "").Trim() + "'");
                                        }
                                        catch (Exception exception2)
                                        {
                                            Result = false;
                                            ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表过程中查询PRPS表发生异常:\t\n" + exception2);
                                            return Result;
                                        }
                                    }
                                }
                                strBuilder.Append(" DELETE FROM HB_XMFW WHERE XMFW_DDH='" + subRowEkko["EBELN"].ToString() + "' AND XMFW_XMH='" + subRowEKPO["EBELP"].ToString() + "' AND XMFW_MXBH='" + structOther.strZEKKN + "';");

                                //添加数据
                                strBuilder.Append(" INSERT INTO HB_XMFW");
                                strBuilder.Append("(XMFW_ID,XMFW_DATE,XMFW_DDH,XMFW_CJRYBM,XMFW_CJRYMC,XMFW_SPRYBM,XMFW_SPRYMC,XMFW_SP,");
                                strBuilder.Append("XMFW_DDSC,XMFW_JFDWBM,XMFW_JFDWMC,XMFW_XMH,XMFW_CGFUMC,XMFW_FUSBM,XMFW_FUSMC,XMFW_JE,");
                                strBuilder.Append("XMFW_MXBH,XMFW_GZBH,XMFW_GZNR,XMFW_WBSBM,XMFW_WBSMC,XMFW_DJ,XMFW_DJJE,XMFW_HT)");
                                strBuilder.Append(" VALUES(");
                                strBuilder.Append("SQ_XMFW.NEXTVAL,");
                                strBuilder.Append("'" + p_para.Sap_AEDAT.Substring(0, 6) + "',");
                                strBuilder.Append("'" + structEKKO.strEBELN + "',");
                                strBuilder.Append("'" + structEKKO.strERNAM + "',");
                                strBuilder.Append("'" + structEKKO.strERNAMMC + "',");
                                strBuilder.Append("'" + structEKKO.strUSERNAME + "',");
                                strBuilder.Append("'" + structEKKO.strUSERNAMEMC + "',");
                                strBuilder.Append("'" + structEKKO.strFRGKE + "',");
                                strBuilder.Append("'" + structEKKO.strLOEKZ + "',");
                                strBuilder.Append("'" + structEKKO.strBUKRS + "',");
                                strBuilder.Append("'" + structEKKO.strBUKRSMC + "',");
                                strBuilder.Append("'" + structEKPO.strEBELP + "',");
                                strBuilder.Append("'" + structEKPO.strTXZ01 + "',");
                                strBuilder.Append("'" + structEKKO.strLIFNR + "',");
                                strBuilder.Append("'" + structEKKO.strLIFNRMC + "',");
                                strBuilder.Append("'" + structEKPO.strBRTWR + "',");
                                strBuilder.Append("'" + structOther.strZEKKN + "',");
                                strBuilder.Append("'" + structESll.strASNUM + "',");
                                strBuilder.Append("'" + structESll.strKTEXT1 + "',");
                                strBuilder.Append("'" + structOther.strPOSID + "',");
                                strBuilder.Append("'" + structOther.strPOST1 + "',");
                                strBuilder.Append("'" + (string.IsNullOrEmpty(structESll.strBRTWR) ? "0" : (Convert.ToDecimal(structESll.strBRTWR) / 10000).ToString("F2")) + "',");
                                strBuilder.Append("'" + structESll.strBRTWR + "',");
                                strBuilder.Append("'" + structEKPO.strBRTWR + "'");
                                strBuilder.Append(");");
                            }
                           
                        }
                    }
                   
                }
            }

            strBuilder.Append(" End;");  //SQL完成
            try
            {
                if (strBuilder.ToString().Length < 14)
                {
                    return true;
                }
                //数据提交
                Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                m_Conn.Dispose();
            }
            catch (Exception exception5)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "xmfw", "ALL", p_para.Sap_AEDAT, "插入hb_xmfw表发生异常:" + exception5);
            }
            return Result;
        }
    }
}
