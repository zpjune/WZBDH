using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadCG : ISAPLoadInterface
    {
        //数据库连接
        private static ClsDBConnection m_Conn = null;

        /// <summary>
        /// 存储sql字符串变量
        /// </summary>
        private StringBuilder strBuilder = new StringBuilder();

        /// <summary>
        /// 表ZC10MMDG025和ZC10MMDG025B结构体用于存取表数据
        /// </summary>
        private struct ZC025
        {
            public string strRSNUM;     //需求计划号
            public string strRSPOS;     //需求项目号
            public string strZJHLXO;    //需求计划类型
            public string strZJHLX0T;   //需求计划文本             
            public string strZJHLX1;    //需求计划类型1
            public string strZJHLX1T;   //需求计划文本1 
            public string strZJHLX2;    //需求计划类型2
            public string strZJHLX2T;   //需求计划文本2
            public string strRSDAT;     //需求计划提报日期
            public string strCPUTM;     //需求计划提报时间
            public string strWERKS;     //工厂      
            public string strWERKSTXT;  //工厂名称   
            public string strMATNR;     //物料号
            public string strMAKTX;     //物料描述
        }


        private struct OtherData
        {
            public string strYBRTWR;    //预计采购金额
            public string strEBELNCount;     //采购凭证条数        
            public string strBRTWR;     //采购订单总价
            public string strPREIS;     //评价价格
            public string strEBELPCount;     //采购订单行项目条数
            public string strDMBTR;     //完成入库金额
        }
        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;

            m_Conn = ClsUtility.GetConn();


            string m_time = p_para.Sap_AEDAT.Substring(0, 6);

            #region 查询主表信息
            ZC025 strZC025 = new ZC025();

            DataTable dtZC025 = new DataTable();
            try
            {
                StringBuilder sbSqlZC025 = new StringBuilder();
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,a.ZJHLX1,a.ZJHLX1T,a.ZJHLX2,a.ZJHLX2T,");
                sbSqlZC025.Append(" b.XMH,'' as MATNR,'' as MAKTX");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,(select count(RSPOS) XMH,RSNUM  from RESB group by RSNUM) b");
                sbSqlZC025.Append(" where b.RSNUM = a.RSNUM and substr(replace(a.rsdat,'.',''),0,6)='" + m_time + "'");
                sbSqlZC025.Append(" union all ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,a.ZJHLX1,a.ZJHLX1T,a.ZJHLX2,a.ZJHLX2T,");
                sbSqlZC025.Append(" b.XMH,'' as MATNR,'' as MAKTX");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,(select count(POSNR) XMH,vbeln from VBAP group by vbeln) b");
                sbSqlZC025.Append(" where b.vbeln = a.RSNUM and substr(replace(a.rsdat,'.',''),0,6)='" + m_time + "'");

                dtZC025 = m_Conn.GetSqlResultToDt(sbSqlZC025.ToString());
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG025表发生异常:\t\n" + exception);
                return Result;
            }

            DataTable dtZC025B = new DataTable();
            try
            {
                string sbSqlZC025B = " select";
                sbSqlZC025B += " REQ_NUM,BDTER,CPUTM,count(REQ_ITEM) as REQ_ITEM,ZJHLX0,WERKS,ZJHLX1,ZJHLX2 ";
                sbSqlZC025B += " from ZC10MMDG025B WHERE (XLOEK<>'X' or XLOEK IS NULL) and substr(replace(BDTER, '.', ''), 0, 6) = '" + m_time + "'";
                sbSqlZC025B += " group by REQ_NUM,BDTER,CPUTM,ZJHLX0,WERKS,ZJHLX1,ZJHLX2";

                dtZC025B = m_Conn.GetSqlResultToDt(sbSqlZC025B);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG025B表发生异常:\t\n" + exception);
                return Result;
            }

            DataRow drZC025 = null;
            foreach (DataRow subRowZC025B in dtZC025B.Rows)
            {
                drZC025 = dtZC025.NewRow();
                drZC025["RSNUM"] = subRowZC025B["REQ_NUM"].ToString();    //需求计划号 
                drZC025["XMH"] = subRowZC025B["REQ_ITEM"].ToString();    //需求项目号                        
                drZC025["ZJHLX0"] = subRowZC025B["ZJHLX0"].ToString();   //需求计划类型   
                drZC025["ZJHLX0T"] = string.Empty;   //需求计划文本                       
                drZC025["ZJHLX1"] = subRowZC025B["ZJHLX1"].ToString();    //需求计划类型1  
                drZC025["ZJHLX1T"] = string.Empty;  //需求计划文本1 
                drZC025["ZJHLX2"] = subRowZC025B["ZJHLX2"].ToString();   //需求计划类型2  
                drZC025["ZJHLX2T"] = string.Empty;   //需求计划文本2         
                drZC025["RSDAT"] = subRowZC025B["BDTER"].ToString();    //需求计划提报日期
                drZC025["CPUTM"] = subRowZC025B["CPUTM"].ToString();    //需求计划提报时间
                drZC025["WERKS"] = subRowZC025B["WERKS"].ToString();    //工厂       
                drZC025["MATNR"] = string.Empty;    //物料号          
                drZC025["MAKTX"] = string.Empty;    //物料描述   

                dtZC025.Rows.Add(drZC025);
            }

            #endregion
            DataTable dtGC = new DataTable();
            try
            {
                string sbGC = " select * from  T001W";



                dtGC = m_Conn.GetSqlResultToDt(sbGC);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG025B表发生异常:\t\n" + exception);
                return Result;
            }




            int commitcount = 0;
            int commitMax = 10;
            strBuilder.Length = 0;
            strBuilder.Append(" Begin "); //开始执行SQL

            foreach (DataRow subRowZC025 in dtZC025.Rows)
            {
                strBuilder.Append(" DELETE FROM HB_WZCG WHERE WL_JHH='" + subRowZC025["RSNUM"].ToString() + "';");
                strZC025.strRSNUM = subRowZC025["RSNUM"].ToString();     //需求计划号
                strZC025.strRSPOS = subRowZC025["XMH"].ToString();     //需求项目号                
                strZC025.strZJHLXO = subRowZC025["ZJHLX0"].ToString();    //需求计划类型
                strZC025.strZJHLX0T = subRowZC025["ZJHLX0T"].ToString();   //需求计划文本                
                strZC025.strZJHLX1 = subRowZC025["ZJHLX1"].ToString();    //需求计划类型1
                strZC025.strZJHLX1T = subRowZC025["ZJHLX1T"].ToString();   //需求计划文本1 
                strZC025.strZJHLX2 = subRowZC025["ZJHLX2"].ToString();    //需求计划类型2
                strZC025.strZJHLX2T = subRowZC025["ZJHLX2T"].ToString();   //需求计划文本2
                strZC025.strRSDAT = subRowZC025["RSDAT"].ToString();     //需求计划提报日期
                strZC025.strCPUTM = subRowZC025["CPUTM"].ToString();     //需求计划提报时间
                strZC025.strWERKS = subRowZC025["WERKS"].ToString();     //工厂
                strZC025.strWERKSTXT = dtGC.Select("WERKS='" + subRowZC025["WERKS"].ToString() + "'")[0]["NAME1"].ToString();    //工厂
                strZC025.strMATNR = subRowZC025["MATNR"].ToString();     //物料号
                strZC025.strMAKTX = subRowZC025["MAKTX"].ToString();     //物料描述               

                //if (!string.IsNullOrEmpty(strZC025.strWERKS))
                //{
                //    try
                //    {
                //        //按RSNUM汇总预计采购金额
                //        string strSqlT001 = "SELECT T.BUTXT FROM T001 T WHERE T.BUKRS='" + strZC025.strWERKS + "' ";
                //        strZC025.strWERKSTXT = m_Conn.GetSqlResultToStr(strSqlT001);
                //    }
                //    catch (Exception exception2)
                //    {
                //        Result = false;
                //        ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询T001表发生异常:\t\n" + exception2);
                //        return Result;
                //    }
                //}
                #region 026公司级取数

                OtherData otherdata026 = new OtherData();
                string strSqlZC026 = string.Empty;
                try
                {
                    //按RSNUM汇总预计采购金额
                    strSqlZC026 = "select sum(Z_BRTWR) as Z_BRTWR from ZC10MMDG026 ";
                    strSqlZC026 += " where RSNUM='" + strZC025.strRSNUM + "'";
                    otherdata026.strYBRTWR = m_Conn.GetSqlResultToStr(strSqlZC026);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG026表发生异常:\t\n" + exception2);
                    return Result;
                }

                try
                {
                    //完成采购条数
                    strSqlZC026 = "select count(1) Z_BRTWR from ZC10MMDG026 ";
                    strSqlZC026 += " where RSNUM='" + strZC025.strRSNUM + "' and zebeln is not null";
                    otherdata026.strEBELNCount = m_Conn.GetSqlResultToStr(strSqlZC026);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG026表发生异常:\t\n" + exception2);
                    return Result;
                }

                try
                {
                    //汇总BRTWR，输出（完成采购金额）
                    string strSqlEKPO = "SELECT SUM(B.BRTWR) FROM ZC10MMDG026 A ,EKPO B WHERE A.ZEBELN=B.EBELN AND A.ZEBELP=B.EBELP";
                    strSqlEKPO += " AND A.RSNUM='" + strZC025.strRSNUM + "'";
                    otherdata026.strBRTWR = m_Conn.GetSqlResultToStr(strSqlEKPO);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG026表发生异常:\t\n" + exception2);
                    return Result;
                }

                #endregion

                #region 203_HZ二级单位

                OtherData otherdata203 = new OtherData();
                try
                {
                    //汇总PREIS 输出（预计采购金额）
                    string strSqlZP203_HZ = "select SUM(B.PREIS* MENGE)";
                    strSqlZP203_HZ += " from ZP10MMDG203_HZ A ,EBAN B";
                    strSqlZP203_HZ += " WHERE A.BANFN=B.BANFN AND A.BNFPO=B.BNFPO ";
                    strSqlZP203_HZ += " and A.RSNUM='" + strZC025.strRSNUM + "'";
                    otherdata203.strYBRTWR = m_Conn.GetSqlResultToStr(strSqlZP203_HZ);



                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询EBAN表发生异常:\t\n" + exception2);
                    return Result;
                }

                try
                {
                    //汇总BRTWR，输出（完成采购金额）
                    string strSqlEKPO = "select SUM(B.BRTWR)";
                    strSqlEKPO += " from ZP10MMDG203_HZ A ,EKPO B";
                    strSqlEKPO += " WHERE A.BNFPO=B.BNFPO AND A.BANFN=B.BANFN";
                    strSqlEKPO += " and A.RSNUM='" + strZC025.strRSNUM + "'";
                    otherdata203.strBRTWR = m_Conn.GetSqlResultToStr(strSqlEKPO);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询EKPO表发生异常:\t\n" + exception2);
                    return Result;
                }

                try
                {
                    //汇总EBELP条数，输出（完成采购条数）
                    string strSqlEKPO = "select COUNT(1)";
                    strSqlEKPO += " from ZP10MMDG203_HZ A ,EKPO B";
                    strSqlEKPO += " WHERE A.BNFPO=B.BNFPO AND A.BANFN=B.BANFN";
                    strSqlEKPO += " and A.RSNUM='" + strZC025.strRSNUM + "'";
                    otherdata203.strEBELNCount = m_Conn.GetSqlResultToStr(strSqlEKPO);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询EKPO表发生异常:\t\n" + exception2);
                    return Result;
                }
                #endregion

                OtherData otherdata = new OtherData();
                DataTable dtOther = new DataTable();
                try
                {
                    //汇总EBELP条数，输出（完成采购条数）
                    StringBuilder sb = new StringBuilder();
                    sb.Append(" SELECT SUM(TS) TS, SUM(DMBTR) DMBTR");
                    sb.Append(" FROM (select COUNT(1) AS TS, SUM(C.DMBTR) AS DMBTR");
                    sb.Append(" from ZP10MMDG203_HZ A, EKPO B, MSEG C");
                    sb.Append(" WHERE A.BNFPO = B.BNFPO");
                    sb.Append(" AND A.BANFN = B.BANFN");
                    sb.Append(" AND B.EBELN = C.EBELN");
                    sb.Append(" AND B.EBELP = C.EBELP");
                    sb.Append(" AND (C.BWART='101' OR C.BWART='105') ");
                    sb.Append(" AND A.RSNUM='" + strZC025.strRSNUM + "'");
                    sb.Append(" UNION ALL");
                    sb.Append(" SELECT COUNT(1) AS TS, SUM(B.DMBTR) AS DMBTR");
                    sb.Append(" FROM ZC10MMDG026 A, MSEG B ");
                    sb.Append(" WHERE A.ZEBELN = B.EBELN");
                    sb.Append(" AND A.ZEBELP = B.EBELP");
                    sb.Append(" AND (B.BWART ='101' OR B.BWART ='105')");
                    sb.Append(" AND A.RSNUM='" + strZC025.strRSNUM + "')");
                    dtOther = m_Conn.GetSqlResultToDt(sb.ToString());

                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询MSEG表发生异常:\t\n" + exception2);
                    return Result;
                }

                decimal YBRTWR026 = Convert.ToDecimal(string.IsNullOrEmpty(otherdata026.strYBRTWR) ? "0" : otherdata026.strYBRTWR);
                decimal YBRTWR203 = Convert.ToDecimal(string.IsNullOrEmpty(otherdata203.strYBRTWR) ? "0" : otherdata203.strYBRTWR);
                otherdata.strYBRTWR = (YBRTWR026 + YBRTWR203).ToString("F2");    //预计采购金额

                decimal EBELNCount026 = Convert.ToDecimal(string.IsNullOrEmpty(otherdata026.strEBELNCount) ? "0" : otherdata026.strEBELNCount);
                decimal EBELNCount203 = Convert.ToDecimal(string.IsNullOrEmpty(otherdata203.strEBELNCount) ? "0" : otherdata203.strEBELNCount);
                otherdata.strEBELNCount = (EBELNCount026 + EBELNCount203).ToString("F0");     //采购凭证条数  

                decimal BRTWR026 = Convert.ToDecimal(string.IsNullOrEmpty(otherdata026.strBRTWR) ? "0" : otherdata026.strBRTWR);
                decimal BRTWR203 = Convert.ToDecimal(string.IsNullOrEmpty(otherdata203.strBRTWR) ? "0" : otherdata203.strBRTWR);
                otherdata.strBRTWR = (BRTWR026 + BRTWR203).ToString("F2");     //采购订单总价



                if (dtOther != null && dtOther.Rows.Count > 0)
                {
                    otherdata.strEBELPCount = (dtOther.Rows[0]["TS"] ?? "0").ToString();     //采购订单行项目条数
                    otherdata.strDMBTR = (dtOther.Rows[0]["DMBTR"] ?? "0").ToString();     //本位币金额
                }

                #region 插入sql

                strBuilder.Append(" INSERT INTO HB_WZCG");
                strBuilder.Append(" (WL_ID,WL_JHH,WL_XMH,WL_LX ,WL_WB ,WL_LX1,WL_WB1,WL_LX2,WL_WB2,");
                strBuilder.Append(" WL_JHTBRQ,WL_JHTBSJ,WL_GC,WL_GCMC,WL_WL,WL_WLMS,WL_YJCGJE,");
                strBuilder.Append(" WL_CGPZTS,WL_CGDDZJ,WL_BWBJE,WL_CGDDTS");
                strBuilder.Append(" ) VALUES(");
                strBuilder.Append("SQ_WZCG.NEXTVAL,");
                strBuilder.Append("'" + strZC025.strRSNUM + "',");//需求计划号	        
                strBuilder.Append("'" + strZC025.strRSPOS + "',");//需求项目号	        
                strBuilder.Append("'" + strZC025.strZJHLXO + "',");//需求计划类型	      
                strBuilder.Append("'" + strZC025.strZJHLX0T + "',");//需求计划文本	      
                strBuilder.Append("'" + strZC025.strZJHLX1 + "',");//需求计划类型1	      
                strBuilder.Append("'" + strZC025.strZJHLX1T + "',");//需求计划文本1	      
                strBuilder.Append("'" + strZC025.strZJHLX2 + "',");//需求计划类型2	      
                strBuilder.Append("'" + strZC025.strZJHLX2T + "',");//需求计划文本2	      
                strBuilder.Append("'" + strZC025.strRSDAT.Replace(".", "") + "',");//需求计划提报日期	  
                strBuilder.Append("'" + strZC025.strCPUTM + "',");//需求计划提报时间	  
                strBuilder.Append("'" + strZC025.strWERKS + "',");//工厂	
                strBuilder.Append("'" + strZC025.strWERKSTXT + "',");//工厂	   
                strBuilder.Append("'" + strZC025.strMATNR + "',");//物料号	            
                strBuilder.Append("'" + strZC025.strMAKTX + "',");//物料描述	 
                strBuilder.Append("'" + otherdata.strYBRTWR + "',");        //预计采购金额	      
                strBuilder.Append("'" + otherdata.strEBELNCount + "',");    //采购凭证条数	      
                strBuilder.Append("'" + otherdata.strBRTWR + "',");         //采购订单总价	      

                strBuilder.Append("'" + otherdata.strDMBTR + "',");         //本位币金额	        
                strBuilder.Append("'" + otherdata.strEBELPCount + "'");     //采购订单行项目条数	
                strBuilder.Append(");");

                #endregion

                commitcount++;
                if (commitcount % commitMax == 0)
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
                        ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表发生异常:" + exception5);
                    }
                }
            }

              //SQL完成
            try
            {
                if (strBuilder.ToString().Length < 14)
                {
                    return true;
                }
                strBuilder.Append(" End;");
                //数据提交
                Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
            }
            catch (Exception exception5)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表发生异常:" + exception5);
            }

            return Result;
        }
    }
}
