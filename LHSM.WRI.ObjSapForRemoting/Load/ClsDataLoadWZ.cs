using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadWZ : ISAPLoadInterface
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
            public string strRSDAT;     //需求计划提报日期
            public string strCPUTM;     //需求计划提报时间
            public string strZJHLXO;    //需求计划类型
            public string strZJHLX0T;   //需求计划文本
            public string strWERKS;     //工厂
            public string strWNAME;     //工厂名称
            public string strZJHLX1;    //需求计划类型1
            public string strZJHLX1T;   //需求计划文本1 
            public string strZJHLX2;    //需求计划类型2
            public string strZJHLX2T;   //需求计划文本2
            public string strBWART;     //移动类型
            public string strRSPOS;     //需求项目号
            public string strMATNR;     //物料号
            public string strARKTX;     //物料描述
            public string strMATKL;     //物料组
            public string strWGBEZ;     //物料组描述
            public string strERDAT;     //创建日期
            public string strERZET;     //创建时间
        }

        /// <summary>
        /// 表ZC10MMDG026和ZP10MMDG203_HZ结构体用于存取表数据
        /// </summary>
        private struct ZC026
        {
            public string strAEDAT;     //采购申请生成日期
            public string strZCGJHNUM;  //采购计划号
            public string strZCGJHPOS;  //采购计划项目
            public string strMATNR;     //采购物料号
            public string strMAKTX;     //采购物料描述
            public string strMATKL;     //采购物料组
            public string strWGBEZ;     //采购物料组描述
            public string strZEBELN;    //采购订单
            public string strZEBELP;    //采购订单项目号
        }

        /// <summary>
        /// 表EKKO和EKPO结构体用于存取表数据
        /// </summary>
        private struct EKKPO
        {
            public string strAEDAT;       //采购订单生成日期
            public string strLIFNR;       //供应商
            public string strNAME1;       //供应商名称 
            public string strNETPR;       //采购单价
            public string strMENGE;       //采购数量
            public string strMEINS;       //采购单位
            public string strNETWR;       //采购总价
        }

        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;

            m_Conn = ClsUtility.GetConn();

            #region 查询主表信息
            ZC025 strZC025 = new ZC025();

            DataTable dtZC025 = new DataTable();
            try
            {
                StringBuilder sbSqlZC025 = new StringBuilder();
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,");
                sbSqlZC025.Append(" to_number(b.RSPOS) as XMH,b.MATNR,'' MAKTX,'' MATKL,'' WGBEZ,'' ERDAT,'' ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,RESB b where a.RSNUM=b.RSNUM and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                sbSqlZC025.Append(" union all ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,");
                sbSqlZC025.Append(" to_number(b.POSNR) as XMH,b.MATNR,b.ARKTX MAKTX,'' MATKL,'' WGBEZ,b.ERDAT,b.ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,VBAP b where a.RSNUM=b.vbeln and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                sbSqlZC025.Append(" union ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,");
                sbSqlZC025.Append(" to_number(b.RSPOS)as XMH,b.MATNR,'' MAKTX,'' MATKL,'' WGBEZ,'' ERDAT,'' ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,ZC10MMDG026 b where a.RSNUM=b.RSNUM and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                sbSqlZC025.Append(" union ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,");
                sbSqlZC025.Append(" to_number(b.RSPOS) as XMH,b.MATNR,'' MAKTX,'' MATKL,'' WGBEZ,'' ERDAT,'' ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,ZP10MMDG203_HZ b where a.RSNUM=b.RSNUM and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                dtZC025 = m_Conn.GetSqlResultToDt(sbSqlZC025.ToString());

            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询ZC10MMDG025表发生异常:\t\n" + exception);
                return Result;
            }

            DataTable dtZC025B = new DataTable();
            try
            {
                string sbSqlZC025B = " select";
                sbSqlZC025B += " REQ_NUM,BDTER,CPUTM,REQ_ITEM,MATNR,ZJHLX0,WERKS,MAKTX,MATKL,WGBEZ,ZJHLX1,ZJHLX2,BWART";
                sbSqlZC025B += " from ZC10MMDG025B WHERE REQ_NUM IS NOT NULL and DLDATE='" + p_para.Sap_AEDAT + "'";

                dtZC025B = m_Conn.GetSqlResultToDt(sbSqlZC025B);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询ZC10MMDG025B表发生异常:\t\n" + exception);
                return Result;
            }

            DataRow drZC025 = null;
            foreach (DataRow subRowZC025B in dtZC025B.Rows)
            {
                drZC025 = dtZC025.NewRow();
                drZC025["RSNUM"] = subRowZC025B["REQ_NUM"].ToString();   //需求计划号                      
                drZC025["RSDAT"] = subRowZC025B["BDTER"].ToString();    //需求计划提报日期
                drZC025["CPUTM"] = subRowZC025B["CPUTM"].ToString();    //需求计划提报时间
                drZC025["ZJHLX0"] = subRowZC025B["ZJHLX0"].ToString();   //需求计划类型   
                drZC025["ZJHLX0T"] = string.Empty;   //需求计划文本  
                drZC025["WERKS"] = subRowZC025B["WERKS"].ToString();    //工厂                 
                drZC025["XMH"] = subRowZC025B["REQ_ITEM"].ToString();    //需求项目号        
                drZC025["MATNR"] = subRowZC025B["MATNR"].ToString();    //物料号
                drZC025["MAKTX"] = subRowZC025B["MAKTX"].ToString();    //物料描述        
                drZC025["MATKL"] = subRowZC025B["MATKL"].ToString();    //物料组          
                drZC025["WGBEZ"] = subRowZC025B["WGBEZ"].ToString();   //物料组描述      
                drZC025["ERDAT"] = string.Empty;    //创建日期        
                drZC025["ERZET"] = string.Empty;   //创建时间  

                dtZC025.Rows.Add(drZC025);

            }

            #endregion

            int commitcount = 0;
            int commitMax = 500;
            strBuilder.Length = 0;
            strBuilder.Append(" Begin "); //开始执行SQL
            List<string> ls = new List<string>();

            foreach (DataRow subRowZC025 in dtZC025.Rows)
            {
                if (!ls.Contains(subRowZC025["RSNUM"].ToString()))
                {
                    strBuilder.Append(" DELETE FROM HB_WZ WHERE WL_JHH='" + subRowZC025["RSNUM"].ToString() + "';");
                    ls.Add(subRowZC025["RSNUM"].ToString());
                }

                strZC025.strRSNUM = subRowZC025["RSNUM"].ToString();     //需求计划号
                strZC025.strRSDAT = subRowZC025["RSDAT"].ToString().Replace(".", "");     //需求计划提报日期
                strZC025.strCPUTM = subRowZC025["CPUTM"].ToString();     //需求计划提报时间
                strZC025.strZJHLXO = subRowZC025["ZJHLX0"].ToString();    //需求计划类型
                strZC025.strZJHLX0T = subRowZC025["ZJHLX0T"].ToString();   //需求计划文本 
                strZC025.strWERKS = subRowZC025["WERKS"].ToString();     //工厂
                strZC025.strRSPOS = subRowZC025["XMH"].ToString();     //需求项目号
                strZC025.strMATNR = subRowZC025["MATNR"].ToString();     //物料号
                strZC025.strARKTX = subRowZC025["MAKTX"].ToString();     //物料描述
                strZC025.strMATKL = subRowZC025["MATKL"].ToString();     //物料组
                strZC025.strWGBEZ = subRowZC025["WGBEZ"].ToString();     //物料组描述
                strZC025.strERDAT = subRowZC025["ERDAT"].ToString();     //创建日期
                strZC025.strERZET = subRowZC025["ERZET"].ToString();     //创建时间
                //工厂名称
                DataTable dtGC = new DataTable();
                try
                {
                    string sbGC = " select name1 from  T001W where WERKS='" + strZC025.strWERKS + "'";
                    dtGC = m_Conn.GetSqlResultToDt(sbGC);
                }
                catch (Exception exception)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cg", "ALL", p_para.Sap_AEDAT, "插入hb_wzcg表过程中查询ZC10MMDG025B表发生异常:\t\n" + exception);
                    return Result;
                }
                if (dtGC != null && dtGC.Rows.Count > 0)
                {
                    strZC025.strWNAME = (dtGC.Rows[0]["NAME1"] ?? string.Empty).ToString();
                }

                #region 采购

                DataTable dtZC026 = new DataTable();
                try
                {
                    string strSqlZC026 = "select RSNUM,RSPOS,AEDAT,MATNR,MAKTX,MATKL,";
                    strSqlZC026 += " WGBEZ,ZEBELN,ZEBELP";
                    strSqlZC026 += " from ZC10MMDG026";
                    strSqlZC026 += " where  RSNUM='" + strZC025.strRSNUM + "' and TO_NUMBER(RSPOS)=" + Convert.ToInt16(strZC025.strRSPOS).ToString();
                    dtZC026 = m_Conn.GetSqlResultToDt(strSqlZC026);

                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询ZC10MMDG026表发生异常:\t\n" + exception2);
                    return Result;
                }

                //二级单位
                DataTable dtZP203_HZ = new DataTable();
                try
                {
                    string strSqlZP203_HZ = "select BANFN,BNFPO,AEDAT,MATNR,MATKL,EBELN,EBELP";
                    strSqlZP203_HZ += " from ZP10MMDG203_HZ";
                    strSqlZP203_HZ += " where RSNUM='" + strZC025.strRSNUM + "' and TO_NUMBER(RSPOS)=" + Convert.ToInt16(strZC025.strRSPOS);
                    strSqlZP203_HZ += " and  MATNR='" + strZC025.strMATNR + "'";
                    dtZP203_HZ = m_Conn.GetSqlResultToDt(strSqlZP203_HZ);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询ZP10MMDG203_HZ表发生异常:\t\n" + exception2);
                    return Result;
                }

                //二级单位和公司级合并数据
                if (dtZP203_HZ != null && dtZP203_HZ.Rows.Count > 0)
                {
                    DataRow drZC026 = null;
                    foreach (DataRow subRowZP203 in dtZP203_HZ.Rows)
                    {
                        drZC026 = dtZC026.NewRow();

                        drZC026["RSNUM"] = subRowZP203["BANFN"].ToString();    //采购申请号
                        drZC026["RSPOS"] = subRowZP203["BNFPO"].ToString();     //采购申请行项目号
                        drZC026["AEDAT"] = subRowZP203["AEDAT"].ToString();     //采购申请生成日期
                        drZC026["MATNR"] = subRowZP203["MATNR"].ToString();   //采购物料号
                        drZC026["MAKTX"] = string.Empty;   //采购物料描述
                        drZC026["MATKL"] = subRowZP203["MATKL"].ToString();   //采购物料组
                        drZC026["WGBEZ"] = string.Empty;  //采购物料组描述
                        drZC026["ZEBELN"] = subRowZP203["EBELN"].ToString(); //采购订单
                        drZC026["ZEBELP"] = subRowZP203["EBELP"].ToString();   //采购订单项目号
                        dtZC026.Rows.Add(drZC026);

                    }
                }

                #endregion

                ZC026 strZC026 = new ZC026();
                foreach (DataRow subRowZC026 in dtZC026.Rows)
                {
                    strZC026.strZCGJHNUM = subRowZC026["RSNUM"].ToString();   //采购计划号
                    strZC026.strZCGJHPOS = subRowZC026["RSPOS"].ToString();   //采购计划项目
                    strZC026.strAEDAT = subRowZC026["AEDAT"].ToString();     //采购申请生成日期
                    strZC026.strMATNR = subRowZC026["MATNR"].ToString();     //采购物料号
                    strZC026.strMAKTX = subRowZC026["MAKTX"].ToString();     //采购物料描述
                    strZC026.strMATKL = subRowZC026["MATKL"].ToString();     //采购物料组
                    strZC026.strWGBEZ = subRowZC026["WGBEZ"].ToString();     //采购物料组描述
                    strZC026.strZEBELN = subRowZC026["ZEBELN"].ToString();     //采购订单
                    strZC026.strZEBELP = subRowZC026["ZEBELP"].ToString();     //采购订单项目号
                    DataTable dtEKPO = new DataTable();
                    if (strZC026.strZEBELN != null && strZC026.strZEBELN != "")
                    {
                        try
                        {
                            string strSqlEKPO = "select EBELN,EBELP,BUKRS,AEDAT,NETPR,MENGE,NETWR,MEINS  from EKPO";
                            strSqlEKPO += " where EBELN='" + strZC026.strZEBELN + "' and EBELP ='" + strZC026.strZEBELP + "'";
                            strSqlEKPO += " and (LOEKZ<>'L' or LOEKZ IS NULL)";

                            dtEKPO = m_Conn.GetSqlResultToDt(strSqlEKPO);
                        }
                        catch (Exception exception2)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询EKPO表发生异常:\t\n" + exception2);
                            return Result;
                        }

                    }
                    else
                    {
                        try
                        {
                            string strSqlEKPO = "select EBELN,EBELP,BUKRS,AEDAT,NETPR,MENGE,NETWR,MEINS from EKPO";
                            strSqlEKPO += " where BANFN='" + strZC026.strZCGJHNUM + "' and TO_NUMBER(BNFPO)=" + Convert.ToInt16(strZC026.strZCGJHPOS);
                            strSqlEKPO += " and (LOEKZ<>'L' or LOEKZ IS NULL)";

                            dtEKPO = m_Conn.GetSqlResultToDt(strSqlEKPO);
                        }
                        catch (Exception exception2)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询EKPO表发生异常:\t\n" + exception2);

                        }
                    }
                    if (dtEKPO!=null&&dtEKPO.Rows.Count>0)
                    {
                    strZC026.strZEBELN = (dtEKPO.Rows[0]["EBELN"] ?? string.Empty).ToString();    //采购订单
                    strZC026.strZEBELP = (dtEKPO.Rows[0]["EBELP"] ?? string.Empty).ToString();     //采购订单项目号
                    }
                    #region 采购订单生成


                    if (dtEKPO != null && dtEKPO.Rows.Count > 0)
                    {
                        foreach (DataRow subRowEKPO in dtEKPO.Rows)
                        {
                            #region EKKO数据查询
                            EKKPO strEKKPO = new EKKPO();
                            strEKKPO.strMENGE = subRowEKPO["MENGE"].ToString();
                            strEKKPO.strNETWR = subRowEKPO["NETWR"].ToString();
                            strEKKPO.strNETPR = subRowEKPO["NETPR"].ToString();
                            strEKKPO.strMEINS = subRowEKPO["MEINS"].ToString();

                            DataTable dtEKKO = new DataTable();
                            try
                            {
                                string strSqlEKKO = "select a.AEDAT,a.LIFNR,b.NAME1 from EKKO a,LFA1 b ";//ERP1.0取数方式
                                strSqlEKKO += " where a.EBELN='" + subRowEKPO["EBELN"].ToString() + "'";
                                strSqlEKKO += " and a.LIFNR=b.LIFNR";
                                dtEKKO = m_Conn.GetSqlResultToDt(strSqlEKKO);
                            }
                            catch (Exception exception2)
                            {
                                Result = false;
                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表过程中查询EKPO表发生异常:\t\n" + exception2);
                                return Result;
                            }
                            if (dtEKKO != null && dtEKKO.Rows.Count > 0)
                            {
                                strEKKPO.strAEDAT = (dtEKKO.Rows[0]["AEDAT"] ?? string.Empty).ToString();
                                strEKKPO.strLIFNR = (dtEKKO.Rows[0]["LIFNR"] ?? string.Empty).ToString();
                                strEKKPO.strNAME1 = (dtEKKO.Rows[0]["NAME1"] ?? string.Empty).ToString();
                            }
                            #endregion

                            #region 插入sql
                            strBuilder.Append(" INSERT INTO HB_WZ");
                            strBuilder.Append(" (WL_ID,WL_JHH,WL_JHLX,WL_JHLXWB,WL_GC,WL_GCMC,WL_XMH");
                            strBuilder.Append(",WL_WL,WL_WLMS,WL_WLZ,WL_WLZMS,WL_JHCJRQ,WL_CGDD,WL_CGDDXMH");
                            strBuilder.Append(",WL_GYS,WL_GYSMC,WL_CGSL,WL_JLDW,WL_CGDJ,WL_CGZJ,WL_CGDDRQ)");
                            strBuilder.Append(" VALUES(");
                            strBuilder.Append("SQ_WZCGSJ.NEXTVAL,");
                            strBuilder.Append("'" + strZC025.strRSNUM + "',");     //需求计划号
                            strBuilder.Append("'" + strZC025.strZJHLXO + "',");    //需求计划类型
                            strBuilder.Append("'" + strZC025.strZJHLX0T + "',");   //需求计划文本 
                            strBuilder.Append("'" + strZC025.strWERKS + "',");     //工厂 
                            strBuilder.Append("'" + strZC025.strWNAME + "',");    //工厂名称  
                            strBuilder.Append("'" + strZC025.strRSPOS + "',");     //需求项目号      
                            strBuilder.Append("'" + strZC026.strMATNR + "',");     //采购物料号
                            strBuilder.Append("'" + strZC026.strMAKTX + "',");     //采购物料描述
                            strBuilder.Append("'" + strZC026.strMATKL + "',");     //采购物料组
                            strBuilder.Append("'" + strZC026.strWGBEZ + "',");     //采购物料组描述
                            strBuilder.Append("'" + strZC025.strRSDAT + "',");     //需求计划提报日期 
                            strBuilder.Append("'" + strZC026.strZEBELN + "',");    //采购订单
                            strBuilder.Append("'" + strZC026.strZEBELP + "',");    //采购订单项目号
                            strBuilder.Append("'" + strEKKPO.strLIFNR + "',");    //供应商
                            strBuilder.Append("'" + strEKKPO.strNAME1 + "',");    //供应商名称
                            strBuilder.Append("'" + strEKKPO.strMENGE + "',");    //采购数量
                            strBuilder.Append("'" + strEKKPO.strMEINS + "',");    //采购数量
                            strBuilder.Append("'" + strEKKPO.strNETPR + "',");    //采购单价
                            strBuilder.Append("'" + strEKKPO.strNETWR + "',");    //采购总价
                            strBuilder.Append("'" + strEKKPO.strAEDAT + "'");    //采购订单创建日期
                            strBuilder.Append(");");

                            #endregion

                            #region 提交sql
                            commitcount++;
                            if (commitcount % commitMax == 0)
                            {
                                strBuilder.Append(" End;");  //SQL完成
                                try
                                {
                                    if (strBuilder.ToString().Length < 14)
                                    {
                                        strBuilder.Length = 0;
                                        strBuilder.Append(" Begin "); //开始执行SQL
                                        continue;
                                    }
                                    //数据提交
                                    Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
                                    strBuilder.Length = 0;
                                    strBuilder.Append(" Begin "); //开始执行SQL
                                }
                                catch (Exception exception5)
                                {
                                    Result = false;
                                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表发生异常:" + exception5);
                                }
                            }
                            #endregion
                    #endregion
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
            }
            catch (Exception exception5)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wz表发生异常:" + exception5);
            }

            return Result;
        }
    }
}
