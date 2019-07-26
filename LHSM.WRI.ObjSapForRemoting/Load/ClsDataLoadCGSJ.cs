using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadCGSJ : ISAPLoadInterface
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
        /// 表ZP10MMDG065_10结构体用于存取表数据
        /// </summary>
        private struct ZP065
        {
            public string strCPUDT;     //需求计划审批日期
            public string strCPUTM;     //需求计划审批时间
            public string strFRGGR;     //审批组
            public string strFRGSX;     //审批策略
            public string strFRGCO;     //审批代码
            public string strAPPTEXT;   //审批意见
            public string strBNAME;     //审批人
        }

        /// <summary>
        /// 表ZC10MMDG026和ZP10MMDG203_HZ结构体用于存取表数据
        /// </summary>
        private struct ZC026
        {
            public string strZBANFN;    //采购申请号
            public string strBNFPO;     //采购申请行项目号
            public string strAEDAT;     //采购申请生成日期
            public string strCPUTM;     //采购申请生成时间
            public string strZCGJHNUM;  //采购计划号
            public string strZCGJHPOS;  //采购计划项目
            public string strMATNR;     //采购物料号
            public string strMAKTX;     //采购物料描述
            public string strMATKL;     //采购物料组
            public string strWGBEZ;     //采购物料组描述
            public string strZEBELN;    //采购订单
            public string strZEBELP;    //采购订单项目号
            public string str31AEDAT;   //供货单位确认日期
            public string str31CPUTM;   //供货单位确认时间
            public string str31ZPKRQ;   //选择供货单位时间
        }

        /// <summary>
        /// 表ZP10MMIF041A和ZP10MMIF044B结构体用于存取表数据
        /// </summary>
        private struct ZP041A
        {
            public string strERDAT;       //采购传物采创建日期
            public string strERZET;       //采购传物采创建时间
            public string strRERDAT;      //采购传物采日期
            public string strRERZET;      //采购传物采时间
            public string strERDATB;      //物采平台返回日期
            public string strERZETB;      //物采平台返回时间
        }

        /// <summary>
        /// 表EKKO和EKPO结构体用于存取表数据
        /// </summary>
        private struct EKKPO
        {
            public string strBUKRS;       //公司代码
            public string strAEDAT;       //采购订单生成日期
            public string strUDATE;       //采购订单审批日期
            public string strUTIME;       //采购订单审批时间
            public string strCGEBLNR;     //采购发票号
            public string strCGCPUDT;     //发票校验输入日期
            public string strCGBUDAT;     //发票校验输入日期
        }

        /// <summary>
        /// 表ZC10MMDG022和ZC10MMDG022_D结构体用于存取表数据
        /// </summary>
        private struct ZC022
        {
            public string strTBSJ;        //提报时间
            public string strZJYPSTR;    //检验批
            public string strSQH;         //建造申请号
            public string strJZSJ;        //转建造时间
            public string strSQLX;        //申请类型
            public string strZT;          //申请状态
            public string strJZLXR;       //建造联系人
            public string strSPJB;        //建造审批级别
            public string strDATUM;       //建造审批日期
            public string strUZEIT;       //建造审批时间
            public string strSPYJ;        //建造审批意见
        }

        /// <summary>
        /// 表EKES结构体用于存取表数据
        /// </summary>
        private struct EKES
        {
            public string strERDAT;        //生成交货单日期
            public string strEZEIT;        //生成交货单时间
            public string strEINDT;        //交货日期
            public string strVBELN;        //交货单
            public string strVBELP;        //交货单行项目
            public string strCJRQ;         //外观检查创建日期
            public string strJYWCRQ;       //外观检查完成日期
            public string strTZDCJRQ;      //交接验收单创建日期
        }
        private struct EKBE
        {
            public string strBELNR;        //物料凭证
            public string strBUZEI;        //物料凭证行项目号
            public string strMJAHR;        //物料凭证年度
            public string strDJBUDAT;      //过账日期收货至冻结库存日期质检收货日期105   
            public string strDJCPUTM;      //过账时间收货至冻结库存时间质检收货时间105
            public string strBUDAT;        //过账日期物资入库日期 101
            public string strCPUTM;        //过账时间物资入库时间 101   
        }

        /// <summary>
        /// 表ZC10MMDG035结构体用于存取表数据
        /// </summary>
        private struct ZC035
        {
            public string strZCJDATE;        //报检单创建日期
            public string strZBJNO;          //报检单号
            public string strZBJITEM;        //报检单行项目
            public string strZCJNAME;        //报检单创建人
        }

        /// <summary>
        /// 表ZC10MMDG033结构体用于存取表数据
        /// </summary>
        private struct ZC033
        {
            public string strZFXZDATE;        //转非限制库存日期
            public string strZFXZMBLNR;       //非限制物料凭证
        }

        /// <summary>
        /// 表QALS结构体用于存取表数据
        /// </summary>
        private struct QALS
        {
            public string strERSTELDA;        //检验结果创建日期
            public string strERSTELZEIT;      //检验结果创建时间
            public string strPAENDTERM;       //检验结束日期
            public string strBUDAT;           //检验过账日期
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
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,a.ZJHLX1,a.ZJHLX1T,a.ZJHLX2,a.ZJHLX2T,a.BWART,");
                sbSqlZC025.Append(" to_number(b.RSPOS) as XMH,b.MATNR,'' MAKTX,'' MATKL,'' WGBEZ,'' ERDAT,'' ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,RESB b where a.RSNUM=b.RSNUM and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                sbSqlZC025.Append(" union all ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,a.ZJHLX1,a.ZJHLX1T,a.ZJHLX2,a.ZJHLX2T,a.BWART,");
                sbSqlZC025.Append(" to_number(b.POSNR) as XMH,b.MATNR,b.ARKTX MAKTX,'' MATKL,'' WGBEZ,b.ERDAT,b.ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,VBAP b where a.RSNUM=b.vbeln and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                sbSqlZC025.Append(" union ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,a.ZJHLX1,a.ZJHLX1T,a.ZJHLX2,a.ZJHLX2T,a.BWART,");
                sbSqlZC025.Append(" to_number(b.RSPOS)as XMH,b.MATNR,'' MAKTX,'' MATKL,'' WGBEZ,'' ERDAT,'' ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,ZC10MMDG026 b where a.RSNUM=b.RSNUM and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                sbSqlZC025.Append(" union ");
                sbSqlZC025.Append(" select");
                sbSqlZC025.Append(" a.RSNUM,a.RSDAT,a.CPUTM,a.ZJHLX0,a.ZJHLX0T,a.WERKS,a.ZJHLX1,a.ZJHLX1T,a.ZJHLX2,a.ZJHLX2T,a.BWART,");
                sbSqlZC025.Append(" to_number(b.RSPOS) as XMH,b.MATNR,'' MAKTX,'' MATKL,'' WGBEZ,'' ERDAT,'' ERZET");
                sbSqlZC025.Append(" from ZC10MMDG025 a ,ZP10MMDG203_HZ b where a.RSNUM=b.RSNUM and a.DLDATE='" + p_para.Sap_AEDAT + "'");
                dtZC025 = m_Conn.GetSqlResultToDt(sbSqlZC025.ToString());
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG025表发生异常:\t\n" + exception);
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
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG025B表发生异常:\t\n" + exception);
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
                drZC025["ZJHLX1"] = subRowZC025B["ZJHLX1"].ToString();    //需求计划类型1  
                drZC025["ZJHLX1T"] = string.Empty;  //需求计划文本1 
                drZC025["ZJHLX2"] = subRowZC025B["ZJHLX2"].ToString();   //需求计划类型2  
                drZC025["ZJHLX2T"] = string.Empty;   //需求计划文本2 
                drZC025["BWART"] = subRowZC025B["BWART"].ToString();     //移动类型        
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
                    strBuilder.Append(" DELETE FROM HB_WZCGSJ WHERE WL_JHH='" + subRowZC025["RSNUM"].ToString() + "';");
                    ls.Add(subRowZC025["RSNUM"].ToString());
                }

                strZC025.strRSNUM = subRowZC025["RSNUM"].ToString();     //需求计划号
                strZC025.strRSDAT = subRowZC025["RSDAT"].ToString().Replace(".", "");     //需求计划提报日期
                strZC025.strCPUTM = subRowZC025["CPUTM"].ToString();     //需求计划提报时间
                strZC025.strZJHLXO = subRowZC025["ZJHLX0"].ToString();    //需求计划类型
                strZC025.strZJHLX0T = subRowZC025["ZJHLX0T"].ToString();   //需求计划文本 
                strZC025.strWERKS = subRowZC025["WERKS"].ToString();     //工厂
                strZC025.strZJHLX1 = subRowZC025["ZJHLX1"].ToString();    //需求计划类型1
                strZC025.strZJHLX1T = subRowZC025["ZJHLX1T"].ToString();   //需求计划文本1 
                strZC025.strZJHLX2 = subRowZC025["ZJHLX2"].ToString();    //需求计划类型2
                strZC025.strZJHLX2T = subRowZC025["ZJHLX2T"].ToString();   //需求计划文本2
                strZC025.strBWART = subRowZC025["BWART"].ToString();     //移动类型
                strZC025.strRSPOS = subRowZC025["XMH"].ToString();     //需求项目号
                strZC025.strMATNR = subRowZC025["MATNR"].ToString();     //物料号
                strZC025.strARKTX = subRowZC025["MAKTX"].ToString();     //物料描述
                strZC025.strMATKL = subRowZC025["MATKL"].ToString();     //物料组
                strZC025.strWGBEZ = subRowZC025["WGBEZ"].ToString();     //物料组描述
                strZC025.strERDAT = subRowZC025["ERDAT"].ToString();     //创建日期
                strZC025.strERZET = subRowZC025["ERZET"].ToString();     //创建时间

                #region 审批信息

                if (string.IsNullOrEmpty(strZC025.strRSPOS))
                {
                    continue;
                }
                DataTable dtZP065 = new DataTable();
                try
                {
                    string strSqlZP065 = "select CPUDT,CPUTM,FRGGR,FRGSX,FRGCO,APPTEXT,BNAME from ZP10MMDG065_10";
                    strSqlZP065 += " where KEY1='" + strZC025.strRSNUM + "' and TO_NUMBER(KEY2)=" + Convert.ToInt16(strZC025.strRSPOS);
                    //取每个KEY2中状态是已审批（05）ZAEHK的最大值
                    strSqlZP065 += " and ZAEHK in (select max(ZAEHK) from ZP10MMDG065_10 where KEY1='" + strZC025.strRSNUM + "' and TO_NUMBER(KEY2)=" + Convert.ToInt16(strZC025.strRSPOS) + " and PROCSTAT='05')";
                    dtZP065 = m_Conn.GetSqlResultToDt(strSqlZP065);
                }
                catch (Exception exception1)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZP10MMDG065_10表发生异常:\t\n" + exception1);
                    return Result;
                }

                ZP065 strZP065 = new ZP065();
                if (dtZP065 != null && dtZP065.Rows.Count > 0)
                {
                    strZP065.strCPUDT = (dtZP065.Rows[0]["CPUDT"] ?? string.Empty).ToString();     //需求计划审批日期
                    strZP065.strCPUTM = (dtZP065.Rows[0]["CPUTM"] ?? string.Empty).ToString(); ;     //需求计划审批时间
                    strZP065.strFRGGR = (dtZP065.Rows[0]["FRGGR"] ?? string.Empty).ToString(); ;     //审批组
                    strZP065.strFRGSX = (dtZP065.Rows[0]["FRGSX"] ?? string.Empty).ToString(); ;     //审批策略
                    strZP065.strFRGCO = (dtZP065.Rows[0]["FRGCO"] ?? string.Empty).ToString(); ;     //审批代码 
                    strZP065.strAPPTEXT = (dtZP065.Rows[0]["APPTEXT"] ?? string.Empty).ToString(); ;   //审批意见
                    strZP065.strBNAME = (dtZP065.Rows[0]["BNAME"] ?? string.Empty).ToString(); ;     //审批人 
                }

                #endregion

                #region 采购

                DataTable dtZC026 = new DataTable();
                try
                {
                    string strSqlZC026 = "select ZCGJHNUM,ZCGJHPOS,AEDAT,CPUTM, MATNR,MAKTX,MATKL,";
                    strSqlZC026 += " WGBEZ,ZEBELN,ZEBELP, ";
                    strSqlZC026 += " '' as  AEDAT31,'' as  CPUTM31,'' as   ZPKRQ31";
                    strSqlZC026 += " from ZC10MMDG026";
                    strSqlZC026 += " where  RSNUM='" + strZC025.strRSNUM + "' and TO_NUMBER(RSPOS)=" + Convert.ToInt16(strZC025.strRSPOS).ToString();

                    dtZC026 = m_Conn.GetSqlResultToDt(strSqlZC026);

                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG026表发生异常:\t\n" + exception2);
                    return Result;
                }


                DataTable dtZC031 = new DataTable();
                try
                {
                    string strSqlZC031 = "select a.ZCGJHNUM,a.ZCGJHPOS,a.AEDAT,a.CPUTM, a.MATNR,a.MAKTX,a.MATKL,";
                    strSqlZC031 += " a.WGBEZ,a.ZEBELN,a.ZEBELP, ";
                    strSqlZC031 += " b.AEDAT AEDAT31,b.CPUTM CPUTM31,b.ZPKRQ ZPKRQ31";
                    strSqlZC031 += " from ZC10MMDG026 a , ZC10MMDG031 b ";
                    strSqlZC031 += " where a.ZCGJHNUM=b.ZCGJHNUM and a.ZCGJHPOS=b.ZCGJHPOS";
                    strSqlZC031 += " and b.ZSCBJ is null";
                    strSqlZC031 += " and a.RSNUM='" + strZC025.strRSNUM + "' and TO_NUMBER(a.RSPOS)=" + Convert.ToInt16(strZC025.strRSPOS);

                    dtZC031 = m_Conn.GetSqlResultToDt(strSqlZC031);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG026表发生异常:\t\n" + exception2);
                    return Result;
                }








                //二级单位
                DataTable dtZP203_HZ = new DataTable();
                try
                {
                    string strSqlZP203_HZ = "select BANFN,BNFPO,AEDAT,CPUTM,MATNR,MATKL,EBELN,EBELP";
                    strSqlZP203_HZ += " from ZP10MMDG203_HZ";
                    strSqlZP203_HZ += " where RSNUM='" + strZC025.strRSNUM + "' and TO_NUMBER(RSPOS)=" + Convert.ToInt16(strZC025.strRSPOS);
                    dtZP203_HZ = m_Conn.GetSqlResultToDt(strSqlZP203_HZ);
                }
                catch (Exception exception2)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZP10MMDG203_HZ表发生异常:\t\n" + exception2);
                    return Result;
                }

                //二级单位和公司级合并数据
                if (dtZP203_HZ != null && dtZP203_HZ.Rows.Count > 0)
                {
                    DataRow drZC026 = null;
                    foreach (DataRow subRowZP203 in dtZP203_HZ.Rows)
                    {
                        drZC026 = dtZC026.NewRow();

                        drZC026["ZCGJHNUM"] = subRowZP203["BANFN"].ToString();    //采购申请号
                        drZC026["ZCGJHPOS"] = subRowZP203["BNFPO"].ToString();     //采购申请行项目号
                        drZC026["AEDAT"] = subRowZP203["AEDAT"].ToString();     //采购申请生成日期
                        drZC026["CPUTM"] = subRowZP203["CPUTM"].ToString();      //采购申请生成时间
                        //drZC026["ZCGJHNUM"] =string.Empty;  //采购计划号
                        //drZC026["ZCGJHPOS"] =string.Empty;  //采购计划项目
                        drZC026["MATNR"] = subRowZP203["MATNR"].ToString();   //采购物料号
                        drZC026["MAKTX"] = string.Empty;   //采购物料描述
                        drZC026["MATKL"] = subRowZP203["MATKL"].ToString();   //采购物料组
                        drZC026["WGBEZ"] = string.Empty;  //采购物料组描述
                        drZC026["ZEBELN"] = subRowZP203["EBELN"].ToString(); //采购订单
                        drZC026["ZEBELP"] = subRowZP203["EBELP"].ToString();   //采购订单项目号
                        drZC026["AEDAT31"] = string.Empty; //供货单位确认日期
                        drZC026["CPUTM31"] = string.Empty;  //供货单位确认时间
                        drZC026["ZPKRQ31"] = string.Empty;  //选择供货单位时间

                        dtZC026.Rows.Add(drZC026);

                    }
                }

                #endregion

                ZC026 strZC026 = new ZC026();
                foreach (DataRow subRowZC026 in dtZC026.Rows)
                {
                    strZC026.strZCGJHNUM = subRowZC026["ZCGJHNUM"].ToString();   //采购计划号
                    strZC026.strZCGJHPOS = subRowZC026["ZCGJHPOS"].ToString();   //采购计划项目
                    strZC026.strAEDAT = subRowZC026["AEDAT"].ToString();     //采购申请生成日期
                    strZC026.strCPUTM = subRowZC026["CPUTM"].ToString();     //采购申请生成时间
                    //strZC026.strZCGJHNUM = subRowZC026["ZCGJHNUM"].ToString();     //采购计划号 
                    //strZC026.strZCGJHPOS = subRowZC026["ZCGJHPOS"].ToString();   //采购计划项目
                    strZC026.strMATNR = subRowZC026["MATNR"].ToString();     //采购物料号
                    strZC026.strMAKTX = subRowZC026["MAKTX"].ToString();     //采购物料描述
                    strZC026.strMATKL = subRowZC026["MATKL"].ToString();     //采购物料组
                    strZC026.strWGBEZ = subRowZC026["WGBEZ"].ToString();     //采购物料组描述
                    strZC026.strZEBELN = subRowZC026["ZEBELN"].ToString();     //采购订单
                    strZC026.strZEBELP = subRowZC026["ZEBELP"].ToString();     //采购订单项目号
                    strZC026.str31AEDAT = subRowZC026["AEDAT31"].ToString();     //供货单位确认日期
                    strZC026.str31CPUTM = subRowZC026["CPUTM31"].ToString();     //供货单位确认时间
                    strZC026.str31ZPKRQ = subRowZC026["ZPKRQ31"].ToString();     //选择供货单位时间

                    try
                    {
                        var v = dtZC031.Select("ZCGJHNUM='" + strZC026.strZCGJHNUM + "' and ZCGJHPOS='" + strZC026.strZCGJHPOS + "'");
                        if (v.Length != 0)
                        {
                            try
                            {
                                strZC026.str31AEDAT = v[0]["AEDAT31"].ToString();     //供货单位确认日期
                            }
                            catch
                            { }
                            try
                            {
                                strZC026.str31CPUTM = v[0]["CPUTM31"].ToString();
                            }
                            catch
                            { }//供货单位确认时间
                            try
                            {
                                strZC026.str31ZPKRQ = v[0]["ZPKRQ31"].ToString();
                            }
                            catch
                            { }//选择供货单位时间
                        }
                    }
                    catch
                    {

                    }

                    #region 查询ZP10MMIF041A和ZP10MMIF044B

                    DataTable dtZP041A = new DataTable();
                    ZP041A strZP041A = new ZP041A();
                    try
                    {
                        string strSqlZP041A = "select ERDAT,ERZET,RERDAT,RERZET";
                        strSqlZP041A += " from ZP10MMIF041A ";
                        strSqlZP041A += " where KEY1='" + strZC026.strZCGJHNUM + "' and TO_NUMBER(KEY2)=" + Convert.ToInt16(strZC026.strZCGJHPOS);
                        dtZP041A = m_Conn.GetSqlResultToDt(strSqlZP041A);
                    }
                    catch (Exception exception2)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZP10MMIF041A表发生异常:\t\n" + exception2);
                        return Result;
                    }

                    if (dtZP041A != null && dtZP041A.Rows.Count > 0)
                    {
                        strZP041A.strERDAT = (dtZP041A.Rows[0]["ERDAT"] ?? string.Empty).ToString();       //采购传物采创建日期
                        strZP041A.strERZET = (dtZP041A.Rows[0]["ERZET"] ?? string.Empty).ToString();       //采购传物采创建时间
                        strZP041A.strRERDAT = (dtZP041A.Rows[0]["RERDAT"] ?? string.Empty).ToString();      //采购传物采日期
                        strZP041A.strRERZET = (dtZP041A.Rows[0]["RERZET"] ?? string.Empty).ToString();      //采购传物采时间

                        DataTable dtZP044B = new DataTable();
                        try
                        {
                            string strSqlZP044B = "select ERDAT,ERZET from ZP10MMIF044B";
                            strSqlZP044B += " where KEY1='" + strZC026.strZCGJHNUM + "' and TO_NUMBER(KEY2)='" + Convert.ToInt16(strZC026.strZCGJHPOS) + "' and STATUS='S'";
                            dtZP044B = m_Conn.GetSqlResultToDt(strSqlZP044B);
                        }
                        catch (Exception exception2)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZP10MMIF044B表发生异常:\t\n" + exception2);
                            return Result;
                        }

                        if (dtZP044B != null && dtZP044B.Rows.Count > 0)
                        {
                            strZP041A.strERDATB = (dtZP044B.Rows[0]["ERDAT"] ?? string.Empty).ToString();      //物采平台返回日期 
                            strZP041A.strERZETB = (dtZP044B.Rows[0]["ERZET"] ?? string.Empty).ToString();      //物采平台返回时间
                        }
                        else
                        {
                            continue;
                        }
                    }
                    #endregion

                    DataTable dtEKPO = new DataTable();
                    if (strZC026.strZEBELN != null && strZC026.strZEBELN != "")
                    {
                        try
                        {
                            string strSqlEKPO = "select EBELN,EBELP,BUKRS,AEDAT from EKPO";
                            strSqlEKPO += " where EBELN='" + strZC026.strZEBELN + "' and EBELP ='" + strZC026.strZEBELP + "'";
                            strSqlEKPO += " and (LOEKZ<>'L' or LOEKZ IS NULL)";

                            dtEKPO = m_Conn.GetSqlResultToDt(strSqlEKPO);
                        }
                        catch (Exception exception2)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询EKPO表发生异常:\t\n" + exception2);
                            return Result;
                        }

                    }
                    else
                    {
                        try
                        {
                            string strSqlEKPO = "select EBELN,EBELP,BUKRS,AEDAT from EKPO";
                            strSqlEKPO += " where BANFN='" + strZC026.strZCGJHNUM + "' and TO_NUMBER(BNFPO)=" + Convert.ToInt16(strZC026.strZCGJHPOS);
                            strSqlEKPO += " and (LOEKZ<>'L' or LOEKZ IS NULL)";

                            dtEKPO = m_Conn.GetSqlResultToDt(strSqlEKPO);
                        }
                        catch (Exception exception2)
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询EKPO表发生异常:\t\n" + exception2);

                        }
                    }



                    #region 采购订单生成


                    if (dtEKPO != null && dtEKPO.Rows.Count > 0)
                    {
                        foreach (DataRow subRowEKPO in dtEKPO.Rows)
                        {
                            #region EKKO数据查询
                            EKKPO strEKKPO = new EKKPO();
                            strEKKPO.strBUKRS = subRowEKPO["BUKRS"].ToString();
                            strEKKPO.strAEDAT = subRowEKPO["AEDAT"].ToString();

                            DataTable dtEKKO = new DataTable();
                            try
                            {
                                //string strSqlEKKO ="select ZHTDAT from EKKO";
                                //strSqlEKKO +=" where EBELN='"+ subRowEKPO["EBELN"].ToString() +"'";//ERP2.0以后的取数方式

                                string strSqlEKKO = "select a.UDATE,a.UTIME from CDHDR a, EKKO b ";//ERP1.0取数方式
                                strSqlEKKO += " where a.objectid='" + subRowEKPO["EBELN"].ToString() + "'";
                                strSqlEKKO += " AND b.EBELN='" + subRowEKPO["EBELN"].ToString() + "'";
                                strSqlEKKO += " AND b.FRGGR='F1' and b.FRGSX='01' and b.FRGKE='1'   and (a.TCODE='ME28'or a.TCODE='ME29N')";
                                dtEKKO = m_Conn.GetSqlResultToDt(strSqlEKKO);
                            }
                            catch (Exception exception2)
                            {
                                Result = false;
                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询EKPO表发生异常:\t\n" + exception2);
                                return Result;
                            }
                            if (dtEKKO != null && dtEKKO.Rows.Count > 0)
                            {
                                strEKKPO.strUDATE = (dtEKKO.Rows[0]["UDATE"] ?? string.Empty).ToString();
                                strEKKPO.strUTIME = (dtEKKO.Rows[0]["UTIME"] ?? string.Empty).ToString();
                            }
                            #endregion


                            #region ZC10MMDG022和ZC10MMDG022_D数据查询
                            ZC022 strZC022 = new ZC022();

                            DataTable dtZC022 = new DataTable();
                            try
                            {
                                string strSqlZC022 = "select TBSJ,ZJYPSTR,SQH,JZSJ,SQLX,ZT,JZLXR from ZC10MMDG022";
                                strSqlZC022 += " where EBELN='" + subRowEKPO["EBELN"].ToString() + "'";
                                dtZC022 = m_Conn.GetSqlResultToDt(strSqlZC022);
                            }
                            catch (Exception exception2)
                            {
                                Result = false;
                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG022表发生异常:\t\n" + exception2);
                                return Result;
                            }
                            if (dtZC022 != null && dtZC022.Rows.Count > 0)
                            {
                                strZC022.strTBSJ = (dtZC022.Rows[0]["TBSJ"] ?? string.Empty).ToString();        //提报时间
                                strZC022.strZJYPSTR = (dtZC022.Rows[0]["ZJYPSTR"] ?? string.Empty).ToString();    //检验批
                                strZC022.strSQH = (dtZC022.Rows[0]["SQH"] ?? string.Empty).ToString();         //建造申请号    
                                strZC022.strJZSJ = (dtZC022.Rows[0]["JZSJ"] ?? string.Empty).ToString();        //转建造时间
                                strZC022.strSQLX = (dtZC022.Rows[0]["SQLX"] ?? string.Empty).ToString();        //申请类型
                                strZC022.strZT = (dtZC022.Rows[0]["ZT"] ?? string.Empty).ToString();          //申请状态    
                                strZC022.strJZLXR = (dtZC022.Rows[0]["JZLXR"] ?? string.Empty).ToString();       //建造联系人
                                DataTable dtZC022D = new DataTable();
                                try
                                {
                                    string strSqlZC022D = "select SPJB,DATUM,UZEIT,SPYJ  from ZC10MMDG022_D";
                                    strSqlZC022D += " where SQH='" + dtZC022.Rows[0]["SQH"].ToString() + "'";
                                    dtZC022D = m_Conn.GetSqlResultToDt(strSqlZC022D);
                                }
                                catch (Exception exception2)
                                {
                                    Result = false;
                                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG022_D表发生异常:\t\n" + exception2);
                                    return Result;
                                }
                                if (dtZC022D != null && dtZC022D.Rows.Count > 0)
                                {
                                    strZC022.strSPJB = (dtZC022D.Rows[0]["SPJB"] ?? string.Empty).ToString();        //建造审批级别
                                    strZC022.strDATUM = (dtZC022D.Rows[0]["DATUM"] ?? string.Empty).ToString();       //建造审批日期    
                                    strZC022.strUZEIT = (dtZC022D.Rows[0]["UZEIT"] ?? string.Empty).ToString();       //建造审批时间
                                    strZC022.strSPYJ = (dtZC022D.Rows[0]["SPYJ"] ?? string.Empty).ToString();        //建造审批意见
                                }
                            }
                            #endregion

                            #region EKES数据查询
                            EKES strEKES = new EKES();

                            DataTable dtEKES = new DataTable();
                            try
                            {
                                string strSqlEKES = "select ERDAT,EZEIT,EINDT,VBELN,VBELP from EKES";
                                strSqlEKES += " where EBELN='" + subRowEKPO["EBELN"].ToString() + "' AND EBELP='" + subRowEKPO["EBELP"].ToString() + "'";
                                dtEKES = m_Conn.GetSqlResultToDt(strSqlEKES);
                            }
                            catch (Exception exception2)
                            {
                                Result = false;
                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询EKES表发生异常:\t\n" + exception2);
                                return Result;
                            }
                            if (dtEKES != null && dtEKES.Rows.Count > 0)
                            {
                                foreach (DataRow subRowEKES in dtEKES.Rows)
                                {
                                    strEKES.strERDAT = (subRowEKES["ERDAT"] ?? string.Empty).ToString();        //生成交货单日期                  
                                    strEKES.strEZEIT = (subRowEKES["EZEIT"] ?? string.Empty).ToString();        //生成交货单时间      
                                    strEKES.strEINDT = (subRowEKES["EINDT"] ?? string.Empty).ToString();        //交货日期            
                                    strEKES.strVBELN = (subRowEKES["VBELN"] ?? string.Empty).ToString();        //交货单              
                                    strEKES.strVBELP = (subRowEKES["VBELP"] ?? string.Empty).ToString();        //交货单行项目        

                                    #region ZC10MMDG023数据查询
                                    DataTable dtZC023 = new DataTable();
                                    try
                                    {
                                        string strSqlZC023 = "select CJRQ,JYWCRQ,TZDCJRQ from ZC10MMDG023";
                                        strSqlZC023 += " where VBELN='" + strEKES.strVBELN + "'";
                                        dtZC023 = m_Conn.GetSqlResultToDt(strSqlZC023);
                                    }
                                    catch (Exception exception2)
                                    {
                                        Result = false;
                                        ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG023表发生异常:\t\n" + exception2);
                                        return Result;
                                    }

                                    if (dtZC023 != null && dtZC023.Rows.Count > 0)
                                    {
                                        strEKES.strCJRQ = (dtZC023.Rows[0]["CJRQ"] ?? string.Empty).ToString();         //外观检查创建日期    
                                        strEKES.strJYWCRQ = (dtZC023.Rows[0]["JYWCRQ"] ?? string.Empty).ToString();       //外观检查完成日期    
                                        strEKES.strTZDCJRQ = (dtZC023.Rows[0]["TZDCJRQ"] ?? string.Empty).ToString();      //交接验收单创建日期  
                                    }
                                    #endregion
                                }
                            }
                            //else
                            //{
                            //    continue;
                            //}
                            #endregion

                            #region EKBE数据查询

                            #region 51开头凭证编号
                            DataTable dtEKBE = new DataTable();
                            string strSqlEKBE = string.Empty;
                            try
                            {
                                strSqlEKBE = "select BELNR,CPUDT,BUDAT,shkzg from EKBE t";
                                strSqlEKBE += " where t.EBELN='" + subRowEKPO["EBELN"].ToString() + "'";
                                strSqlEKBE += " and t.EBELP='" + subRowEKPO["EBELP"].ToString() + "' ";
                                strSqlEKBE += " and t.BELNR like '23%'order by t.cpudt ,t.cputm desc";
                                dtEKBE = m_Conn.GetSqlResultToDt(strSqlEKBE);
                            }
                            catch (Exception exception2)
                            {
                                Result = false;
                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询EKBE表发生异常:\t\n" + exception2);
                                return Result;
                            }
                            if (dtEKBE != null && dtEKBE.Rows.Count > 0 && dtEKBE.Rows[0]["shkzg"].ToString().ToUpper() == "S")
                            {
                                strEKKPO.strCGEBLNR = (dtEKBE.Rows[0]["BELNR"] ?? string.Empty).ToString();
                                strEKKPO.strCGCPUDT = (dtEKBE.Rows[0]["CPUDT"] ?? string.Empty).ToString();
                                strEKKPO.strCGBUDAT = (dtEKBE.Rows[0]["BUDAT"] ?? string.Empty).ToString();
                            }
                            //else
                            //{
                            //    continue;
                            //}

                            #endregion

                            #region BWART=101为正常入库,输出收货凭证; BWART=105为收货至质检库存。

                            dtEKBE = null;
                            strSqlEKBE = "select BELNR,BUDAT,CPUTM,BUZEI,BWART from EKBE";
                            strSqlEKBE += " where EBELN='" + subRowEKPO["EBELN"].ToString() + "'";
                            strSqlEKBE += " and EBELP='" + subRowEKPO["EBELP"].ToString() + "' ";
                            strSqlEKBE += " and BELNR like '12%'order by cpudt, cputm desc";
                            try
                            {
                                dtEKBE = m_Conn.GetSqlResultToDt(strSqlEKBE);
                            }
                            catch (Exception exception2)
                            {
                                Result = false;
                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询MSEG表发生异常:\t\n" + exception2);
                                return Result;
                            }
                            EKBE strEKBE = new EKBE();

                            ZC035 strZC035 = new ZC035();
                            QALS strQALS = new QALS();
                            ZC033 strZC033 = new ZC033();
                            if (dtEKBE != null && dtEKBE.Rows.Count > 0 && (dtEKBE.Rows[0]["BWART"].ToString() == "105" || dtEKBE.Rows[0]["BWART"].ToString() == "101"))
                            {

                                foreach (DataRow subRowEKBE in dtEKBE.Rows)
                                {
                                    strEKBE.strBELNR = (subRowEKBE["BELNR"] ?? string.Empty).ToString();        //物料凭证
                                    strEKBE.strBUZEI = (subRowEKBE["BUZEI"] ?? string.Empty).ToString();        //物料凭证行项目号
                                    if (subRowEKBE["BWART"].ToString() == "105")
                                    {

                                        strEKBE.strDJBUDAT = (subRowEKBE["CPUDT"] ?? string.Empty).ToString();        //过账日期    
                                        strEKBE.strDJCPUTM = (subRowEKBE["CPUTM"] ?? string.Empty).ToString();        //过账时间     
                                    }
                                    else if (subRowEKBE["BWART"].ToString() == "101")
                                    {
                                        strEKBE.strBUDAT = (subRowEKBE["BUDAT"] ?? string.Empty).ToString();        //过账日期    
                                        strEKBE.strCPUTM = (subRowEKBE["CPUTM"] ?? string.Empty).ToString();        //过账时间     
                                    }

                                    //ZC035 strZC035 = new ZC035();
                                    //QALS strQALS = new QALS();
                                    //ZC033 strZC033 = new ZC033();

                                    string strBELNR = (subRowEKBE["BELNR"] ?? string.Empty).ToString();
                                    if (!string.IsNullOrEmpty(strBELNR))
                                    {
                                        #region ZC10MMDG035数据查询

                                        if (subRowEKBE["BWART"].ToString() == "105")
                                        {
                                            DataTable dtZC035 = new DataTable();
                                            try
                                            {
                                                string strSqlZC035 = "select PRUEFLOS,ZCJDATE,ZBJNO,ZBJITEM,ZCJNAME from ZC10MMDG035";
                                                strSqlZC035 += " where MBLNR='" + strBELNR + "' and ZEILE='" + strEKBE.strBUZEI + "'";
                                                dtZC035 = m_Conn.GetSqlResultToDt(strSqlZC035);
                                            }
                                            catch (Exception exception2)
                                            {
                                                Result = false;
                                                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG035表发生异常:\t\n" + exception2);
                                                return Result;
                                            }

                                            if (dtZC035 != null && dtZC035.Rows.Count > 0)
                                            {
                                                strZC035.strZCJDATE = dtZC035.Rows[0]["ZCJDATE"].ToString();        //报检单创建日期
                                                strZC035.strZBJNO = dtZC035.Rows[0]["ZBJNO"].ToString();          //报检单号
                                                strZC035.strZBJITEM = dtZC035.Rows[0]["ZBJITEM"].ToString();        //报检单行项目   
                                                strZC035.strZCJNAME = dtZC035.Rows[0]["ZCJNAME"].ToString();        //报检单创建人

                                                string str_PRUEFLOS = (dtZC035.Rows[0]["PRUEFLOS"] ?? string.Empty).ToString();
                                                if (!string.IsNullOrEmpty(str_PRUEFLOS))
                                                {
                                                    #region QALS数据查询

                                                    DataTable dtQALS = new DataTable();
                                                    try
                                                    {
                                                        string strSqlQALS = "select ERSTELDA,ERSTELZEIT,PAENDTERM,BUDAT from QALS";
                                                        strSqlQALS += " where PRUEFLOS='" + dtZC035.Rows[0]["PRUEFLOS"].ToString() + "'";
                                                        dtQALS = m_Conn.GetSqlResultToDt(strSqlQALS);
                                                    }
                                                    catch (Exception exception2)
                                                    {
                                                        Result = false;
                                                        ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询QALS表发生异常:\t\n" + exception2);
                                                        return Result;
                                                    }

                                                    if (dtQALS != null && dtQALS.Rows.Count > 0)
                                                    {
                                                        strQALS.strERSTELDA = (dtQALS.Rows[0]["ERSTELDA"] ?? string.Empty).ToString();       //检验结果创建日期
                                                        strQALS.strERSTELZEIT = (dtQALS.Rows[0]["ERSTELZEIT"] ?? string.Empty).ToString();      //检验结果创建时间
                                                        strQALS.strPAENDTERM = (dtQALS.Rows[0]["PAENDTERM"] ?? string.Empty).ToString();       //检验结束日期   
                                                        strQALS.strBUDAT = (dtQALS.Rows[0]["BUDAT"] ?? string.Empty).ToString();           //检验过账日期                                                         
                                                    }
                                                    #endregion

                                                    #region ZC10MMDG033数据查询

                                                    DataTable dtZC033 = new DataTable();
                                                    try
                                                    {
                                                        string strSqlZC033 = "select ZFXZDATE,ZFXZMBLNR from ZC10MMDG033";
                                                        strSqlZC033 += " where MBLNR='" + strBELNR + "' and ZEILE='" + strEKBE.strBUZEI + "'";
                                                        dtZC033 = m_Conn.GetSqlResultToDt(strSqlZC033);
                                                    }
                                                    catch (Exception exception2)
                                                    {
                                                        Result = false;
                                                        ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表过程中查询ZC10MMDG033表发生异常:\t\n" + exception2);
                                                        return Result;
                                                    }

                                                    if (dtZC033 != null && dtZC033.Rows.Count > 0)
                                                    {
                                                        strZC033.strZFXZDATE = (dtZC033.Rows[0]["ZFXZDATE"] ?? string.Empty).ToString();       //转非限制库存日期
                                                        strZC033.strZFXZMBLNR = (dtZC033.Rows[0]["ZFXZMBLNR"] ?? string.Empty).ToString();     //非限制物料凭证 
                                                    }
                                                    #endregion
                                                }
                                            }
                                        }
                                        #endregion
                                    }

                                }
                            }
                            #region 插入sql
                            strBuilder.Append(" INSERT INTO HB_WZCGSJ");
                            strBuilder.Append(" (WL_ID,WL_JHH,WL_JHTBRQ,WL_JHTBSJ,WL_LX,WL_WB,WL_GC,WL_LX1,WL_WB1,WL_LX2,WL_WB2");
                            strBuilder.Append(",WL_YDLX,WL_XMH,WL_WL,WL_WLMS,WL_WLZ,WL_WLZMS,WL_CJRQ,WL_CJSJ");
                            strBuilder.Append(",WL_SPRQ,WL_SPSJ,WL_SPZ,WL_SPCL,WL_SPDM,WL_SPYJ,WL_SPR");
                            strBuilder.Append(",WL_CGSQH,WL_CGHXMH,WL_CGSCRQ,WL_CGSCSJ");
                            strBuilder.Append(",WL_CGWL,WL_CGWLMS,WL_CGWLZ,WL_CGWLZMS,WL_CGDD,WL_CGDDXMH");
                            strBuilder.Append(",WL_GHQRRQ,WL_GHQRSJ,WL_GHSJ,WL_WCCJRQ,WL_WCCJSJ,WL_WCRQ");
                            strBuilder.Append(",WL_WCSJ,WL_WCFHRQ,WL_WCFHSJ,WL_GSBM ,WL_CGDDSCSJ,WL_CGDDSPSJ");
                            strBuilder.Append(",WL_TBSJ,WL_JYP,WL_JZSQH,WL_ZJZSJ,WL_SQLX,WL_SQZT");
                            strBuilder.Append(",WL_JZLXR,WL_JZSPJB,WL_JZSPRQ,WL_JZSPSJ,WL_JZSPYJ");
                            strBuilder.Append(",WL_JHDRQ,WL_JHDSJ,WL_JHRQ,WL_JHD,WL_JHDHXM,WL_WGCJRQ,WL_WGWCRQ");
                            strBuilder.Append(",WL_YSDCJRQ,WL_WLPZ,WL_WLPZXMH,WL_WLPZN,WL_SHRQ,WL_SHSJ,WL_GZRQ,WL_GZSJ");
                            strBuilder.Append(",WL_JYDCJRQ,WL_JYDH,WL_JYDHXM,WL_JYDCJR,WL_JYJGCJRQ,WL_JYJGCJSJ");
                            strBuilder.Append(",WL_JYJSRQ,WL_JYJGGZRQ,WL_XZKCRQ,WL_FXZWLPZ,WL_CGFPH,WL_CGSRRQ,WL_CGGZRQ)");
                            strBuilder.Append(" VALUES(");
                            strBuilder.Append("SQ_WZCGSJ.NEXTVAL,");
                            strBuilder.Append("'" + strZC025.strRSNUM + "',");     //需求计划号
                            strBuilder.Append("'" + strZC025.strRSDAT + "',");     //需求计划提报日期
                            strBuilder.Append("'" + strZC025.strCPUTM + "',");     //需求计划提报时间
                            strBuilder.Append("'" + strZC025.strZJHLXO + "',");    //需求计划类型
                            strBuilder.Append("'" + strZC025.strZJHLX0T + "',");   //需求计划文本 
                            strBuilder.Append("'" + strZC025.strWERKS + "',");     //工厂
                            strBuilder.Append("'" + strZC025.strZJHLX1 + "',");    //需求计划类型1
                            strBuilder.Append("'" + strZC025.strZJHLX1T + "',");   //需求计划文本1 
                            strBuilder.Append("'" + strZC025.strZJHLX2 + "',");    //需求计划类型2
                            strBuilder.Append("'" + strZC025.strZJHLX2T + "',");   //需求计划文本2
                            strBuilder.Append("'" + strZC025.strBWART + "',");     //移动类型
                            strBuilder.Append("'" + strZC025.strRSPOS + "',");     //需求项目号
                            strBuilder.Append("'" + strZC025.strMATNR + "',");     //物料号
                            strBuilder.Append("'" + strZC025.strARKTX + "',");     //物料描述
                            strBuilder.Append("'" + strZC025.strMATKL + "',");     //物料组
                            strBuilder.Append("'" + strZC025.strWGBEZ + "',");     //物料组描述
                            strBuilder.Append("'" + strZC025.strERDAT + "',");     //创建日期
                            strBuilder.Append("'" + strZC025.strERZET + "',");     //创建时间

                            strBuilder.Append("'" + strZP065.strCPUDT + "',");     //需求计划审批日期
                            strBuilder.Append("'" + strZP065.strCPUTM + "',");     //需求计划审批时间
                            strBuilder.Append("'" + strZP065.strFRGGR + "',");     //审批组
                            strBuilder.Append("'" + strZP065.strFRGSX + "',");     //审批策略
                            strBuilder.Append("'" + strZP065.strFRGCO + "',");     //审批代码 
                            strBuilder.Append("'" + strZP065.strAPPTEXT + "',");   //审批意见
                            strBuilder.Append("'" + strZP065.strBNAME + "',");     //审批人

                            strBuilder.Append("'" + strZC026.strZCGJHNUM + "',");    //采购申请号
                            strBuilder.Append("'" + strZC026.strZCGJHPOS + "',");    //采购申请行项目号
                            strBuilder.Append("'" + strZC026.strAEDAT + "',");     //采购申请生成日期
                            strBuilder.Append("'" + strZC026.strCPUTM + "',");     //采购申请生成时间

                            strBuilder.Append("'" + strZC026.strMATNR + "',");     //采购物料号
                            strBuilder.Append("'" + strZC026.strMAKTX + "',");     //采购物料描述
                            strBuilder.Append("'" + strZC026.strMATKL + "',");     //采购物料组
                            strBuilder.Append("'" + strZC026.strWGBEZ + "',");     //采购物料组描述
                            strBuilder.Append("'" + strZC026.strZEBELN + "',");    //采购订单
                            strBuilder.Append("'" + strZC026.strZEBELP + "',");    //采购订单项目号
                            strBuilder.Append("'" + strZC026.str31AEDAT + "',");   //供货单位确认日期
                            strBuilder.Append("'" + strZC026.str31CPUTM + "',");   //供货单位确认时间
                            strBuilder.Append("'" + strZC026.str31ZPKRQ + "',");   //选择供货单位时间

                            strBuilder.Append("'" + strZP041A.strERDAT + "',");       //采购传物采创建日期
                            strBuilder.Append("'" + strZP041A.strERZET + "',");       //采购传物采创建时间
                            strBuilder.Append("'" + strZP041A.strRERDAT + "',");      //采购传物采日期
                            strBuilder.Append("'" + strZP041A.strRERZET + "',");      //采购传物采时间
                            strBuilder.Append("'" + strZP041A.strERDATB + "',");      //物采平台返回日期 
                            strBuilder.Append("'" + strZP041A.strERZETB + "',");      //物采平台返回时间   

                            strBuilder.Append("'" + strEKKPO.strBUKRS + "',");       //公司代码                   
                            strBuilder.Append("'" + strEKKPO.strAEDAT + "',");       //采购订单生成时间
                            strBuilder.Append("'" + strEKKPO.strUDATE + "',");       //采购订单审批时间 

                            strBuilder.Append("'" + strZC022.strTBSJ + "',");        //提报时间                    
                            strBuilder.Append("'" + strZC022.strZJYPSTR + "',");    //检验批          
                            strBuilder.Append("'" + strZC022.strSQH + "',");         //建造申请号      
                            strBuilder.Append("'" + strZC022.strJZSJ + "',");        //转建造时间      
                            strBuilder.Append("'" + strZC022.strSQLX + "',");        //申请类型        
                            strBuilder.Append("'" + strZC022.strZT + "',");          //申请状态        
                            strBuilder.Append("'" + strZC022.strJZLXR + "',");       //建造联系人      
                            strBuilder.Append("'" + strZC022.strSPJB + "',");        //建造审批级别    
                            strBuilder.Append("'" + strZC022.strDATUM + "',");       //建造审批日期    
                            strBuilder.Append("'" + strZC022.strUZEIT + "',");       //建造审批时间    
                            strBuilder.Append("'" + strZC022.strSPYJ + "',");        //建造审批意见   

                            strBuilder.Append("'" + strEKES.strERDAT + "',");        //生成交货单日期                  
                            strBuilder.Append("'" + strEKES.strEZEIT + "',");        //生成交货单时间      
                            strBuilder.Append("'" + strEKES.strEINDT + "',");        //交货日期            
                            strBuilder.Append("'" + strEKES.strVBELN + "',");        //交货单              
                            strBuilder.Append("'" + strEKES.strVBELP + "',");        //交货单行项目        
                            strBuilder.Append("'" + strEKES.strCJRQ + "',");         //外观检查创建日期    
                            strBuilder.Append("'" + strEKES.strJYWCRQ + "',");       //外观检查完成日期    
                            strBuilder.Append("'" + strEKES.strTZDCJRQ + "',");      //交接验收单创建日期   

                            strBuilder.Append("'" + strEKBE.strBELNR + "',");        //物料凭证
                            strBuilder.Append("'" + strEKBE.strBUZEI + "',");        //物料凭证行项目号
                            strBuilder.Append("'" + strEKBE.strMJAHR + "',");        //物料凭证年度   
                            strBuilder.Append("'" + strEKBE.strDJBUDAT + "',");      //过账日期  收货至冻结库存日期   
                            strBuilder.Append("'" + strEKBE.strDJCPUTM + "',");      //过账时间  收货至冻结库存时间
                            strBuilder.Append("'" + strEKBE.strBUDAT + "',");        //过账日期  物资入库日期  
                            strBuilder.Append("'" + strEKBE.strCPUTM + "',");        //过账时间  物资入库时间

                            strBuilder.Append("'" + strZC035.strZCJDATE + "',");        //报检单创建日期
                            strBuilder.Append("'" + strZC035.strZBJNO + "',");          //报检单号
                            strBuilder.Append("'" + strZC035.strZBJITEM + "',");        //报检单行项目   
                            strBuilder.Append("'" + strZC035.strZCJNAME + "',");        //报检单创建人  

                            strBuilder.Append("'" + strZC033.strZFXZDATE + "',");        //检验结果创建日期
                            strBuilder.Append("'" + strZC033.strZFXZMBLNR + "',");       //检验结果创建时间
                            strBuilder.Append("'" + strZC033.strZFXZDATE + "',");        //检验结束日期
                            strBuilder.Append("'" + strZC033.strZFXZMBLNR + "',");       //检验过账日期

                            strBuilder.Append("'" + strZC033.strZFXZDATE + "',");        //转非限制库存日期
                            strBuilder.Append("'" + strZC033.strZFXZMBLNR + "',");       //非限制物料凭证

                            strBuilder.Append("'" + strEKKPO.strCGEBLNR + "',");       //采购发票号
                            strBuilder.Append("'" + strEKKPO.strCGCPUDT + "',");       //发票校验输入日期
                            strBuilder.Append("'" + strEKKPO.strCGBUDAT + "'");       //发票校验输入日期

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
                                    ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表发生异常:" + exception5);
                                }
                            }
                            #endregion
                            #endregion

                            #endregion
                        }
                    }
                    #endregion
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
                ClsErrorLogInfo.WriteSapLog("1", "cgsj", "ALL", p_para.Sap_AEDAT, "插入hb_wzcgsj表发生异常:" + exception5);
            }

            return Result;
        }
    }
}
