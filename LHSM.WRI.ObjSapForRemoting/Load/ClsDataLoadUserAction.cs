using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadUserAction : ISAPLoadInterface
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
            public string strBUKRS;   //公司代码
            public string strBUKRSMC; //公司名称
            public string strERNAM;   //采购订单创建人员 
            public string strERNAMMC; //采购订单创建人员
        }

        /// <summary>
        /// 业务单据数量
        /// </summary>
        private struct YWCOUNT
        {
            public string str_lxbm; //采购订单编码(业务模块)
            public string str_lxmc; //业务模块名称
            public int intQBCount;  //全部单据量
            public int intYXCount;  //有效单据量
            public int intCXCount;  //冲销单据量
        }

        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;

            m_Conn = ClsUtility.GetConn();

            DataTable dtEKKODate = new DataTable();
            try
            {
                //查询EKKO的数据               
                string strSqlIMPR = "select distinct AEDAT from EKKO t where dldate='" + p_para.Sap_AEDAT + "' ";
                dtEKKODate = m_Conn.GetSqlResultToDt(strSqlIMPR);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "userAction", "ALL", p_para.Sap_AEDAT, "插入hb_useraction表过程中查询EKKO表发生异常:\t\n" + exception);
                return Result;
            }
            foreach (DataRow subRowEKKODate in dtEKKODate.Rows)
            {
                if (subRowEKKODate["AEDAT"] == null || string.IsNullOrEmpty(subRowEKKODate["AEDAT"].ToString()))
                {
                    continue;
                }

                string strDate = subRowEKKODate["AEDAT"].ToString().Substring(0, 6);

                EKKO strEKKO = new EKKO();
                DataTable dtEKKO = new DataTable();
                try
                {
                    //查询EKKO的数据               
                    string strSqlIMPR = "select distinct t.ERNAM from EKKO t where substr(aedat,0,6)='" + strDate + "'";
                    dtEKKO = m_Conn.GetSqlResultToDt(strSqlIMPR);
                }
                catch (Exception exception)
                {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "userAction", "ALL", p_para.Sap_AEDAT, "插入hb_useraction表过程中查询EKKO表发生异常:\t\n" + exception);
                    return Result;
                }
                if (dtEKKO != null)
                {
                }
                else
                {
                    string sss = "";
                }

                strBuilder.Length = 0;
                strBuilder.Append(" Begin "); //开始执行SQL

                foreach (DataRow subRowEKKO in dtEKKO.Rows)
                {

                    strEKKO.strERNAM = subRowEKKO["ERNAM"].ToString();

                    strBuilder.Append(" DELETE FROM HB_USERACTION WHERE UA_DATE='" + strDate + "'  AND UA_YHBM='" + strEKKO.strERNAM + "';");

                    try
                    {
                        //用户名
                        strEKKO.strERNAMMC = m_Conn.GetSqlResultToStr("select distinct t.name_last from USR02 t where t.bname='" + strEKKO.strERNAM + "'");
                    }
                    catch (Exception exception1)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "userAction", "ALL", p_para.Sap_AEDAT, "插入hb_useraction表过程中查询USER_ADDR表发生异常:\t\n" + exception1);
                        return Result;
                    }

                    try
                    {
                        string dwsql = "select t.department from USR02 t where t.bname='" + strEKKO.strERNAM + "'";
                        //公司编码 //公司名称
                        strEKKO.strBUKRSMC = m_Conn.GetSqlResultToStr(dwsql);
                    }
                    catch (Exception exception1)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "userAction", "ALL", p_para.Sap_AEDAT, "插入hb_useraction表过程中查询USR02表发生异常:\t\n" + exception1);
                        return Result;
                    }

                    try
                    {
                        string strcountsql = " select LX,LOEKZ, count(1) as LXCOUNT from (";
                        strcountsql += " select trim(t.bsart) as LX, t.loekz as LOEKZ from EKKO t";
                        strcountsql += " where t.ernam='" + strEKKO.strERNAM + "' )";
                        strcountsql += " group by lx, loekz";

                        //业务单据量
                        DataTable dtcount = m_Conn.GetSqlResultToDt(strcountsql);

                        if (dtcount != null && dtcount.Rows.Count > 0)
                        {
                            YWCOUNT ywAll = new YWCOUNT();
                            ywAll.str_lxbm = "ALL";
                            ywAll.str_lxmc = getLXMC(ywAll.str_lxbm);
                            ywAll.intQBCount = 0;
                            ywAll.intYXCount = 0;
                            ywAll.intCXCount = 0;

                            YWCOUNT yw70 = SetYWCount("X008", dtcount);
                            if (!string.IsNullOrEmpty(yw70.str_lxbm))
                            {
                                ywAll.intQBCount = yw70.intQBCount;
                                ywAll.intYXCount = yw70.intYXCount;
                                ywAll.intCXCount = yw70.intCXCount;
                                strBuilder.Append(insertsql(strEKKO, yw70, strDate));
                            }

                            YWCOUNT yw71 = SetYWCount("X009", dtcount);
                            if (!string.IsNullOrEmpty(yw71.str_lxbm))
                            {
                                ywAll.intQBCount = yw71.intQBCount;
                                ywAll.intYXCount = yw71.intYXCount;
                                ywAll.intCXCount = yw71.intCXCount;
                                strBuilder.Append(insertsql(strEKKO, yw71, strDate));
                            }

                            YWCOUNT yw45 = SetYWCount(dtcount);
                            if (!string.IsNullOrEmpty(yw45.str_lxbm))
                            {
                                ywAll.intQBCount = yw45.intQBCount;
                                ywAll.intYXCount = yw45.intYXCount;
                                ywAll.intCXCount = yw45.intCXCount;
                                strBuilder.Append(insertsql(strEKKO, yw45, strDate));
                            }

                            if (ywAll.intQBCount > 0)
                            {
                                strBuilder.Append(insertsql(strEKKO, ywAll, strDate));
                            }
                        }


                    }
                    catch (Exception exception1)
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "userAction", "ALL", p_para.Sap_AEDAT, "插入hb_useraction表过程中查询EKKO表业务量发生异常:\t\n" + exception1);
                        return Result;
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
                    ClsErrorLogInfo.WriteSapLog("1", "userAction", "ALL", p_para.Sap_AEDAT, "插入hb_useraction表发生异常:" + exception5);
                }
            }
            return Result;
        }

        private string getLXBM(string str_lx)
        {
            string ret = string.Empty;
            switch (str_lx)
            {
                case "X008":
                    ret = "70";
                    break;
                case "X009":
                    ret = "71";
                    break;

                case "ALL":
                    ret = "全部";
                    break;
                default :  
                    ret = "45";
                    break;
            }

            return ret;
        }

        private YWCOUNT SetYWCount(string str_lx,DataTable dt)
        {
            YWCOUNT yw = new YWCOUNT();

            DataRow[] dr = dt.Select(" LX='" + str_lx + "'");
            if (dr!=null&&dr.Length>0)
            {
                yw.str_lxbm =getLXBM( str_lx);
                yw.str_lxmc = getLXMC(str_lx);

                foreach (DataRow item in dr)
                {
                    string loe = item["LOEKZ"]==null?"":item["LOEKZ"].ToString();
                    int count = int.Parse(item["LXCOUNT"]==null?"0":item["LXCOUNT"].ToString());
                    if (string.IsNullOrEmpty(loe))
                    {
                        yw.intYXCount += count;
                    }
                    else
                    {
                        yw.intCXCount += count;
                    }

                    yw.intQBCount += count;
                }              
            }

            return yw;
        }

        private YWCOUNT SetYWCount( DataTable dt)
        {
            string str_lx = "WZ";
            YWCOUNT yw = new YWCOUNT();

            DataRow[] dr = dt.Select(" LX <> 'X008' AND LX <> 'X009'");
            if (dr != null && dr.Length > 0)
            {
                yw.str_lxbm = "45";
                yw.str_lxmc = getLXMC(str_lx);

                foreach (DataRow item in dr)
                {
                    string loe = item["LOEKZ"] == null ? "" : item["LOEKZ"].ToString();
                    int count = int.Parse(item["LXCOUNT"] == null ? "0" : item["LXCOUNT"].ToString());
                    if (string.IsNullOrEmpty(loe))
                    {
                        yw.intYXCount += count;
                    }
                    else
                    {
                        yw.intCXCount += count;
                    }

                    yw.intQBCount += count;
                }
            }

            return yw;
        }

        private string getLXMC(string str_lx)
        {
            string ret = string.Empty;
            switch (str_lx)
            {
                case "X008":
                    ret = "项目模块";
                    break;
                case "X009":
                    ret = "设备模块";
                    break;
                case "WZ":
                    ret = "物资模块";
                    break;
                case "ALL":
                    ret = "全部";
                    break;
            }

            return ret;
        }

        private StringBuilder insertsql(EKKO ekko,YWCOUNT yw,string str_date)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(" INSERT INTO HB_USERACTION");
            sb.Append("(UA_ID,UA_DATE,UA_GSBM,UA_GSMC,UA_YHBM,UA_YHMC,UA_YWBM,UA_YEMC,UA_DJZL,UA_DJYXL,UA_DJCXL)");
            sb.Append(" VALUES(");
            sb.Append("SQ_USERACTION.NEXTVAL,");
            sb.Append("'" + str_date + "',");
            sb.Append("'" + ekko.strBUKRS + "',");
            sb.Append("'" + ekko.strBUKRSMC + "',");
            sb.Append("'" + ekko.strERNAM + "',");
            sb.Append("'" + ekko.strERNAMMC + "',");
            sb.Append("'" + yw.str_lxbm + "',");
            sb.Append("'" + yw.str_lxmc + "',");
            sb.Append(yw.intQBCount + ",");
            sb.Append(yw.intYXCount + ",");
            sb.Append(yw.intCXCount + "");
            sb.Append(");");

            return sb;
        }
    }
}
