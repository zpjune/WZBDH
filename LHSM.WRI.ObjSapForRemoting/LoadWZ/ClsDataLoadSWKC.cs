using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 模型转换-实物库存
    /// </summary>
    public class ClsDataLoadSWKC : ISAPLoadInterface
    {
        //数据库连接
        private static ClsDBConnection m_Conn = null;

        /// <summary>
        /// 存储sql字符串变量
        /// </summary>
        private StringBuilder strBuilder = new StringBuilder();
        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;
            m_Conn = ClsUtility.GetConn();
            strBuilder.Append(" Begin ");
            try
            {
                string sqlMARD = @"select A.MATNR,A.WERKS,A.LGORT,A.LABST,A.lgpbe ,B.KCDD_NAME,C.DW_NAME
                                    from MARD A
                                    join WZ_KCDD B on A.WERKS =B.DWCODE AND A.LGORT=B.KCDD_CODE
                                    JOIN WZ_DW C ON C.DW_CODE=A.WERKS
                                    where labst>0 "; //查询mard表 已过帐数据
                DataTable dtMARD = m_Conn.GetSqlResultToDt(sqlMARD);
                if (dtMARD != null && dtMARD.Rows.Count > 0)
                {
                    string sql072 = @"select a.ZDHTZD,a.ZITEM,a.WERKS,a.ZDHSL,a.LGORT,a.MATNR,b.MATKL,b.MAKTX,c.JBJLDW,a.ZCJRQ,a.BUDAT
                                from ZC10MMDG072 a 
                                JOIN  MARA b on a.MATNR=b.MATNR AND b.MATKL=a.MATKL 
                                join WZ_WLZ c on b.MATKL=C.PMCODE 
                                where a.ZSTATUS='05' and a.ZDHSL>0 order by a.ZDHTZD desc, a.ZITEM asc ";//查询图库通知单已过帐数据
                    DataTable dt072 = m_Conn.GetSqlResultToDt(sql072);
                    foreach (DataRow rowMard in dtMARD.Rows)
                    { 
                        double LABST = double.Parse(rowMard["LABST"].ToString());
                        DataRow[] arr072 = dt072.Select("MATNR='" + rowMard["MATNR"] + "' and WERKS='" + rowMard["WERKS"] + "' and  LGORT='" + rowMard["LGORT"] + "' ");
                        if (arr072.Length > 0)
                        {
                            double totalZDHSL = 0;
                            foreach (var item in arr072)
                            {
                                totalZDHSL = totalZDHSL + double.Parse(item["ZDHSL"].ToString());
                                if (LABST > totalZDHSL)
                                {
                                    continue;//根据mard库存按工厂、物料码、库存地点，分组，回找入库通知单，直到入库通知单数量和大于或等于mard数量
                                }
                                else
                                {//插入库存模型表CONVERT_SWKC

                                    strBuilder.Append(" INSERT INTO CONVERT_SWKC (WERKS,ZDHTZD,ZITEM,MATKL,MATNR,MAKTX," +
                                        "MEINS,GESME,LGORT,LGPLA,ERDAT,WERKS_NAME,LGORT_NAME,YXQ) VALUES(");
                                    strBuilder.Append(" '"+rowMard["WERKS"].ToString() +"',");
                                    strBuilder.Append(" '"+item["ZDHTZD"] +"',");
                                    strBuilder.Append(" '" + item["ZITEM"] + "',");
                                    strBuilder.Append(" '" + item["MATKL"] + "',");
                                    strBuilder.Append(" '" + item["MATNR"] + "',");
                                    strBuilder.Append(" '" + item["MAKTX"].ToString().Trim()+ "',");
                                    strBuilder.Append(" '" + item["JBJLDW"].ToString().Trim().Replace('\'', ' ').Replace('(', ' ').Replace(')', ' ').Replace('）', ' ').Replace('（', ' ') + "',");
                                    strBuilder.Append(" '" + rowMard["LABST"] + "',");
                                    strBuilder.Append(" '" + rowMard["LGORT"] + "',");
                                    strBuilder.Append(" '',");
                                    if (item["BUDAT"] !=null|| item["BUDAT"].ToString()!="00000000")
                                    {
                                        strBuilder.Append(" '" + item["BUDAT"] + "',");//入库单有过账日期就存过账日期
                                    }
                                    else {
                                        strBuilder.Append(" '" + item["ZCJRQ"] + "',");//没有过账日期就存创建日期
                                    }
                                    strBuilder.Append(" '" + rowMard["DW_NAME"] + "',");
                                    strBuilder.AppendLine(" '" + rowMard["KCDD_NAME"].ToString().Replace('(',' ').Replace(')', ' ').Replace('）', ' ').Replace('（', ' ').Replace('\'', ' ') + "','');");
                                    break; 
                                }
                            }
                        }

                    }
                    if (strBuilder.Length > 8)
                    {
                        strBuilder.Append(" end ;");
                        if (ClsUtility.ExecuteSqlToDb("delete from CONVERT_SWKC;"))
                        {
                            Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());//执行失败竟然也返回true
                            if (Result)
                            {
                                DataTable dt = m_Conn.GetSqlResultToDt("select 1 from CONVERT_SWKC ");//要重新判断是否插入成功
                                if (dt==null||dt.Rows.Count==0) {
                                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表失败,应该是sql有问题，可能含特殊字符了，");
                                    Result = false;
                                }
                                else
                                {
                                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表成功");
                                }
                            }
                            else
                            {
                                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表失败");
                            }
                        }
                    }
                }
                strBuilder.Length = 0;
            }
            catch (Exception ex)
            {
                strBuilder.Length = 0;
                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表发生异常:" + ex.Message);
                return false;
            }
            return Result;
        }
    }
}
