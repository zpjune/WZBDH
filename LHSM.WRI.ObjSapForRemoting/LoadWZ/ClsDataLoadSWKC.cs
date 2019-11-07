using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
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
        private StringBuilder strDetail = new StringBuilder();
        public bool SAPLoadData(ClsSAPDataParameter p_para)
        {
            string dldate = p_para.Sap_AEDAT;
            bool Result = true;
            m_Conn = ClsUtility.GetConn();
            strBuilder.Append(" Begin ");
            strDetail.Append(" Begin ");
            try
            {
                string sqlLQUA = @"select SUM(t.GESME) GESME,t.MATNR,t.WERKS,t.LGORT,substr(t.LGPLA,1,4) LGPLA,b.DW_NAME,c.KCDD_NAME
                                    from LQUA t 
                                    join WZ_DW b ON b.DW_CODE=t.Werks
                                    join WZ_KCDD c ON c.DWCODE=t.WERKS AND c.KCDD_CODE=t.LGORT
                                    where  regexp_like(lgpla,'^[0-9]+[0-9]$') and matnr  in (select matnr from ZC10MMDG085B) 
                                     GROUP BY t.MATNR,t.WERKS,t.LGORT,substr(t.LGPLA,1,4),b.DW_NAME,c.KCDD_NAME  "; //查询lqua表库存，以上架后的(lgpla为数字的，并且matnr 在085b里有的) 
                //regexp_like(lgpla,'^[0-9]+[0-9]$')过滤不是数字的
                DataTable dtLQUA = m_Conn.GetSqlResultToDt(sqlLQUA);
                if (dtLQUA != null && dtLQUA.Rows.Count > 0)
                {
                    string sqlWLZ = @"  select  d.MATNR,d.MATKL,d.MAKTX,e.PMNAME,e.JBJLDW ,e.DLNAME
                                    FROM   MARA d 
                                    join WZ_WLZ e on e.PMCODE=d.MATKL ";//查询物料组基本计量单位，物料描述
                    DataTable dtWLZ= m_Conn.GetSqlResultToDt(sqlWLZ);
                    string sql085B = @"select MATNR,WERKS,LGORT,substr(LGPLA,1,4)LGPLA,GESME,ERDAT,ZDHTZD ,ZITEM 
                                          from ZC10MMDG085B A
                                         WHERE GESME>0 and ZDHTZD is not null
                                          order by  MATNR,WERKS,LGORT,substr(LGPLA,1,4) ASC,ERDAT  DESC";//查询085B 因为这个表有仓位//倒查是否有积压
                    DataTable dt085B = m_Conn.GetSqlResultToDt(sql085B);
                    int a = 0;
                    foreach (DataRow rowLQUA in dtLQUA.Rows)
                    { 
                        double GESME = double.Parse(rowLQUA["GESME"].ToString());
                        //按工厂，库存地点，仓位前四位，物料编码 分组查询085b
                        DataRow[] arr085B = dt085B.Select("MATNR='" + rowLQUA["MATNR"] + "' and WERKS='" + rowLQUA["WERKS"] + "' and  LGORT='" + rowLQUA["LGORT"] + "' and LGPLA='"+ rowLQUA["LGPLA"] + "' ");
                        //按物料查询物料描述，基本计量单位等。
                        DataRow[] arrWLZ = dtWLZ.Select(" MATNR='"+ rowLQUA["MATNR"] + "'  ");
                        if (arr085B.Length > 0)
                        {
                            double totalGESME = 0;
                            foreach (var item in arr085B)
                            {
                                totalGESME = totalGESME + double.Parse(item["GESME"].ToString());
                                if (GESME > totalGESME)
                                {
                                    if (arrWLZ.Length>0) {
                                        strDetail.Append(" INSERT INTO CONVERT_SWKCDETAIL(WERKS,WERKS_NAME,LGORT,MATNR,GESME,ERDATE,DLNAME,PMNAME,DLDATE) VALUES(  ");//插入明细表，为后面算账务金额做基础
                                        strDetail.Append(" '" + rowLQUA["WERKS"].ToString() + "', ");
                                        strDetail.Append(" '" + rowLQUA["DW_NAME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + rowLQUA["LGORT"]?.ToString() + "', ");
                                        strDetail.Append(" '" + item["MATNR"]?.ToString() + "', ");
                                        strDetail.Append(" '" + item["GESME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + item["ERDAT"]?.ToString() + "', ");
                                        strDetail.Append(" '" + arrWLZ[0]["DLNAME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + arrWLZ[0]["PMNAME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + dldate + "'); ");
                                    }
                                    continue;//如果当前累计数量和小于库存，继续循环
                                }
                                else {
                                    //插入明细表，为后面算账务金额做基础
                                    if (arrWLZ.Length > 0)
                                    {
                                        strDetail.Append(" INSERT INTO CONVERT_SWKCDETAIL(WERKS,WERKS_NAME,LGORT,MATNR,GESME,ERDATE,DLNAME,PMNAME,DLDATE) VALUES(  ");//插入明细表，为后面算账务金额做基础
                                        strDetail.Append(" '" + rowLQUA["WERKS"].ToString() + "', ");
                                        strDetail.Append(" '" + rowLQUA["DW_NAME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + rowLQUA["LGORT"]?.ToString() + "', ");
                                        strDetail.Append(" '" + item["MATNR"]?.ToString() + "', ");
                                        strDetail.Append(" '" + item["GESME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + item["ERDAT"]?.ToString() + "', ");
                                        strDetail.Append(" '" + arrWLZ[0]["DLNAME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + arrWLZ[0]["PMNAME"]?.ToString() + "', ");
                                        strDetail.Append(" '" + dldate + "'); ");
                                    }


                                    //根据mard库存按工厂、物料码、库存地点，分组，回找入库通知单，直到入库通知单数量和大于或等于mard数量
                                    //将近期入库通知单收货数量小于库存的都插入实物库存表CONVERT_SWKC
                                    strBuilder.Append("  INSERT INTO CONVERT_SWKC (WERKS,ZDHTZD,ZITEM,MATKL,MATNR,MAKTX," +
                                       "MEINS,GESME,LGORT,LGPLA,ERDAT,WERKS_NAME,LGORT_NAME,ZSTATUS,YXQ,DLDATE,KCTYPE) VALUES(");
                                    strBuilder.Append(" '" + rowLQUA["WERKS"].ToString() + "',");
                                    strBuilder.Append(" '" + item["ZDHTZD"] + "',");
                                    strBuilder.Append(" '" + item["ZITEM"] + "',");
                                    if (arrWLZ.Length>0) {
                                        strBuilder.Append(" '" + arrWLZ[0]["MATKL"] ?.ToString().Trim() );
                                        strBuilder.Append("',");
                                    }
                                    else {
                                        strBuilder.Append(" '',");
                                    }
                                    strBuilder.Append(" '" + item["MATNR"] + "',");
                                    if (arrWLZ.Length > 0)
                                    {
                                        strBuilder.Append(" '" + arrWLZ[0]["MAKTX"] ?.ToString().Trim() );
                                        strBuilder.Append("',");
                                        strBuilder.Append(" '" + arrWLZ[0]["JBJLDW"] ?.ToString().Trim().Replace('\'', ' ').Replace('(', ' ').Replace(')', ' ').Replace('）', ' ').Replace('（', ' ') );
                                        strBuilder.Append("',");
                                    }
                                    else
                                    {
                                        strBuilder.Append(" '','',");
                                    }
                                   
                                    strBuilder.Append(" '" + GESME.ToString() + "',");
                                    strBuilder.Append(" '" + rowLQUA["LGORT"] + "',");
                                    strBuilder.Append(" '"+ rowLQUA["LGPLA"] + "',");//仓位

                                    strBuilder.Append(" '" + item["ERDAT"]?.ToString() + "',");
                                    strBuilder.Append(" '" + rowLQUA["DW_NAME"] + "',");
                                    strBuilder.AppendLine(" '" + rowLQUA["KCDD_NAME"].ToString().Replace('(', ' ').Replace(')', ' ').Replace('）', ' ').Replace('（', ' ').Replace('\'', ' ') +
                                        "','04','','"+ dldate + "',0);  ");

                                }
                            }
                        }

                    }
                    if (strBuilder.Length > 8)
                    {
                        strBuilder.Append(" COMMIT; end ; ");
                        strDetail.Append(" COMMIT; end ; ");
                        if (m_Conn.ExecuteSql(" begin delete from CONVERT_SWKC;DELETE FROM CONVERT_SWKCDETAIL; commit; end; "))
                        {
                            Result = m_Conn.ExecuteSql(strDetail.ToString());//执行失败竟然也返回true
                            if (Result)
                            {
                                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAIL", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKCDETAIL失败,应该是sql有问题，可能含特殊字符了，");
                            }
                            else {
                                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAIL", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKCDETAIL失败,应该是sql有问题，可能含特殊字符了，");
                            }
                            Result = ClsUtility.ExecuteSqlToDb(strBuilder.ToString());//执行失败竟然也返回true
                            if (Result)
                            {
                                DataTable dt = m_Conn.GetSqlResultToDt("select 1 from CONVERT_SWKC ");//要重新判断是否插入成功
                                if (dt==null||dt.Rows.Count==0) {
                                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表上架部分失败,应该是sql有问题，可能含特殊字符了，");
                                    Result = false;
                                }
                                else
                                {
                                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表上架部分成功");
                                    strBuilder.Length = 0;
                                    strBuilder.Append(@" begin INSERT INTO CONVERT_SWKC (WERKS,ZDHTZD,ZITEM,MATKL,MATNR,MAKTX,
                                                    MEINS,GESME,LGORT,LGPLA,ERDAT,WERKS_NAME,LGORT_NAME,ZSTATUS,YXQ,DLDATE)                                          
                                                     select t.WERKS,t.ZDHTZD,t.ZITEM,d.MATKL,  t.MATNR,d.MAKTX,
                                                     e.JBJLDW,t.ZDHSL,t.LGORT,'' LGPLA ,t.ZCJRQ, b.DW_NAME,c.KCDD_NAME,'03','','" + dldate + "' ");
                                          strBuilder.Append(@" from ZC10MMDG072 t
                                                     join WZ_DW b ON b.DW_CODE=t.Werks
                                                     join WZ_KCDD c ON c.DWCODE=t.WERKS AND c.KCDD_CODE=t.LGORT
                                                    join    MARA d on d.matnr=t.matnr
                                                    join WZ_WLZ e on e.PMCODE=d.MATKL
                                                     where ZSTATUS ='03'; commit; end; ");//插入质检状态的入库单作为另一部分库存
                                    Result = m_Conn.ExecuteSql(strBuilder.ToString());
                                    if (Result) {
                                        ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表质检部分成功");
                                    }
                                    else {
                                        ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC表质检部分失败");
                                    }

                                }
                                string sqlupdateje = @"UPDATE CONVERT_SWKCDETAIL A SET A.SCJE=(select ROUND(B.SALK3/LBKUM*A.GESME,2) 
                                                        FROM MBEW B
                                                        WHERE B.SALK3>0 AND A.MATNR=B.MATNR AND  A.WERKS=B.BWKEY)";
                                if (!m_Conn.ExecuteSql(sqlupdateje)) {
                                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAIL", "ALL", p_para.Sap_AEDAT, "更新CONVERT_SWKCDETAIL表金额失败");
                                }
                            }
                            else
                            {
                                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_SWKC上架部分表失败");
                            }
                        }
                    }
                }
                ClsDataLoadBAOBIAO.SAPLoadData(p_para);//下架未过账库存金额模型
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
