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
            try
            {
                string sqlLQUA = @"begin delete from CONVERT_SWKC where KCTYPE=0;
                                    INSERT INTO CONVERT_SWKC (WERKS,MATKL,MATNR,MAKTX,
                                     MEINS,GESME,LGORT,LGPLA,ERDAT,WERKS_NAME,LGORT_NAME,ZSTATUS,YXQ,DLDATE,KCTYPE)
                                     SELECT A.WERKS,C.MATKL,A.MATNR,C.MAKTX,
                                     F.JBJLDW,A.GESME,A.LGORT,A.LGPLA,case when SUBSTR(A.WDATU, 0, 8)='00000000' then A.BDATU ELSE A.WDATU END,D.DW_NAME,E.KCDD_NAME,'04','',to_char(sysdate,'yyyymmdd'),0
                                     FROM LQUA A JOIN(
                                    select WERKS,MATNR,substr(lgpla,0,2) DKCODE,LGORT,max(BDATU) BDATU,max(LQNUM) LQNUM
                                    from LQUA where regexp_like(lgpla,'^[0-9]+[0-9]$') 
                                     group by werks,matnr,LGORT,substr(lgpla,0,2)) B ON A.WERKS=B.WERKS AND A.MATNR=B.MATNR AND substr(A.lgpla,0,2)=B.DKCODE AND A.LGORT=B.LGORT AND A.BDATU=B.BDATU AND A.LQNUM=B.LQNUM
                                     JOIN MARA C ON A.MATNR=C.MATNR
                                     join WZ_DW D ON D.DW_CODE=A.Werks
                                    LEFT join WZ_KCDD E ON E.DWCODE=A.WERKS AND E.KCDD_CODE=A.LGORT
                                    JOIN WZ_WLZ F ON F.PMCODE=C.MATKL ; COMMIT; end;"; //查询lqua表库存，lapla 为数字得，子查询是按物料编码取最新一条数据，主要取里面得BDATU,WDATU
                //regexp_like(lgpla,'^[0-9]+[0-9]$')过滤lapla不是数字的
                Result = m_Conn.ExecuteSql(sqlLQUA);
                if (Result)
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKC表上架部分成功");
                }
                else
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKC表上架部分失败");
                }
                string sqlSWKCDetail = @"  begin DELETE FROM CONVERT_SWKCDETAIL;
                                            INSERT INTO CONVERT_SWKCDETAIL(WERKS,WERKS_NAME,LGORT,MATNR,GESME,ERDATE,DLNAME,PMNAME,DLDATE)
                                            SELECT A.WERKS,D.DW_NAME,A.LGORT,A.MATNR,A.GESME 
                                            ,case when SUBSTR(A.WDATU, 0, 8)='00000000' then A.BDATU ELSE A.WDATU END,F.DLNAME,F.PMNAME,to_char(sysdate,'yyyymmdd')
                                             FROM LQUA A JOIN(
                                            select WERKS,MATNR,substr(lgpla,0,2) DKCODE,LGORT,max(BDATU) BDATU,max(LQNUM) LQNUM
                                            from LQUA where regexp_like(lgpla,'^[0-9]+[0-9]$') --AND  matnr='000000011000683835'
                                             group by werks,matnr,LGORT,substr(lgpla,0,2)) B ON A.WERKS=B.WERKS AND A.MATNR=B.MATNR AND substr(A.lgpla,0,2)=B.DKCODE AND A.LGORT=B.LGORT AND A.BDATU=B.BDATU AND A.LQNUM=B.LQNUM
                                             JOIN MARA C ON A.MATNR=C.MATNR
                                             join WZ_DW D ON D.DW_CODE=A.Werks
                                            JOIN WZ_WLZ F ON F.PMCODE=C.MATKL ;COMMIT;end;";
                Result = m_Conn.ExecuteSql(sqlSWKCDetail);
                if (Result)
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAIL", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKCDETAIL表上架部分成功");
                }
                else
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAIL", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKCDETAIL表上架部分失败");
                }

                strBuilder.Length = 0;
                strBuilder.Append(@" begin INSERT INTO CONVERT_SWKC (WERKS,ZDHTZD,ZITEM,MATKL,MATNR,MAKTX,
                                                    MEINS,GESME,LGORT,LGPLA,ERDAT,WERKS_NAME,LGORT_NAME,ZSTATUS,YXQ,DLDATE,KCTYPE)                                          
                                                     select t.WERKS,t.ZDHTZD,t.ZITEM,d.MATKL,  t.MATNR,d.MAKTX,
                                                     e.JBJLDW,k.VMENGE01,t.LGORT,'' LGPLA ,t.ZCJRQ, b.DW_NAME,c.KCDD_NAME,'03','','" + dldate + "',0 ");
                strBuilder.Append(@" from ZC10MMDG072 t
                                                       join ZC10MMDG076 k on t.ZDHTZD =k.ZDHTZD and t.ZITEM=k.ZITEM
                                                     join WZ_DW b ON b.DW_CODE=t.Werks
                                                     join WZ_KCDD c ON c.DWCODE=t.WERKS AND c.KCDD_CODE=t.LGORT
                                                    join    MARA d on d.matnr=t.matnr
                                                    join WZ_WLZ e on e.PMCODE=d.MATKL
                                                     where ZSTATUS ='03'; commit; end; ");//插入质检状态的入库单作为另一部分库存
                Result = m_Conn.ExecuteSql(strBuilder.ToString());
                if (Result)
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKC表质检部分成功");
                }
                else
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKC表质检部分失败");
                }

                string sqlupdateje = @"UPDATE CONVERT_SWKCDETAIL A SET A.SCJE=(select ROUND(B.SALK3/LBKUM*A.GESME,2) 
                                                        FROM MBEW B
                                                        WHERE B.SALK3>0 AND A.MATNR=B.MATNR AND  A.WERKS=B.BWKEY)";
                if (!m_Conn.ExecuteSql(sqlupdateje))
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAIL", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "更新CONVERT_SWKCDETAIL表金额失败");
                }
                ClsDataLoadBAOBIAO.SAPLoadData(p_para);//下架未过账库存金额模型
            }
            catch (Exception ex)
            {
                strBuilder.Length = 0;
                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKC", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKC表发生异常:" + ex.Message);
                return false;
            }
            return Result;
        }
    }
}
