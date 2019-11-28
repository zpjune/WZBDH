using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
   public class ClsDataLoadBAOBIAO 
    {
        //积压物资统计报表
        //库存物资统计报表
        //数据库连接
        private static ClsDBConnection m_Conn = null;

        /// <summary>
        /// 存储sql字符串变量
        /// </summary>
        private StringBuilder strBuilder = new StringBuilder();

        public static void SAPLoadData(ClsSAPDataParameter p_para)
        {
            bool Result = true;
            string dldate = p_para.Sap_AEDAT;
            m_Conn = ClsUtility.GetConn();
            string sqlDelete = "begin DELETE FROM CONVERT_SWKCDETAILCKJE;commit; end;";
            if (m_Conn.ExecuteSql(sqlDelete))
            {
                string sql = "BEGIN  INSERT INTO CONVERT_SWKCDETAILCKJE(WERKS,WERKS_NAME,LGORT,MATNR,GESME,ZCKTZD,ZITEM,DLNAME,PMNAME,DLDATE,CKJE) ";
                sql += "select A.WERKS,D.DW_NAME,A.LGORT,A.MATNR,A.ZFHSL,A.ZCKTZD,A.ZCITEM,C.DLNAME,C.PMNAME,'" + DateTime.Now.ToString("yyyy-MM-dd") + "',ROUND(E.SALK3/LBKUM*A.ZFHSL,2)";
                sql += @" from  ZC10MMDG078 A 
                    JOIN WZ_DW D ON A.WERKS = D.DW_CODE
                    JOIN MBEW E ON E.BWKEY = A.WERKS AND E.MATNR = A.MATNR AND E.SALK3 > 0
                    JOIN WZ_WLZ C ON C.PMCODE = A.MATKL
                    WHERE A.ZSTATUS = '03'; COMMIT; END ;";
                Result = m_Conn.ExecuteSql(sql);
                if (Result)
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAILCKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKCDETAILCKJE表成功");
                }
                else
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAILCKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_SWKCDETAILCKJE表失败");
                }
            }
            else {
                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_SWKCDETAILCKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "删除CONVERT_SWKCDETAILCKJE表失败");
            }
            
        }
    }
}
