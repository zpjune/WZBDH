using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
   public class ClsDataLoadCKJE : ISAPLoadInterface
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
            string sqldelete = "BEGIN delete from CONVERT_CKJE;COMMIT;END; ";
            try
            {
                Result = m_Conn.ExecuteSql(sqldelete);
            }
            catch (Exception ex)
            {
                ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "模型转换-删除CONVERT_RKJE表发生异常:" + ex.Message);
                return false;
            }
            if (Result)
            {
                try
                {
                    string sql = " begin  INSERT INTO CONVERT_CKJE(WERKS,WERKS_NAME,LGORT,LGORT_NAME,ZCKTZD,ZCITEM,MBLNR,ZEILE,JE,BUDAT_MKPF,DLDATE)";
                    sql += "  SELECT A.WERKS, C.DW_NAME ,A.LGORT, D.KCDD_NAME,A.ZCKTZD,A.ZCITEM,A.MBLNR, A.ZEILE,B.DMBTR,B.BUDAT_MKPF,'" + DateTime.Now.ToString("yyyyMMdd") + "'";
                    sql += "  FROM ZC10MMDG078 A";
                    sql += "  JOIN MSEG B ON A.MBLNR=B.MBLNR AND A.ZEILE=B.ZEILE";
                    sql += "  JOIN WZ_DW C ON A.WERKS=C.DW_CODE";
                    sql += "  JOIN WZ_KCDD D ON A.WERKS=D.DWCODE AND A.LGORT=D.KCDD_CODE; commit;end;";
                    Result = m_Conn.ExecuteSql(sql);
                    if (Result)
                    {
                        ClsErrorLogInfo.WriteSapLog("1", "CKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "模型转换-插入CONVERT_CKJE表成功");
                    }
                    else
                    {
                        ClsErrorLogInfo.WriteSapLog("1", "CKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "模型转换-插入CONVERT_CKJE表失败");
                    }
                }
                catch (Exception ex)
                {
                    ClsErrorLogInfo.WriteSapLog("1", "CKJE", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "模型转换-插入CONVERT_CKJE表发生异常:" + ex.Message);
                    return false;
                }
                
            }
            else
            {
                ClsErrorLogInfo.WriteSapLog("1", "CKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_CKJE表失败");
            }
            return Result;
            
        }
    }
}
