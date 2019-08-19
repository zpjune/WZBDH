using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadRKJE : ISAPLoadInterface
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
            string sqldelete = " delete from CONVERT_RKJE ";
            try
            {
                Result = m_Conn.ExecuteSql(sqldelete);
            }
            catch (Exception ex)
            {
                ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_RKJE表发生异常:" + ex.Message);
                return false;
            }
            if (Result) {
                try
                {
                    ///根据入库单和凭证查询入库金额
                    string sql = "begin INSERT INTO CONVERT_RKJE(WERKS,WERKS_NAME,LGORT,LGORT_NAME,ZDHTZD,ZITEM,MBLNR,ZEILE,JE,BUDAT_MKPF,DLDATE)";
                    sql += "  SELECT A.WERKS, C.DW_NAME ,A.LGORT, D.KCDD_NAME,A.ZDHTZD,A.ZITEM,A.MBLNR, A.ZEILE,B.DMBTR,B.BUDAT_MKPF,'" + p_para.Sap_AEDAT + "'";
                    sql += "  FROM ZC10MMDG072 A";
                    sql += "  JOIN MSEG B ON A.MBLNR = B.MBLNR AND A.ZEILE = B.ZEILE";//凭证号和项目号
                    sql += "  JOIN WZ_DW C ON A.WERKS = C.DW_CODE";
                    sql += "  JOIN WZ_KCDD D ON A.WERKS = D.DWCODE AND A.LGORT = D.KCDD_CODE ; commit; end ; ";
                    Result = m_Conn.ExecuteSql(sql);
                    if (Result)
                    {
                        ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-插入CONVERT_RKJE表成功");
                    }
                    else
                    {
                        ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-插入CONVERT_RKJE表失败");
                    }
                }
                catch (Exception ex)
                {
                    ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-插入CONVERT_RKJE表发生异常:" + ex.Message);
                    return false;
                }
                
                
            }
            else {
                ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_RKJE表失败");
            }
            return Result;
        }
    }
}
