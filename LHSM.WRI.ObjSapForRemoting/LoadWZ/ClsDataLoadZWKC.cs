using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 模型转换-账务库存
    /// </summary>
    public class ClsDataLoadZWKC : ISAPLoadInterface
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
            DataTable dtSWKC = new DataTable();
            try
            {
                string stedate = p_para.Sap_AEDAT.Substring(0, 6);
                //是第一次转换，先删除操作 ，再insert
                //  插入CONVERT_ZWKC表模型
                string strSqlEkko = @" begin delete from CONVERT_ZWKC; INSERT INTO  CONVERT_ZWKC (BWKEY,BWKEY_NAME,MATNR,SALK3,DLCODE,DLNAME,ZLCODE,ZLNAME,XLCODE,XLNAME,MATKL,PMNAME,LBKUM,DANJIA,DLDATE)
                                    SELECT   A.BWKEY,D.DW_NAME,A.MATNR,A.SALK3,C.DLCODE,C.DLNAME,C.ZLCODE,C.ZLNAME,C.XLCODE,C.XLNAME,B.MATKL,C.PMNAME,A.LBKUM,A.SALK3/A.LBKUM,'" + DateTime.Now.ToString("yyyy-MM-dd") + "'";
                strSqlEkko+= @" FROM MBEW A 
                                    JOIN MARA B ON A.MATNR=B.MATNR
                                    JOIN WZ_WLZ C ON C.PMCODE=B.MATKL
                                    JOIN WZ_DW D ON D.DW_CODE=A.BWKEY 
                                     ; commit;end ;";
                    Result = m_Conn.ExecuteSql(strSqlEkko);
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_ZWKC", "ALL", DateTime.Now.ToString("yyyy-MM-dd"), "插入CONVERT_ZWKC表过程中发生异常:\t\n" + exception);
                return Result;
            }
            return Result;
        }
    }
}
