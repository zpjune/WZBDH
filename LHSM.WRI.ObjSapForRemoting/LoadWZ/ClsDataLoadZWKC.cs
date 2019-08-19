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
                string strMBEW = "SELECT COUNT(*) FROM MBEW";
                string stedate = p_para.Sap_AEDAT.Substring(0, 6);
                DataTable dtMBEW= m_Conn.GetSqlResultToDt(strMBEW);
                
                if (dtMBEW != null && dtMBEW.Rows.Count > 0 && dtMBEW.Rows[0][0].ToString() != "0")
                {//不是第一次转换，先删除操作 ，再insert
                    string sql= @"SELECT A.BWKEY,D.DW_NAME,A.SALK3,C.DLCODE,C.DLNAME,C.ZLCODE,C.ZLNAME,C.XLCODE,C.XLNAME,B.MATKL,C.PMNAME
                                    FROM MBEW A 
                                    JOIN MARA B ON A.MATNR=B.MATNR
                                    JOIN WZ_WLZ C ON C.PMCODE=B.MATKL
                                    JOIN WZ_DW D ON D.DW_CODE=A.BWKEY 
                                    WHERE A.DLDATE>='" + stedate + "'";
                    DataTable dtNew = m_Conn.GetSqlResultToDt(sql);
                    strBuilder.Append("begin ");
                    if (dtNew!=null&&dtNew.Rows.Count>0) {
                        foreach (DataRow row in dtMBEW.Rows)
                        {
                            strBuilder.AppendLine("  delete from CONVERT_ZWKC where MANDT='" + row["MANDT"] + "' and MATNR='" + row["MATNR"] + "' and BWKEY='" + row["BWKEY"] + "' and  BWTAR='" + row["BWTAR"] + "' ;  ");
                        }
                        strBuilder.AppendLine(@" INSERT INTO  CONVERT_ZWKC
                                                 (MANDT, MATNR, BWKEY, BWTAR, BWKEY_NAME, SALK3, DLCODE, DLNAME, ZLCODE, ZLNAME, XLCODE, XLNAME, MATKL, PMNAME)
                                    SELECT A.BWKEY, D.DW_NAME, A.SALK3, C.DLCODE, C.DLNAME, C.ZLCODE, C.ZLNAME, C.XLCODE, C.XLNAME, B.MATKL, C.PMNAME
                                    FROM MBEW A
                                    JOIN MARA B ON A.MATNR = B.MATNR
                                    JOIN WZ_WLZ C ON C.PMCODE = B.MATKL
                                    JOIN WZ_DW D ON D.DW_CODE = A.BWKEY
                                    WHERE A.DLDATE >= '" + stedate + "' ; commit; end");
                        Result= m_Conn.ExecuteSql(strBuilder.ToString());
                    }
                    strBuilder.Length = 0;//清空
                }
                else {
                    //是第一次转换，先删除操作 ，再insert
                    //  插入CONVERT_ZWKC表模型
                    string strSqlEkko = @" begin INSERT INTO  CONVERT_ZWKC
                                                 (MANDT,MATNR,BWKEY,BWTAR,BWKEY_NAME,SALK3,DLCODE,DLNAME,ZLCODE,ZLNAME,XLCODE,XLNAME,MATKL,PMNAME)
                                    SELECT A.BWKEY,D.DW_NAME,A.SALK3,C.DLCODE,C.DLNAME,C.ZLCODE,C.ZLNAME,C.XLCODE,C.XLNAME,B.MATKL,C.PMNAME
                                    FROM MBEW A 
                                    JOIN MARA B ON A.MATNR=B.MATNR
                                    JOIN WZ_WLZ C ON C.PMCODE=B.MATKL
                                    JOIN WZ_DW D ON D.DW_CODE=A.BWKEY 
                                    WHERE A.DLDATE>='" + stedate + "' ; commit;end ;";
                    Result = m_Conn.ExecuteSql(strSqlEkko);
                }
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("1", "CONVERT_ZWKC", "ALL", p_para.Sap_AEDAT, "插入CONVERT_ZWKC表过程中发生异常:\t\n" + exception);
                return Result;
            }
            return Result;
        }
    }
}
