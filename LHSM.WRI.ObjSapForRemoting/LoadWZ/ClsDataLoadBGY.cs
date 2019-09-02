using LHSM.DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
    public class ClsDataLoadBGY : ISAPLoadInterface
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
            try
            {
                string dldate = p_para.Sap_AEDAT;
                m_Conn = ClsUtility.GetConn();
                strBuilder.Append("BEGIN  ");
                strBuilder.Append(" DELETE FROM CONVERT_BGYGZL WHERE ERDAT>='"+ dldate + "' ;");//先按选择日期删除数据
                strBuilder.Append("  COMMIT ;END;");
                if (m_Conn.ExecuteSql(strBuilder.ToString()))
                {
                    strBuilder.Length = 0;
                    strBuilder.Append(" begin ");
                    strBuilder.Append(@" INSERT INTO CONVERT_BGYGZL(WERKS,WERKS_NAME,TZD,ITEMS,MATNR,MATKL,PMNAME,MAKTX,
                                        JBJLDW,NSOLM,ERNAME,WORKER_NAME,TZDTYPE,DLDATE,ERDAT)
                                        select A.WERKS,D.DW_NAME,A.ZDHTZD,A.ZITEM,A.MATNR,E.MATKL,F.PMNAME,E.MAKTX,
                                        F.JBJLDW,B.NSOLM,B.ERNAM,C.WORKER_NAME,1,'" + dldate + "',ERDAT");
                    strBuilder.Append(@" from ZC10MMDG072 A
                                        join ZC10MMDG085A B on A.ZDHTZD=B.ZDHTZD and A.ZITEM=B.ZITEM 
                                        join WZ_BGY C on  C.WORKER_CODE=B.ERNAM
                                        join WZ_DW D ON D.DW_CODE=A.WERKS
                                        JOIN MARA E ON E.MATNR=A.MATNR
                                        JOIN WZ_WLZ F ON F.PMCODE=E.MATKL where B.ERDAT>='" + dldate + "' ;");//根据日期插入入库单相关保管员信息
                    strBuilder.Append(" commit; end; ");
                    if (m_Conn.ExecuteSql(strBuilder.ToString()))
                    {
                        ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_BGYGZL表入库通知单保管员信息成功");
                        strBuilder.Length = 0;
                        strBuilder.Append(" begin ");
                        strBuilder.Append(@" INSERT INTO CONVERT_BGYGZL(WERKS,WERKS_NAME,TZD,ITEMS,MATNR,MATKL,PMNAME,MAKTX,
                                        JBJLDW,NSOLM,ERNAME,WORKER_NAME,TZDTYPE,DLDATE,ERDAT)
                                        select A.WERKS,D.DW_NAME,A.ZCKTZD,A.ZCITEM,A.MATNR,E.MATKL,F.PMNAME,E.MAKTX,
                                        F.JBJLDW,B.NSOLM,B.ERNAM,C.WORKER_NAME,2,'" + dldate + "',ERDAT");
                       strBuilder.Append(@" from ZC10MMDG078 A
                                        join ZC10MMDG085A B on A.ZCKTZD=B.ZCKTZD and A.ZCITEM=B.ZCITEM 
                                        join WZ_BGY C on  C.WORKER_CODE=B.ERNAM
                                        join WZ_DW D ON D.DW_CODE=A.WERKS
                                        JOIN MARA E ON E.MATNR=A.MATNR
                                        JOIN WZ_WLZ F ON F.PMCODE=E.MATKL where B.ERDAT>='" + dldate + "' ;");//根据日期插入出库单相关保管员信息
                        strBuilder.Append(" commit; end; ");
                        if (m_Conn.ExecuteSql(strBuilder.ToString()))
                        {
                            Result = true;
                            ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_BGYGZL表出库通知单保管员信息成功");
                        }
                        else
                        {
                            Result = false;
                            ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_BGYGZL表出库通知单保管员信息失败");
                        }
                    }
                    else
                    {
                        Result = false;
                        ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_BGYGZL表入库通知单保管员信息失败");
                    }
                }
                else {
                    Result = false;
                    ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-删除CONVERT_BGYGZL表失败" );
                }
            }
            catch (Exception ex)
            {
                ClsErrorLogInfo.WriteSapLog("1", "RKJE", "ALL", p_para.Sap_AEDAT, "模型转换-插入CONVERT_BGYGZL表发生异常:" + ex.Message);
                return false;
            }
            return Result;

        }
    }
}
