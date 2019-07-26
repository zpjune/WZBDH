using System;
using System.Collections.Generic;
using System.Text;
using LHSM.DataAccess;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：下载日志
    /// 编写时间：20141014
    /// 编写人：孙冰
    /// </summary>
    public static class ClsLogInfo
    {
        /// <summary>
        /// 写入SAP数据日志
        /// </summary>
        /// <param name="p_Type">日志类型，0.接口下载，1.数据转换</param>
        /// <param name="p_Name">方案编号，接口编号，转化表编号</param>
        /// <param name="p_AEDAT">获取数据的日期</param>
        /// <param name="p_REMARK">备注信息</param>
        public static void WriteSapLog(string p_Type, string p_Name, string p_AEDAT, string p_REMARK)
        {
            //日志条件生成
            string strId = Guid.NewGuid().ToString();
            string strType = p_Type;
            string strDate = System.DateTime.Now.ToString("yyyyMMdd HH:mm:ss");
            string strName = p_Name;
            string strNameCls = ClsUtility.GetClassName(p_Name);
            string strAEDAT = p_AEDAT;
            string strREMARK = p_REMARK;

            //SQL生成
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append(" INSERT INTO LOGINFO ");
            strBuilder.Append(" (LOG_CODE,LOG_TYPE,LOG_DATEGET,PLAN_CODE,PLAN_NAME,LOG_DATE,LOG_REMARK ) ");
            strBuilder.Append(" VALUES (");
            strBuilder.Append(" '" + strId + "',");
            strBuilder.Append(" '" + strType + "',");
            strBuilder.Append(" '" + strDate + "',");
            strBuilder.Append(" '" + strName + "',");
            strBuilder.Append(" '" + strNameCls + "',");
            strBuilder.Append(" '" + strAEDAT + "',");
            strBuilder.Append(" '" + strREMARK + "'");
            strBuilder.Append(" ) ");

            ////执行SQL
            ClsUtility.ExecuteSqlToDb(strBuilder.ToString());
        }

    }
}
