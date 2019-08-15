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
            return Result;
        }
    }
}
