using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：ClsZP10PSIF013_PRPS数据下载
    /// 编写时间：20141014
    /// 编写人：孙冰
    /// </summary>
    public class ClsHB_PRPS : ISAPInterface
    {
        #region =====变量

        /// <summary>
        /// 参数数据
        /// </summary>
        private ClsSAPDataParameter m_para = null;

        /// <summary>
        /// 存储sql字符串变量
        /// </summary>
        private StringBuilder strBuilder = new StringBuilder();

        #endregion

        #region ====== 接口数据获取

        bool ISAPInterface.GetSAPData(ClsSAPDataParameter p_para)
        {
            bool Result = true;
            m_para = p_para;

            try
            {
                
            }
            catch (Exception exception)
            {
                Result = false;
                ClsErrorLogInfo.WriteSapLog("0", "ZP10PSIF013_PRPS", "ALL", m_para.Sap_AEDAT, "异步连接接口发生异常:" + exception);
            }

            return Result;
        }
      
        #endregion
    }
}
