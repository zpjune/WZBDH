using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：数据下载SAP接口类
    /// 编写时间：20141013
    /// 编写人：孙冰
    /// </summary>
    public  interface ISAPInterface
    {
        /// <summary>
        /// 从SAP中获取数据并转换成DataSet
        /// </summary>
        /// <param name="p_para">下载参数</param>
        /// <returns>完成true全部成功，false有错误</returns>
        bool GetSAPData(ClsSAPDataParameter p_para);
    }
}
