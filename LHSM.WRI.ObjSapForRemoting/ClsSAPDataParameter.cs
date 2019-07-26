using System;
using System.Collections.Generic;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：数据下载参数类
    /// 编写时间：20141013
    /// 编写人：孙冰
    /// </summary>
    public class ClsSAPDataParameter
    {
        #region =======字段

        /// <summary>
        /// 连接SAP的字符串
        /// </summary>
        private string p_Sap_Conn = "";

        /// <summary>
        /// 创建时间
        /// </summary>
        private string p_Sap_AEDAT = "";

        /// <summary>
        /// 单位
        /// </summary>
        private string p_Sap_BUKRS = "";

        /// <summary>
        /// 选择数据接口
        /// </summary>
        private string[] p_Sap_Select = null;

        #endregion

        #region =======属性

        /// <summary>
        /// 连接SAP的字符串
        /// </summary>
        public string Sap_Conn
        {
            get
            {
                return p_Sap_Conn;
            }
            set
            {
                p_Sap_Conn = value;
            }
        }

        /// <summary>
        /// 创建时间
        /// </summary>
        public string Sap_AEDAT
        {
            get
            {
                return p_Sap_AEDAT;
            }
            set
            {
                p_Sap_AEDAT = value;
            }
        }

        /// <summary>
        /// 创建时间
        /// </summary>
        public string Sap_BUKRS
        {
            get
            {
                return p_Sap_BUKRS;
            }
            set
            {
                p_Sap_BUKRS = value;
            }
        }

        /// <summary>
        /// 选择数据接口
        /// </summary>
        public string[] Sap_Select
        {
            get
            {
                return p_Sap_Select;
            }
            set
            {
                p_Sap_Select = value;
            }
        }

        #endregion
    }
}
