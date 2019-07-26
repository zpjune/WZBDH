using System;
using System.Collections.Generic;
using System.Text;

namespace WebServicetest
{
    public class ClsReadTable
    {
        /// <summary>
        /// 表明
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// 表主键
        /// </summary>
        public string TablePK { get; set; }

        /// <summary>
        /// 是否下载
        /// </summary>
        public bool IsDownLoad { get; set; }
    }
}
