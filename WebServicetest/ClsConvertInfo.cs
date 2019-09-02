using System;
using System.Collections.Generic;
using System.Text;

namespace WebServicetest
{
    public static class ClsConvertInfo
    {
        /// <summary>
        /// 昨日、今日 1昨日 0今日
        /// </summary>
        public static string ToDay { get; set; }

        /// <summary>
        /// 时间间隔 天为单位整数
        /// </summary>
        public static string Interval { get; set; }


        /// <summary>
        /// 上次下载时间
        /// </summary>
        public static string LastDateTime { get; set; }

        /// <summary>
        /// 下次下载时间
        /// </summary>
        public static string NextDateTime { get; set; }

        /// <summary>
        /// 账务库存分析
        /// </summary>
        public static string ZWKC { get; set; }
        public static string ZWKCDate { get; set; }
       
        /// <summary>
        /// 实物库存分析
        /// </summary>
        public static string SWKC { get; set; }
        public static string SWKCDate { get; set; }
   
        /// <summary>
        /// 出库情况分析
        /// </summary>
        public static string CK { get; set; }
        public static string CKDate { get; set; }
   
        /// <summary>
        /// 入库情况分析
        /// </summary>
        public static string RK { get; set; }
        public static string RKDate { get; set; }
        /// <summary>
        /// 出库金额分析
        /// </summary>
        public static string CKJE { get; set; }
        public static string CKJEDate { get; set; }

        /// <summary>
        /// 入库金额分析
        /// </summary>
        public static string RKJE { get; set; }
        public static string RKJEDate { get; set; }

        /// <summary>
        /// 保管员工作量分析
        /// </summary>
        public static string BGY { get; set; }
        public static string BGYDate { get; set; }


        /// <summary>
        /// 是否下载
        /// </summary>
        public static bool IsDownLoad { get; set; }
    }
}
