using System;
using System.Collections.Generic;
using System.Text;

namespace WebServicetest
{
    public static class ClsReadTxtInfo
    {
        /// <summary>
        /// 时间间隔 天为单位整数
        /// </summary>
        public static string Interval { get; set; }


        /// <summary>
        /// 下载时间
        /// </summary>
        public static string LoadDateTime { get; set; }


        /// <summary>
        /// 上次下载时间
        /// </summary>
        public static string LastDateTime { get; set; }

        /// <summary>
        /// 下次下载时间
        /// </summary>
        public static string NextDateTime { get; set; }


        /// <summary>
        /// 读取本地路径
        /// </summary>
        public static string LocalFilePath { get; set; }

        /// <summary>
        /// 是否下载
        /// </summary>
        public static bool IsDownLoad { get; set; }

        /// <summary>
        /// 分隔符
        /// </summary>
        public static string Separator { get; set; }
    }
}
