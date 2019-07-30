using System;
using System.Collections.Generic;
using System.Text;

namespace WebServicetest
{
    /// <summary>
    /// FTP获取数据配置信息
    /// </summary>
    public static class ClsFtpInfo
    {
        /// <summary>
        /// 时间间隔 天为单位
        /// </summary>
        public static string Interval { get; set; }

        /// <summary>
        /// 上次下载日期
        /// </summary>
        public static string LastLoadDate { get; set; }

        /// <summary>
        /// 上次下载时间
        /// </summary>
        public static string LastLoadTime { get; set; }

        /// <summary>
        /// FTP服务器IP地址
        /// </summary>
        public static string ServerIP { get; set; }

        /// <summary>
        /// FTP服务器登录名
        /// </summary>
        public static string UserName { get; set; }

        /// <summary>
        ///  FTP服务器登录密码
        /// </summary>
        public static string PassWord { get; set; }
        

        /// <summary>
        /// 保存到本地路径
        /// </summary>
        public static string LocalFilePath { get; set; }

        /// <summary>
        /// 是否下载
        /// </summary>
        public static bool IsDownLoad { get; set; }

        

    }
}
