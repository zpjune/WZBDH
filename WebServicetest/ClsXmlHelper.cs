using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using LHSM.HB.ObjSapForRemoting;
using LHSM.Logs;

namespace WebServicetest
{
    public class ClsXmlHelper
    {

        #region XML操作Ftp节点
        /// <summary>
        /// 读取xml
        /// </summary>
        public static void ReadXmlFtp()
        {
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("LHSMTimerJobConfig.xml");

                #region 读取FtpInfo节点并赋值
                XmlNodeList xmlNodes = xmlDoc.SelectSingleNode("JobConfig/FtpInfo").ChildNodes;

                foreach (XmlNode xmlEle in xmlNodes)
                {
                    switch (xmlEle.Name.Trim())
                    {
                        case "Interval":
                            ClsFtpInfo.Interval = xmlEle.InnerText.Trim();
                            break;
                        case "LastLoadDate":
                            ClsFtpInfo.LastLoadDate = xmlEle.InnerText.Trim();
                            break;
                        case "LastLoadTime":
                            ClsFtpInfo.LastLoadTime = xmlEle.InnerText.Trim();
                            break;
                        case "ServerIP":
                            ClsFtpInfo.ServerIP = xmlEle.InnerText.Trim();
                            break;
                        case "UserName":
                            ClsFtpInfo.UserName = xmlEle.InnerText.Trim();
                            break;
                        case "PassWord":
                            ClsFtpInfo.PassWord = xmlEle.InnerText.Trim();
                            break;
                        case "IsDownLoad":
                            ClsFtpInfo.IsDownLoad = Convert.ToBoolean(xmlEle.InnerText.Trim());
                            break;
                        case "LocalFilePath":
                            ClsFtpInfo.LocalFilePath = xmlEle.InnerText.Trim();
                            break;
                    }

                }

                #endregion

            }
            catch (Exception exception)
            {
                ClsErrorLogInfo.WriteSapLog("2", "", "xml", System.DateTime.Now.ToString(), "xml读取配置文件发生异常:" + exception);
            }
        }

        /// <summary>
        /// 修改Ftp信息
        /// </summary>
        /// <returns></returns>
        public static bool UpdateXmlFtp()
        {
            bool ret = true;
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("LHSMTimerJobConfig.xml");
                XmlNode xmlNode = xmlDoc.SelectSingleNode("JobConfig/FtpInfo");
                if (xmlNode != null)
                {
                    xmlNode.SelectSingleNode("Interval").InnerText = ClsFtpInfo.Interval;
                    xmlNode.SelectSingleNode("LastLoadDate").InnerText = ClsFtpInfo.LastLoadDate;
                    xmlNode.SelectSingleNode("LastLoadTime").InnerText = ClsFtpInfo.LastLoadTime;
                    xmlNode.SelectSingleNode("ServerIP").InnerText = ClsFtpInfo.ServerIP;
                    xmlNode.SelectSingleNode("UserName").InnerText = ClsFtpInfo.UserName;
                    xmlNode.SelectSingleNode("PassWord").InnerText = ClsFtpInfo.PassWord;
                    xmlNode.SelectSingleNode("LocalFilePath").InnerText = ClsFtpInfo.LocalFilePath;
                    xmlNode.SelectSingleNode("IsDownLoad").InnerText = ClsFtpInfo.IsDownLoad.ToString();
                }
                xmlDoc.Save("LHSMTimerJobConfig.xml");
            }
            catch (Exception exception)
            {
                ret = false;
                ClsErrorLogInfo.WriteSapLog("2", "", "xml", System.DateTime.Now.ToString(), "xml保存Ftp配置信息发生异常:" + exception);
                throw exception;
            }
            return ret;
        }

        #endregion      

        #region XML操作读取Txt文本节点
        /// <summary>
        /// 读取xml
        /// </summary>
        public static void ReadXmlTxt()
        {
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("LHSMTimerJobConfig.xml");

                #region 读取ReadTxt节点并赋值
                XmlNodeList xmlNodes = xmlDoc.SelectSingleNode("JobConfig/ReadTxt").ChildNodes;

                foreach (XmlNode xmlEle in xmlNodes)
                {
                    switch (xmlEle.Name.Trim())
                    {
                        case "Interval":
                            ClsReadTxtInfo.Interval =  Specialcharacter(xmlEle.InnerText.Trim(),true);
                            break;
                        case "LoadDateTime":
                            ClsReadTxtInfo.LoadDateTime = Specialcharacter(xmlEle.InnerText.Trim(),true);
                            break;
                        case "LastDateTime":
                            ClsReadTxtInfo.LastDateTime = Specialcharacter(xmlEle.InnerText.Trim(),true);
                            break;
                        case "LocalFilePath":
                            ClsReadTxtInfo.LocalFilePath = Specialcharacter(xmlEle.InnerText.Trim(),true);
                            break;
                        case "IsDownLoad":
                            ClsReadTxtInfo.IsDownLoad = Convert.ToBoolean(Specialcharacter(xmlEle.InnerText.Trim(),true));
                            break;
                        case "Separator":
                            ClsReadTxtInfo.Separator = Specialcharacter(xmlEle.InnerText.Trim(),true);;
                            break;    
                    }

                }

                #endregion

            }
            catch (Exception exception)
            {
                ClsErrorLogInfo.WriteSapLog("2", "", "xml", System.DateTime.Now.ToString(), "xml读取配置文件发生异常:" + exception);
            }
        }

        /// <summary>
        /// 修改txt信息
        /// </summary>
        /// <returns></returns>
        public static bool UpdateXmlTxt(string flag)
        {
            bool ret = true;
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("LHSMTimerJobConfig.xml");
                XmlNode xmlNode = xmlDoc.SelectSingleNode("JobConfig/ReadTxt");
                if (xmlNode != null)
                {
                    if (flag == "cb") //修改是否自动下载
                    {
                        xmlNode.SelectSingleNode("IsDownLoad").InnerText = Specialcharacter(ClsReadTxtInfo.IsDownLoad.ToString(), false);
                    }
                    else if (flag == "ld") //修改上次执行时间
                    {
                        xmlNode.SelectSingleNode("LastDateTime").InnerText = Specialcharacter(ClsReadTxtInfo.LastDateTime, false);
                    }
                    else
                    {
                        xmlNode.SelectSingleNode("Interval").InnerText = Specialcharacter(ClsReadTxtInfo.Interval,false);
                        xmlNode.SelectSingleNode("LoadDateTime").InnerText = Specialcharacter(ClsReadTxtInfo.LoadDateTime, false);
                        xmlNode.SelectSingleNode("LocalFilePath").InnerText = Specialcharacter(ClsReadTxtInfo.LocalFilePath, false);
                        xmlNode.SelectSingleNode("Separator").InnerText = Specialcharacter(ClsReadTxtInfo.Separator.ToString(), false);
                    }
                }
                xmlDoc.Save("LHSMTimerJobConfig.xml");
            }
            catch (Exception exception)
            {
                ret = false;
                ClsErrorLogInfo.WriteSapLog("2", "", "xml", System.DateTime.Now.ToString(), "xml保存Txt配置信息发生异常:" + exception);
                throw exception;
            }
            return ret;
        }

        #endregion      

        #region XML操作读取转化节点
        /// <summary>
        /// 读取xml
        /// </summary>
        public static void ReadXmlConvert()
        {
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("LHSMTimerJobConfig.xml");

                #region 读取ReadTxt节点并赋值
                XmlNodeList xmlNodes = xmlDoc.SelectSingleNode("JobConfig/Convent").ChildNodes;

                foreach (XmlNode xmlEle in xmlNodes)
                {
                    switch (xmlEle.Name.Trim())
                    {
                        case "ToDay":
                            ClsConvertInfo.ToDay = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "Interval":
                            ClsConvertInfo.Interval = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;                       
                        case "LastDateTime":
                            ClsConvertInfo.LastDateTime = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "ZWKC":
                            ClsConvertInfo.ZWKC = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "ZWKCDate":
                            ClsConvertInfo.ZWKCDate = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "SWKC":
                            ClsConvertInfo.SWKC = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "SWKCDate":
                            ClsConvertInfo.SWKCDate = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "RK":
                            ClsConvertInfo.RK = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "RKDate":
                            ClsConvertInfo.RKDate = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "CK":
                            ClsConvertInfo.CK = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "CKDate":
                            ClsConvertInfo.CKDate = Specialcharacter(xmlEle.InnerText.Trim(), true); 
                            break;
                        case "RKJE":
                            ClsConvertInfo.RKJE = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "RKJEDate":
                            ClsConvertInfo.RKJEDate = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "CKJE":
                            ClsConvertInfo.CKJE = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "CKJEDate":
                            ClsConvertInfo.CKJEDate = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "BGY":
                            ClsConvertInfo.BGY = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "BGYDate":
                            ClsConvertInfo.BGYDate = Specialcharacter(xmlEle.InnerText.Trim(), true);
                            break;
                        case "IsDownLoad":
                            ClsConvertInfo.IsDownLoad = Convert.ToBoolean(Specialcharacter(xmlEle.InnerText.Trim(), true));
                            break;
                    }

                }

                #endregion
            }
            catch (Exception exception)
            {
                ClsErrorLogInfo.WriteSapLog("2", "", "xml", System.DateTime.Now.ToString(), "xml读取配置文件发生异常:" + exception);
            }
        }

        /// <summary>
        /// 修改Ftp信息
        /// </summary>
        /// <returns></returns>
        public static bool UpdateXmlConvert(string flag)
        {
            bool ret = true;
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load("LHSMTimerJobConfig.xml");
                XmlNode xmlNode = xmlDoc.SelectSingleNode("JobConfig/Convent");
                if (xmlNode != null)
                {
                    if (flag == "cb") //修改是否自动下载
                    {
                        xmlNode.SelectSingleNode("IsDownLoad").InnerText = Specialcharacter(ClsConvertInfo.IsDownLoad.ToString(), false);
                    }
                    else if (flag == "ld") //修改上次执行时间
                    {
                        xmlNode.SelectSingleNode("LastDateTime").InnerText = Specialcharacter(ClsConvertInfo.LastDateTime, false);
                    }
                    else
                    {
                        xmlNode.SelectSingleNode("ToDay").InnerText = Specialcharacter(ClsConvertInfo.ToDay, false);
                        xmlNode.SelectSingleNode("Interval").InnerText = Specialcharacter(ClsConvertInfo.Interval, false);
                        xmlNode.SelectSingleNode("ZWKC").InnerText = Specialcharacter(ClsConvertInfo.ZWKC, false);
                        xmlNode.SelectSingleNode("ZWKCDate").InnerText = Specialcharacter(ClsConvertInfo.ZWKCDate, false);
                        xmlNode.SelectSingleNode("SWKC").InnerText = Specialcharacter(ClsConvertInfo.SWKC.ToString(), false);
                        xmlNode.SelectSingleNode("SWKCDate").InnerText = Specialcharacter(ClsConvertInfo.SWKCDate, false);
                        xmlNode.SelectSingleNode("RK").InnerText = Specialcharacter(ClsConvertInfo.RK, false);
                        xmlNode.SelectSingleNode("RKDate").InnerText = Specialcharacter(ClsConvertInfo.RKDate, false);
                        xmlNode.SelectSingleNode("CK").InnerText = Specialcharacter(ClsConvertInfo.CK.ToString(), false);
                        xmlNode.SelectSingleNode("CKDate").InnerText = Specialcharacter(ClsConvertInfo.CKDate, false);

                        xmlNode.SelectSingleNode("RKJE").InnerText = Specialcharacter(ClsConvertInfo.RKJE, false);
                        xmlNode.SelectSingleNode("RKJEDate").InnerText = Specialcharacter(ClsConvertInfo.RKJEDate, false);
                        xmlNode.SelectSingleNode("CKJE").InnerText = Specialcharacter(ClsConvertInfo.CKJE.ToString(), false);
                        xmlNode.SelectSingleNode("CKJEDate").InnerText = Specialcharacter(ClsConvertInfo.CKJEDate, false);
                        xmlNode.SelectSingleNode("BGY").InnerText = Specialcharacter(ClsConvertInfo.BGY.ToString(), false);
                        xmlNode.SelectSingleNode("BGYDate").InnerText = Specialcharacter(ClsConvertInfo.BGYDate, false);
                    }
                }
                xmlDoc.Save("LHSMTimerJobConfig.xml");
            }
            catch (Exception exception)
            {
                ret = false;
                ClsErrorLogInfo.WriteSapLog("2", "", "xml", System.DateTime.Now.ToString(), "xml保存Txt配置信息发生异常:" + exception);
                throw exception;
            }
            return ret;
        }

        #endregion      

        /// <summary>
        /// xml中特殊字符的转换
        /// </summary>
        /// <param name="Character"></param>
        /// <param name="IsBack"></param>
        /// <returns></returns>
        private static string Specialcharacter(string Character, bool IsBack)
        {
            string ret = Character;
            if (IsBack)
            {
                if (Character.Contains("&amp;"))
                {
                    ret = Character.Replace("&amp;", "&");
                }

                if (Character.Contains("&lt;"))
                {
                    ret = Character.Replace("&lt;", "<");
                }

                if (Character.Contains("&gt;"))
                {
                    ret = Character.Replace("&gt;", ">");
                }

                if (Character.Contains("&quot;"))
                {
                    ret = Character.Replace("&quot;", "\"");
                }

                if (Character.Contains("&apos;"))
                {
                    ret = Character.Replace("&apos;", "'");
                }
            }
            else
            {
                if (Character.Contains("&"))
                {
                    ret = Character.Replace("&", "&amp;");
                }

                if (Character.Contains("<"))
                {
                    ret = Character.Replace("<", "&lt;");
                }

                if (Character.Contains(">"))
                {
                    ret = Character.Replace(">", "&gt;");
                }

                if (Character.Contains("\""))
                {
                    ret = Character.Replace("\"", "&quot;");
                }

                if (Character.Contains("'"))
                {
                    ret = Character.Replace("'", "&apos;");
                }
            }
            return ret;
        }
    }
}
