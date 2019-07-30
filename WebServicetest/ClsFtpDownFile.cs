﻿using LHSM.HB.ObjSapForRemoting;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net;
using System.Text;

namespace WebServicetest
{
    public class ClsFtpDownFile
    {
        /// <summary>
        /// 下载
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="fileName"></param>
        public void Download(string fileName)
        {
            FtpWebRequest reqFTP;
            string serverIP = ClsFtpInfo.ServerIP;
            string userName = ClsFtpInfo.UserName;
            string password = ClsFtpInfo.PassWord;
            string url = "ftp://" + serverIP + "/" + Path.GetFileName(fileName);
            try
            {
                FileStream outputStream = new FileStream(ClsFtpInfo.LocalFilePath + "//" + fileName, FileMode.Create);

                reqFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri(url));
                reqFTP.Method = WebRequestMethods.Ftp.DownloadFile;
                reqFTP.UseBinary = true;
                reqFTP.Credentials = new NetworkCredential(userName, password);
                FtpWebResponse response = (FtpWebResponse)reqFTP.GetResponse();
                Stream ftpStream = response.GetResponseStream();
                long cl = response.ContentLength;
                int bufferSize = 2048;
                int readCount;
                byte[] buffer = new byte[bufferSize];

                readCount = ftpStream.Read(buffer, 0, bufferSize);
                while (readCount > 0)
                {
                    outputStream.Write(buffer, 0, readCount);
                    readCount = ftpStream.Read(buffer, 0, bufferSize);
                }

                ftpStream.Close();
                outputStream.Close();
                response.Close();
            }
            catch (Exception ex)
            {
                ClsErrorLogInfo.WriteSapLog("2", "", "Ftp", System.DateTime.Now.ToString(), "Ftp远程下载文件发生异常:" + ex);
            }
        }

        /// <summary>
        /// 从ftp服务器上获得文件列表
        /// </summary>
        /// <param name="RequedstPath">服务器下的相对路径</param>
        /// <returns></returns>
        public static List<string> GetFile(DataTable fileNameDt,DateTime dtLastDownLoadDate)
        {
            List<string> strs = new List<string>();
            try
            {
                string serverIP = ClsFtpInfo.ServerIP;
                string userName = ClsFtpInfo.UserName;
                string password = ClsFtpInfo.PassWord;
               
                string uri = "ftp://" + serverIP ;
                FtpWebRequest reqFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri(uri));
                // ftp用户名和密码
                reqFTP.Credentials = new NetworkCredential(userName, password);
                reqFTP.Method = WebRequestMethods.Ftp.ListDirectoryDetails;
                WebResponse response = reqFTP.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream());//中文文件名
                DateTime txtDate ;
                string line = reader.ReadLine();
                while (line != null)
                {
                    if (!line.Contains("<DIR>"))
                    {
                        string msg = line.Substring(39).Trim();
                        string[] arr = msg.Split('_');
                        txtDate = Convert.ToDateTime(arr[1].Substring(0, 4) + "-" + arr[1].Substring(4, 2) + "-" + arr[1].Substring(6, 2));
                        if (fileNameDt.Select("TXT_TABLENAME='"+arr[0]+"'").Length>0&& txtDate >= dtLastDownLoadDate) {
                            strs.Add(msg);
                        }
                    }
                    line = reader.ReadLine();
                }
                reader.Close();
                response.Close();
                return strs;
            }
            catch (Exception ex)
            {
                ClsErrorLogInfo.WriteSapLog("2", "", "Ftp", System.DateTime.Now.ToString(), "Ftp远程获取文件列表出错:" + ex);
            }
            return strs;
        }
        public int DownloadFtp(string filename)
        {
            FtpWebRequest reqFTP;
            string serverIP = ClsFtpInfo.ServerIP;
            string userName = ClsFtpInfo.UserName;
            string password = ClsFtpInfo.PassWord;
            string url = "ftp://" + serverIP + "/" + Path.GetFileName(filename);

            try
            {
                FileStream outputStream = new FileStream(filename, FileMode.Create);
                reqFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri(url));
                reqFTP.Method = WebRequestMethods.Ftp.DownloadFile;
                reqFTP.UseBinary = true;
                reqFTP.KeepAlive = false;
                reqFTP.Credentials = new NetworkCredential(userName, password);
                FtpWebResponse response = (FtpWebResponse)reqFTP.GetResponse();
                Stream ftpStream = response.GetResponseStream();
                long cl = response.ContentLength;
                int bufferSize = 2048;
                int readCount;
                byte[] buffer = new byte[bufferSize];
                readCount = ftpStream.Read(buffer, 0, bufferSize);
                while (readCount > 0)
                {
                    outputStream.Write(buffer, 0, readCount);
                    readCount = ftpStream.Read(buffer, 0, bufferSize);
                }
                ftpStream.Close();
                outputStream.Close();
                response.Close();
                return 0;
            }
            catch (Exception ex)
            {                
                ClsErrorLogInfo.WriteSapLog("2", "", "Ftp", System.DateTime.Now.ToString(), "Ftp远程下载文件发生异常:" + ex);
                return -2;
            }
            finally {
                serverIP = string.Empty;
                userName = string.Empty;
                password = string.Empty;
                url = string.Empty;
            }
        }
        

    }
}
