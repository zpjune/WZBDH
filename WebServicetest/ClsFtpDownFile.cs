using LHSM.HB.ObjSapForRemoting;
using System;
using System.Collections.Generic;
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
