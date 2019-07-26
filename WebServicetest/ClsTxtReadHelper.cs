using LHSM.HB.ObjSapForRemoting;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace WebServicetest
{
    public class ClsTxtReadHelper
    {
        /// <summary>
        /// 线程开始事件
        /// </summary>
        public event EventHandler threadStartEvent;
        /// <summary>
        /// 线程执行时事件
        /// </summary>
        public event EventHandler threadEvent;
        /// <summary>
        /// 线程结束事件
        /// </summary>
        public event EventHandler threadEndEvent;

        public string TableName;
        public string FilePath;
        public DataRow[] dr;

        public List<string> ReadTxtLine(string filePath)
        {
            var file = File.Open(filePath, FileMode.Open);
            List<string> txt = new List<string>();
            using (var stream = new StreamReader(file))
            {
                while (!stream.EndOfStream)
                {
                    txt.Add(stream.ReadLine());
                }
            }
            file.Close();

            return txt;
        }

        public void ReadTxtInsert()
        {
            threadStartEvent.Invoke(new object(), new EventArgs());//线程开始

            string[] infotablename = FilePath.Split('/');
            string infodate = ClsCom.GetDateForArr(infotablename[infotablename.Length - 1]);

            LHSM.Logs.Log.Logger.Error("表"+infotablename[infotablename.Length - 1]+"开始读取！");   

            List<string> txt = ReadTxtLine(FilePath);

            txt = txt.FindAll(d => d != "");//zhangping修改txt文件 去空行

            if (txt != null && txt.Count > 1)
            {
                string[] colList = Regex.Split(txt[0], ClsReadTxtInfo.Separator, RegexOptions.IgnoreCase);
                string str_insert = "INSERT INTO " + TableName.ToUpper() + " ( ";

                string str_del = "DELETE FROM " + TableName.ToUpper();

                string[] colPKList = null;
                Dictionary<string, int> colPKDel = new Dictionary<string, int>();
                if (dr != null && dr.Length > 0)
                {
                    if (dr[0]["TXT_PK"] != null && !string.IsNullOrEmpty(dr[0]["TXT_PK"].ToString()))
                    {
                        if (dr[0]["TXT_PK"].ToString().Contains("|"))
                        {
                            colPKList = dr[0]["TXT_PK"].ToString().Split('|');
                        }
                        else
                        {
                            colPKList = new string[]{dr[0]["TXT_PK"].ToString()};
                        }
                    }
                }

                string strcol = string.Empty;
                for (int col = 0; col < colList.Length; col++)
                {
                    if (colList[col].Contains("/"))//字段带"/"处理
                    {
                         strcol += '"'+colList[col].ToUpper() + '"'+",";
                    }
                    else
                    {
                        strcol += colList[col].ToUpper() + ",";
                    }                 
                    foreach (string colpk in colPKList)
                    {
                        if (colList[col].ToUpper() == colpk.ToUpper())
                        {
                            colPKDel.Add(colList[col].ToUpper(), col);
                            break;
                        }
                    }
                }
                str_insert += strcol+ "DLDATE) values";


                StringBuilder sbsql = new StringBuilder();
                sbsql.Append(" Begin ");
                bool boolinfo = true;

                for (int row = 1,startdba=0; row < txt.Count; row++)
                {
                    int frist = txt[row].IndexOf(ClsReadTxtInfo.Separator);

                    string strtxt = txt[row].Replace("'", "''"); //替换特殊字符
                    string[] coldata = Regex.Split(strtxt, ClsReadTxtInfo.Separator, RegexOptions.IgnoreCase);
                    //删除主键sql
                    if (colPKDel != null && colPKDel.Count > 0)
                    {
                        sbsql.Append(str_del + " WHERE ");
                        int countpk = colPKDel.Count;
                        foreach (KeyValuePair<string, int> item in colPKDel)
                        {
                            countpk--;
                            int colPosition = item.Value;
                            if (frist == 0)
                            {
                                colPosition++;
                            }

                            if (countpk > 0)
                            {
                                sbsql.Append(item.Key + "='" + coldata[colPosition] + "' AND ");
                            }
                            else
                            {
                                sbsql.Append(item.Key + "='" + coldata[colPosition] + "';");
                            }

                        }
                    }

                    //插入sql
                    string colTemp = string.Empty;
                    for (int i = 0; i < coldata.Length; i++)
                    {
                        if (frist==0&&i==0)
                        {
                            continue;
                        }   
                        if (coldata[i].Contains("-"))
                        {
                             colTemp += "'" + ClsCom.GetNumberForArr(coldata[i]) + "',";//对于SAP中的491.00-负数形式做处理
                        }
                        else{
                             colTemp += "'" + coldata[i] + "',";
                        }
                       
                    }
                    colTemp = colTemp+DateTime.Now.ToString("yyyyMMdd");
                    sbsql.Append(str_insert + " ( " + colTemp + " ); ");

                    startdba++;
                    if (startdba*coldata.Length>=50000)
                    {
                         startdba = 0;
                         sbsql.Append(" end; ");

                         //LHSM.Logs.Log.Logger.Error("sql:" + sbsql.ToString());   
                         bool retfp = ClsUtility.ExecuteSqlToDb(sbsql.ToString());
                         if (!retfp)
                         {
                             boolinfo = false;
                             ClsErrorLogInfo.WriteSapLog("0", "downLoad", TableName.ToUpper(), DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "读取" + TableName.ToUpper() + "表分批插入发生异常:\t\n");
                             ClsLogInfo.WriteSapLog("0", "downLoad", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "读取" + TableName.ToUpper() + "表分批插入失败！\t\n");
                         }

                         sbsql.Length = 0;
                         sbsql.Append(" Begin ");
                    }
                }

                sbsql.Append(" end; ");

                if (sbsql.Length > 14)
                {
                    //LHSM.Logs.Log.Logger.Error("sql:" + sbsql.ToString());   
                    bool ret = ClsUtility.ExecuteSqlToDb(sbsql.ToString());
                    if (!ret)
                    {
                        boolinfo = false;
                        ClsErrorLogInfo.WriteSapLog("0", "downLoad", TableName.ToUpper(), DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "读取" + TableName.ToUpper() + "表发生异常:\t\n");
                        ClsLogInfo.WriteSapLog("0", "downLoad", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "读取" + TableName.ToUpper() + "表失败！\t\n");
                    }
                    else
                    {
                        ClsLogInfo.WriteSapLog("0", "downLoad", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "读取" + TableName.ToUpper() + "表成功！\t\n");
                    }
                }
                else
                {
                    ClsLogInfo.WriteSapLog("0", "downLoad", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "读取" + TableName.ToUpper() + "表成功！\t\n");
                }

                if (boolinfo)
                {
                    LHSM.Logs.Log.Logger.Error("表" + infotablename[infotablename.Length - 1] + "读取完成，读取成功！");   
                    string updateSql = "UPDATE hb_txttable SET DLDATE='" + infodate + "' WHERE TXT_TABLENAME='" + TableName.ToUpper() + "'";
                    if (dr[0]["DLDATE"]!=null&&!string.IsNullOrEmpty(dr[0]["DLDATE"].ToString()))
                    {
                        updateSql += " AND DLDATE<'" + infodate + "'";
                    }                   
                    bool ret = ClsUtility.ExecuteSqlToDb(updateSql);

                    threadEndEvent.Invoke(null, new EventArgs());//线程结束
                }
                else
                {
                    LHSM.Logs.Log.Logger.Error("表" + infotablename[infotablename.Length - 1] + "读取失败！");

                    threadEndEvent.Invoke(infotablename[infotablename.Length - 1], new EventArgs());//线程结束
                }

                
            }
        }
    }
}
