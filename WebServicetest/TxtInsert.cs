using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows.Forms;

namespace WebServicetest
{
    public partial class TxtInsert : Form
    {
        public TxtInsert()
        {
            InitializeComponent();
        }

        private void btReadFile_Click(object sender, EventArgs e)
        {
            OpenFileDialog ofd = new OpenFileDialog();
            ofd.Filter = "Txt文件|*.txt";
            ofd.ValidateNames = true;
            ofd.CheckPathExists = true;
            ofd.CheckFileExists = true;
            if (ofd.ShowDialog() == DialogResult.OK)
            {
                this.tbReadHandFile.Text = ofd.FileName;
            }
        }

        private void btSaveFileName_Click(object sender, EventArgs e)
        {
            OpenFileDialog ofd = new OpenFileDialog();
            ofd.Filter = "Txt文件|*.txt";
            ofd.ValidateNames = true;
            ofd.CheckPathExists = true;
            ofd.CheckFileExists = true;
            if (ofd.ShowDialog() == DialogResult.OK)
            {
                this.tbFileName.Text = ofd.FileName;
            }
        }

        private void btReadHand_Click(object sender, EventArgs e)
        {
            string TableName = this.tbReadTableName.Text;

            List<string> txt = ReadTxtLine(this.tbReadHandFile.Text);

            if (txt != null && txt.Count > 1)
            {
                string[] colList = Regex.Split(txt[0], this.tbFGF.Text.Trim(), RegexOptions.IgnoreCase);
                string str_insert = "INSERT INTO " + TableName.ToUpper() + " ( ";

                string str_del = "DELETE FROM " + TableName.ToUpper();

                string[] colPKList = this.tbReadPK.Text.Split('|');

                Dictionary<string, int> colPKDel = new Dictionary<string, int>();

                string strcol = string.Empty;
                for (int col = 0; col < colList.Length; col++)
                {
                    strcol += colList[col].ToUpper() + ",";
                    foreach (string colpk in colPKList)
                    {
                        if (colList[col].ToUpper() == colpk.ToUpper())
                        {
                            colPKDel.Add(colList[col].ToUpper(), col);
                            break;
                        }
                    }
                }
                str_insert += strcol + "DLDATE) values";


                StringBuilder sbsql = new StringBuilder();

                for (int row = 1, startdba = 0; row < txt.Count; row++)
                {
                    int frist = txt[row].IndexOf(this.tbFGF.Text.Trim());

                    string strtxt = txt[row].Replace("'", "''"); //替换特殊字符
                    string[] coldata = Regex.Split(strtxt, this.tbFGF.Text.Trim(), RegexOptions.IgnoreCase);

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
                                sbsql.Append(item.Key + "='" + coldata[colPosition] + "'; \r\n");
                            }

                        }
                    }

                    //插入sql
                    string colTemp = string.Empty;
                    for (int i = 0; i < coldata.Length; i++)
                    {
                        if (frist == 0 && i == 0)
                        {
                            continue;
                        }
                        colTemp += "'" + coldata[i] + "',";
                    }
                    colTemp = colTemp + DateTime.Now.ToString("yyyyMMdd");
                    sbsql.Append(str_insert + " ( " + colTemp + " ); \r\n");

                    startdba++;
                    if (row != 0 && row % 10000 == 0)
                    {
                        sbsql.Append(" commit; \r\n");
                        TxtAppent(sbsql.ToString());
                        sbsql.Length = 0;
                       
                    }
                }

                if (sbsql.Length > 0)
                {
                    sbsql.Append(" commit; \r\n");
                    TxtAppent(sbsql.ToString());                    
                }

                label4.Text = "转化插入SQL完成！";
            }
        }

        private List<string> ReadTxtLine(string filePath)
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

        private void TxtAppent(string strTxt)
        {
            string path = this.tbFileName.Text;//文件的路径，保证文件存在。
            FileStream fs = new FileStream(path, FileMode.Append);
            StreamWriter sw = new StreamWriter(fs);
            sw.WriteLine(strTxt);
            sw.Close();
            fs.Close();
        }
    }
}
