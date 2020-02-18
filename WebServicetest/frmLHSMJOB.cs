using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Xml;
using System.IO;
using System.Timers;
using System.Runtime.InteropServices;
using LHSM.HB.ObjSapForRemoting;
using LHSM.Logs;
using System.Threading;
using System.Globalization;

namespace WebServicetest
{
    public partial class frmLHSMJOB : Form
    {
        public frmLHSMJOB()
        {
            InitializeComponent();

            this.lbReadTxtInfo.Visible = false;
            this.lbConvInfo.Visible = false;

            InitReadWin();
            InitReadTable(false);
            InitReadInitHand(); //手动下载

            ClsXmlHelper.ReadXmlTxt();
            SetReadTxt();

            ClsXmlHelper.ReadXmlConvert();
            SetConventText();

            ClsXmlHelper.ReadXmlFtp();
            SetFtpInfo();
        }

        #region Ftp操作 暂时不需要
        /// <summary>
        /// 初始化Ftp信息
        /// </summary>
        private void InitFtpInfo()
        {
            ClsXmlHelper.ReadXmlFtp(); //读取xml配置文件信息
            SetFtpInfo(); //设置页面Ftp相关信息
        }

        /// <summary>
        /// 设置页面Ftp相关信息
        /// </summary>
        private void SetFtpInfo()
        {
            #region 执行 页面 FTP 配置展示赋值

            if (ClsFtpInfo.IsDownLoad)
            {
                this.cbFtpIsDown.Checked = true;
            }
            else
            {
                this.cbFtpIsDown.Checked = false;
            }
          

            this.tbFtpIpInfo.Text = ClsFtpInfo.ServerIP;
            this.tbFtpPWInfo.Text = ClsFtpInfo.PassWord;
            this.tbFtpUserNameInfo.Text = ClsFtpInfo.UserName;
            this.tbLocalPath.Text = ClsFtpInfo.LocalFilePath;
            this.tbFtpIntervalInfo.Text = ClsFtpInfo.Interval;
            this.tbFtpLastLoadDateInfo.Text = ClsFtpInfo.LastLoadDate;
            this.tbFtpLastLoadTimeInfo.Text = ClsFtpInfo.LastLoadTime;

           
            
            #endregion

        }

        /// <summary>
        /// 设置页面Ftp相关信息
        /// </summary>
        private void SetFtpClass()
        {
            #region 执行 页面

            ClsFtpInfo.IsDownLoad = this.cbFtpIsDown.Checked;

            #endregion

            #region Ftp下载配置 页面

            ClsFtpInfo.ServerIP=this.tbFtpIpInfo.Text ;
            ClsFtpInfo.PassWord=this.tbFtpPWInfo.Text ;
            ClsFtpInfo.UserName= this.tbFtpUserNameInfo.Text ;
            ClsFtpInfo.LocalFilePath= this.tbLocalPath.Text ;
            ClsFtpInfo.Interval=this.tbFtpIntervalInfo.Text  ;
            ClsFtpInfo.LastLoadDate=this.tbFtpLastLoadDateInfo.Text ;
            ClsFtpInfo.LastLoadTime=this.tbFtpLastLoadTimeInfo.Text ;

            #endregion

        }

        /// <summary>
        /// 保存Ftp下载信息
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btFtpInfoSave_Click(object sender, EventArgs e)
        {
            if (ClsXmlHelper.UpdateXmlFtp())
            {
                SetFtpClass();
                MessageBox.Show("保存Ftp信息成功！");
            }
            else
            {
                MessageBox.Show("保存Ftp信息失败！");
            }
        }

        /// <summary>
        /// 手动下载Ftp文件
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btFtpDownLoad_Click(object sender, EventArgs e)
        {
            try
            {
                if (tbFtpDownDateSD.Text=="") {
                    MessageBox.Show("请设置手动下载日期");
                    return;
                }
                string LastLoadDate = tbFtpDownDateSD.Text;
                string Interval = ClsFtpInfo.Interval;
                // DateTime dtLastLoadDate = Convert.ToDateTime(LastLoadDate.Substring(0, 4) + "-" + LastLoadDate.Substring(4, 2) + "-" + LastLoadDate.Substring(6, 2)).AddDays(double.Parse(Interval));
                DateTime dtLastLoadDate = Convert.ToDateTime(LastLoadDate.Substring(0, 4) + "-" + LastLoadDate.Substring(4, 2) + "-" + LastLoadDate.Substring(6, 2));
                DataTable dtFile = GetDataSource(false);
                List<string> list = ClsFtpDownFile.GetFile(dtFile, dtLastLoadDate);
                if (list.Count > 0)
                {
                    ClsFtpDownFile ftpdown = new ClsFtpDownFile();
                    foreach (string filaname in list)
                    {
                        ftpdown.Download(filaname);
                    }
                    MessageBox.Show("下载完成");
                }
                else {
                    MessageBox.Show("没有符合fpt配置的下载文件");
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("手动下载失败！");
                ClsErrorLogInfo.WriteSapLog("2", "", "Ftp", System.DateTime.Now.ToString(), "Ftp远程手动下载文件失败:" + ex);
            }
            

           
        }

        private void cbFtpIsDown_CheckedChanged(object sender, EventArgs e)
        {
            if (this.cbFtpIsDown.Checked)
            {
                ClsFtpInfo.IsDownLoad = true;
            }
            else
            {
                ClsFtpInfo.IsDownLoad = false;
            }
        }

        #endregion

        #region 修改本地系统时间与数据库服务器时间同步

        /// <summary>
        /// 获取服务器时间
        /// </summary>
        /// <returns></returns>
        private void ServiceDate()
        {
            DateTime dtService = new DateTime();
            try
            {
                string strSql = " SELECT SYSDATE FROM DUAL";
                DataTable dt = ClsUtility.GetSelectTable(strSql);
                dtService = Convert.ToDateTime(dt.Rows[0][0].ToString());
                SyncTime(dtService);
            }
            catch (Exception exception)
            {
                dtService = Convert.ToDateTime("0000-00-00 00:00:00");
                throw exception;
            }
        }

        private struct SystemTime
        {
            public ushort wYear;
            public ushort wMonth;
            public ushort wDayOfWeek;
            public ushort wDay;
            public ushort wHour;
            public ushort wMinute;
            public ushort wSecond;
            public ushort wMilliseconds;
        }

        [DllImport("Kernel32.dll")]
        private static extern bool SetSystemTime(ref SystemTime sysTime);

        /// <summary>
        /// 将本地时间服务器时间同步
        /// </summary>
        /// <param name="currentTime">服务器时间</param>
        public static void SyncTime(DateTime currentTime)
        {
            SystemTime sysTime = new SystemTime();
            sysTime.wYear = Convert.ToUInt16(currentTime.Year);
            sysTime.wMonth = Convert.ToUInt16(currentTime.Month);
            sysTime.wDay = Convert.ToUInt16(currentTime.Day);
            sysTime.wDayOfWeek = Convert.ToUInt16(currentTime.DayOfWeek);
            sysTime.wMinute = Convert.ToUInt16(currentTime.Minute);
            sysTime.wSecond = Convert.ToUInt16(currentTime.Second);
            sysTime.wMilliseconds = Convert.ToUInt16(currentTime.Millisecond);

            //处理北京时间 
            int nBeijingHour = currentTime.Hour - 8;
            if (nBeijingHour <= 0)
            {
                nBeijingHour = 24;
                sysTime.wDay = Convert.ToUInt16(currentTime.Day - 1);
            }
            else
            {
                sysTime.wDay = Convert.ToUInt16(currentTime.Day);
                sysTime.wDayOfWeek = Convert.ToUInt16(currentTime.DayOfWeek);
            }
            sysTime.wHour = Convert.ToUInt16(nBeijingHour);

            SetSystemTime(ref sysTime);//设置本机时间 
        }

        #endregion

        #region 窗体操作 关闭与显示
        /// <summary>
        /// 关闭事件 到托盘
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void frmLHSMJOB_FormClosing(object sender, FormClosingEventArgs e)
        {
            this.WindowState = FormWindowState.Minimized;
            e.Cancel = true;
        }

        /// <summary>
        /// 窗体改变事件
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void frmLHSMJOB_SizeChanged(object sender, EventArgs e)
        {
            if (this.WindowState == FormWindowState.Minimized)
            {
                this.ShowInTaskbar = false; //不显示在系统任务栏 
                notifyIcon1.Visible = true; //托盘图标可见 
            }
        }

        /// <summary>
        /// 双击显示窗体
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void notifyIcon1_DoubleClick(object sender, EventArgs e)
        {
            if (this.WindowState == FormWindowState.Minimized)
            {
                this.Show();
                this.WindowState = FormWindowState.Normal;
                this.ShowInTaskbar = true;
            }
        }

        private void 显示ToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (this.WindowState == FormWindowState.Minimized)
            {
                this.Show();
                this.WindowState = FormWindowState.Normal;
                this.ShowInTaskbar = true;
            }
        }

        private void 退出ToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.notifyIcon1.Visible = false;

            timer.Enabled = false;
            timer.Close();
            timer.Dispose();
            this.Close();
            this.Dispose();
            Application.Exit();
        }
        #endregion

        #region 变量

        //实例化时间Timers
        System.Timers.Timer timer = new System.Timers.Timer();

        //存储读取txt的所有表信息
        DataTable m_dt = null;

        int ThreadTotalCount = 0;  //总共线程的数量
        int ThreadErrorCount = 0;  //错误线程的数量
        int ThreadSucessCount = 0; //成功线程的数量

        string lbText = string.Empty;

        #endregion

        #region 定时执行
        /// <summary>
        /// 开始执行定时
        /// </summary>
        private void startTime()
        {
            try
            {
                //设置执行的时间间隔
                timer.Interval = 1000;

                //到达时间的时候执行事件
                timer.Elapsed += new ElapsedEventHandler(timer_Elapsed);

                //是否是一直执行
                timer.AutoReset = true;

                //是否执行
                timer.Enabled = true;
                LHSM.Logs.Log.Logger.Error("startTime()=====start==========="+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            }
            catch (Exception)
            {
                timer.Enabled = false;

                MessageBox.Show("启动失败！");
                return;
            }
        }

        /// <summary>
        /// 结束执行定时
        /// </summary>
        private void stopTime()
        {
            Log.Logger.Debug("自动下载关闭。");
            try
            {
                timer.Stop();
                timer.Elapsed -= new ElapsedEventHandler(timer_Elapsed);
                timer.Enabled = false;
                LHSM.Logs.Log.Logger.Error("stopTime()=====end===========" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            }
            catch (Exception)
            {
                Log.Logger.Error("自动下载关闭失败！");
                MessageBox.Show("停止失败！");
                return;
            }
        }


        int all = 0;
        int check = 0;
        /// <summary>
        /// 执行时间
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            DateTime dtNow = DateTime.Now;
            string isDownLoadFtpFile = ClsFtpInfo.IsDownLoad.ToString().ToUpper();
            if (isDownLoadFtpFile=="TRUE") {
                string LastLoadDate = ClsFtpInfo.LastLoadDate;
                string Interval = ClsFtpInfo.Interval;
                string LastLoadTime = ClsFtpInfo.LastLoadTime;
                DateTime dtLastLoadDate = Convert.ToDateTime(LastLoadDate.Substring(0, 4) + "-" + LastLoadDate.Substring(4, 2) + "-" + LastLoadDate.Substring(6, 2)+" "+ LastLoadTime).AddDays(double.Parse(Interval));

                if (dtLastLoadDate.ToString("yyyyMMdd HH:mm:ss") == dtNow.ToString("yyyyMMdd HH:mm:ss")) {
                    ClsLogInfo.WriteSapLog("3", "fptzidongxiazai", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "自动下载ftp文件开始\t\n");
                    DateTime _LastLoadDate = Convert.ToDateTime(LastLoadDate.Substring(0, 4) + "-" + LastLoadDate.Substring(4, 2) + "-" + LastLoadDate.Substring(6, 2)).AddDays(double.Parse(Interval));
                    DataTable dtFile = GetDataSource(false);
                    List<string> list = ClsFtpDownFile.GetFile(dtFile, _LastLoadDate);
                    if (list.Count > 0)
                    {
                        ClsFtpDownFile ftpdown = new ClsFtpDownFile();
                        foreach (string filaname in list)
                        {
                            ftpdown.Download(filaname);
                        }
                    }
                    ClsFtpInfo.LastLoadDate = _LastLoadDate.ToString("yyyyMMdd");
                    ClsXmlHelper.UpdateXmlFtp();
                    SetFtpClass();
                    SetFtpInfo();
                    ClsLogInfo.WriteSapLog("3", "fptzidongxiazai", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "自动下载ftp文件结束\t\n");
                    deleteFile(ClsFtpInfo.LocalFilePath);
                }
            }
            if (this.cbReadIsDown.Checked)
            {
                if (ClsReadTxtInfo.NextDateTime == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                {
                    m_dt = GetDataSource(false);
                    if (m_dt != null && m_dt.Rows.Count > 0)
                    {
                        List<string> listtable = new List<string>();
                        for (int i = 0; i < m_dt.Rows.Count; i++)
                        {
                            listtable.Add((m_dt.Rows[i]["TXT_TABLENAME"] ?? string.Empty).ToString().ToUpper());
                        }

                        ReadFileTable(listtable, ClsReadTxtInfo.LocalFilePath);

                            ClsReadTxtInfo.LastDateTime = ClsReadTxtInfo.NextDateTime.Substring(0, 8);

                            //DateTime dtNextDate = Convert.ToDateTime(ClsReadTxtInfo.LastDateTime).AddDays(int.Parse(ClsReadTxtInfo.Interval));
                            DateTime dtNextDate = DateTime.ParseExact(ClsReadTxtInfo.NextDateTime.Substring(0, 8), "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture).AddDays(int.Parse(ClsReadTxtInfo.Interval));
                            ClsReadTxtInfo.NextDateTime = dtNextDate.ToString("yyyyMMdd") + " " + ClsReadTxtInfo.LoadDateTime;


                        ClsXmlHelper.UpdateXmlTxt("ld");
                        ClsXmlHelper.ReadXmlTxt();
                        SetReadTxt();

                    }
                }
            }

            if (this.cbConvIsDown.Checked)
            {
                string strDate = ClsConvertInfo.NextDateTime;
                DateTime p_date = DateTime.ParseExact(strDate, "yyyyMMdd", null); 
                
                if (ClsConvertInfo.ToDay=="1")
                {
                    p_date.AddDays(-1);
                }
                string strAEDAT = p_date.ToString("yyyyMMdd");

                if (Convert.ToBoolean(ClsConvertInfo.ZWKC))
                {
                    if (strDate + " " + ClsConvertInfo.ZWKCDate == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                    {
                        string[] strPara = new string[] { strAEDAT, "" };
                        ClsSapOperate.SapLoadExecute("ZWKC", strPara);
                        all++;
                    }
                    check++;
                }

                if (Convert.ToBoolean(ClsConvertInfo.SWKC))
                {
                    if (strDate + " " + ClsConvertInfo.SWKCDate == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                    {
                        string[] strPara = new string[] { strAEDAT, "" };
                        ClsSapOperate.SapLoadExecute("SWKC", strPara);
                        all++;
                    }
                    check++;
                }

                if (Convert.ToBoolean(ClsConvertInfo.RK))
                {
                    if (strDate + " " + ClsConvertInfo.RKDate == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                    {
                        string[] strPara = new string[] { strAEDAT, "" };
                        ClsSapOperate.SapLoadExecute("RK", strPara);
                        all++;
                    }
                    check++;
                }

                if (Convert.ToBoolean(ClsConvertInfo.CK))
                {
                    if (strDate + " " + ClsConvertInfo.CKDate == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                    {
                        string[] strPara = new string[] { strAEDAT, "" };
                        ClsSapOperate.SapLoadExecute("CK", strPara);
                        all++;
                    }
                    check++;
                }


                if (Convert.ToBoolean(ClsConvertInfo.RKJE))
                {
                    if (strDate + " " + ClsConvertInfo.RKDate == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                    {
                        string[] strPara = new string[] { strAEDAT, "" };
                        ClsSapOperate.SapLoadExecute("RKJE", strPara);
                        all++;
                    }
                    check++;
                }

                if (Convert.ToBoolean(ClsConvertInfo.CKJE))
                {
                    if (strDate + " " + ClsConvertInfo.CKDate == dtNow.ToString("yyyyMMdd HH:mm:ss"))
                    {
                        string[] strPara = new string[] { strAEDAT, "" };
                        ClsSapOperate.SapLoadExecute("CKJE", strPara);
                        all++;
                    }
                    check++;
                }



                if (all==check)
                {
                    ClsConvertInfo.LastDateTime = ClsConvertInfo.NextDateTime;
                    DateTime dtNextDate = DateTime.ParseExact(ClsConvertInfo.LastDateTime, "yyyyMMdd", null).AddDays(int.Parse(ClsConvertInfo.Interval));
                    ClsConvertInfo.NextDateTime = dtNextDate.ToString("yyyyMMdd");

                    ClsXmlHelper.UpdateXmlConvert("ld");
                    all = 0; check = 0;
                }
                check = 0;
                ClsXmlHelper.ReadXmlConvert();
                SetConventText();
            }
        }
        /// <summary>
        /// 点击开始按钮自动执行读取和转换数据模型
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btStart_Click(object sender, EventArgs e)
        {
            string readLastDateTime;
            string convertLastDateTime;
            
            if(string.IsNullOrEmpty(ClsReadTxtInfo.LastDateTime))
            {
                readLastDateTime="未记录";
            }
            else
            {
                readLastDateTime=ClsReadTxtInfo.LastDateTime;
            }
            if (string.IsNullOrEmpty(ClsConvertInfo.LastDateTime))
            {
                convertLastDateTime = "未记录";
            }
            else
            {
                convertLastDateTime = ClsConvertInfo.LastDateTime;
            }

            if( MessageBox.Show("读取数据上次下载时间为:"+readLastDateTime+"\n\n转换数据上次下载时间为:"+convertLastDateTime+"\n\n可以在数据配置中修改上次下载时间\n\n是否继续执行","提示",MessageBoxButtons.OKCancel)==DialogResult.OK)
            {
                if (this.cbReadIsDown.Checked)
                {
                    if (string.IsNullOrEmpty(ClsReadTxtInfo.LoadDateTime))
                    {
                        MessageBox.Show("请设置读取Txt自动下载时间！");
                        return;
                    }

                    if (string.IsNullOrEmpty(ClsReadTxtInfo.Interval))
                    {
                        MessageBox.Show("请设置读取Txt自动下载周期！");
                        return;
                    }
                }

                ////日期格式yyyyMMdd HHmmss
                try
                {
                    if (string.IsNullOrEmpty(ClsReadTxtInfo.LastDateTime))
                    {
                        ClsReadTxtInfo.NextDateTime = DateTime.Now.ToString("yyyyMMdd") + " " + ClsReadTxtInfo.LoadDateTime;
                    }
                    else
                    {
                        DateTime dtNextDate = DateTime.ParseExact(ClsReadTxtInfo.LastDateTime, "yyyyMMdd", null).AddDays(int.Parse(ClsReadTxtInfo.Interval));
                        ClsReadTxtInfo.NextDateTime = dtNextDate.ToString("yyyyMMdd") + " " + ClsReadTxtInfo.LoadDateTime;
                    }
                }
                catch (Exception)
                {
                    MessageBox.Show("自动读取数据配置错误！");
                    return;
                }

                if (this.cbConvIsDown.Checked)
                {
                    if (string.IsNullOrEmpty(ClsConvertInfo.Interval))
                    {
                        MessageBox.Show("请设置转换自动下载周期！");
                        return;
                    }
                }

                //日期格式yyyyMMdd
                try
                {
                    if (string.IsNullOrEmpty(ClsConvertInfo.LastDateTime))
                    {
                        string dt = DateTime.Now.ToString("yyyyMMdd");
                        ClsConvertInfo.NextDateTime = dt;
                    }
                    else
                    {
                        DateTime dtNextDate = DateTime.ParseExact(ClsConvertInfo.LastDateTime, "yyyyMMdd", null).AddDays(int.Parse(ClsConvertInfo.Interval));
                        ClsConvertInfo.NextDateTime = dtNextDate.ToString("yyyyMMdd");
                    }
                }
                catch (Exception)
                {
                    MessageBox.Show("自动转换数据配置错误！");
                    return;
                }

                 startTime();

                this.btStart.Enabled = false;
            }         
            
        }

        #endregion

        #region 读取txt文本信息操作

        #region 表操作
        private void InitReadWin()
        {
            List<string> list = new List<string>();
            list.Add("");
            list.Add("生效");
            list.Add("不生效");
            this.cbReadSX.DataSource = list;
        }
        private void InitReadTable(bool isWhere)
        {
            DataTable dt = GetDataSource(isWhere);//获取数据源
            if (dt != null && dt.Rows.Count > 0)
            {
                this.dataGridView1.RowCount = dt.Rows.Count;
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    this.dataGridView1.Rows[i].Cells[0].Value = (dt.Rows[i]["TXT_TABLENAME"] ?? string.Empty).ToString();
                    this.dataGridView1.Rows[i].Cells[1].Value = (dt.Rows[i]["TXT_PK"] ?? string.Empty).ToString();
                    this.dataGridView1.Rows[i].Cells[2].Value = (dt.Rows[i]["TXT_ISREADER"] ?? string.Empty).ToString();
                }
            }
            this.dataGridView1.SelectionChanged += dataGridView1_SelectionChanged;
        }

        void dataGridView1_SelectionChanged(object sender, EventArgs e)
        {
            if (this.dataGridView1.CurrentCell != null)
            {
                string strTableName = (this.dataGridView1.Rows[this.dataGridView1.CurrentCell.RowIndex].Cells[0].Value ?? string.Empty).ToString();
                string strTablePK = (this.dataGridView1.Rows[this.dataGridView1.CurrentCell.RowIndex].Cells[1].Value ?? string.Empty).ToString();
                string strIsRead = (this.dataGridView1.Rows[this.dataGridView1.CurrentCell.RowIndex].Cells[2].Value ?? string.Empty).ToString();

                this.tbReadTableName.Text = strTableName;
                this.tbReadPK.Text = strTablePK;
                switch (strIsRead)
                {
                    case "生效":
                        this.cbReadSX.SelectedIndex = 1;
                        break;
                    case "不生效":
                        this.cbReadSX.SelectedIndex = 1;
                        break;
                    default:
                        this.cbReadSX.SelectedIndex = 0;
                        break;
                }
            }
        }
        private void InitReadInitHand()
        {
            DataTable dt = GetDataSource(false);
            dt.DefaultView.Sort = "TXT_TABLENAME";  //按表名排序    
            if (dt != null && dt.Rows.Count > 0)
            {
                this.cblistReadTable.DataSource = dt;
                this.cblistReadTable.DisplayMember = "TXT_TABLENAME";
            }
        }

        #region 对表HB_TXTTABLE操作

        private DataTable GetDataSource(bool isWhere)
        {
            string strSql = "SELECT TXT_TABLENAME,TXT_PK,decode(TXT_ISREADER,'1','生效','不生效') as TXT_ISREADER,DLDATE FROM HB_TXTTABLE  where TXT_ISREADER='1'  ";
            if (isWhere)
            {
                //strSql += " where 1=1";
                if (!string.IsNullOrEmpty(this.tbReadTableName.Text.Trim()))
                {
                    strSql += " and TXT_TABLENAME='" + this.tbReadTableName.Text.Trim() + "'";
                }
                //if (this.cbReadSX.SelectedIndex > 0)
                //{
                //    strSql += " and TXT_ISREADER='" + (this.cbReadSX.SelectedValue.ToString().Equals("生效") ? "1" : "0") + "'";
                //}
            }
            DataTable dt = ClsUtility.GetSelectTable(strSql);
            return dt;
        }

        /// <summary>
        /// 新增
        /// </summary>
        private void InsertData()
        {
            if (!TableVerify())
            {
                return;
            }
            string strSql = "INSERT INTO HB_TXTTABLE (TXT_TABLENAME,TXT_PK,TXT_ISREADER)";
            strSql += " VALUES('" + this.tbReadTableName.Text.Trim().ToUpper() + "',";
            strSql += "'" + this.tbReadPK.Text.Trim().ToUpper() + "',";
            strSql += "'" + (this.cbReadSX.SelectedValue.ToString().Equals("生效") ? "1" : "0") + "')";

            bool ret = ClsUtility.ExecuteSqlToDb(strSql);
            if (ret)
            {
                MessageBox.Show("添加成功！");
            }
            else
            {
                MessageBox.Show("添加失败！");
            }
        }

        /// <summary>
        /// 修改
        /// </summary>
        private void UpdateData()
        {
            if (!TableVerify())
            {
                return;
            }
            string strTableName = this.dataGridView1.Rows[this.dataGridView1.CurrentCell.RowIndex].Cells[0].Value.ToString();
            string strSql = "UPDATE HB_TXTTABLE SET TXT_TABLENAME='" + this.tbReadTableName.Text.Trim().ToUpper() + "',";
            strSql += " TXT_PK='" + this.tbReadPK.Text.Trim().ToUpper() + "',";
            strSql += " TXT_ISREADER='" + (this.cbReadSX.SelectedValue.ToString().Equals("生效") ? "1" : "0") + "'";
            strSql += " WHERE TXT_TABLENAME='" + strTableName + "'";

            bool ret = ClsUtility.ExecuteSqlToDb(strSql);
            if (ret)
            {
                MessageBox.Show("修改成功！");
            }
            else
            {
                MessageBox.Show("修改失败！");
            }
        }

        /// <summary>
        /// 删除
        /// </summary>
        private void DelData()
        {
            DialogResult dr = MessageBox.Show("确定要删除当前选择的数据吗?", "删除提示");
            if (dr == DialogResult.OK)//如果点击“确定”按钮
            {

                string strTableName = this.dataGridView1.Rows[this.dataGridView1.CurrentCell.RowIndex].Cells[0].Value.ToString();
                string strSql = "DELETE FROM HB_TXTTABLE WHERE TXT_TABLENAME='" + strTableName + "'";

                bool ret = ClsUtility.ExecuteSqlToDb(strSql);
                if (ret)
                {
                    MessageBox.Show("删除成功！");
                }
                else
                {
                    MessageBox.Show("删除失败！");
                }
            }
        }
        #endregion

        /// <summary>
        /// 验证
        /// </summary>
        /// <returns></returns>
        private bool TableVerify()
        {
            string strTableName = this.tbReadTableName.Text.Trim();
            if (string.IsNullOrEmpty(strTableName))
            {
                MessageBox.Show("表名不能为空！");
                return false;
            }

            if (this.cbReadSX.SelectedIndex <= 0)
            {
                MessageBox.Show("请选择是否生效！");
                return false;
            }

            return true;
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btQuery_Click(object sender, EventArgs e)
        {
            InitReadTable(true);
        }

        /// <summary>
        /// 新增
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btReadAdd_Click(object sender, EventArgs e)
        {
            InsertData();

            this.dataGridView1.SelectionChanged -= dataGridView1_SelectionChanged;

            this.dataGridView1.Rows.Insert(0);
            this.dataGridView1.Rows[0].Cells[0].Value = this.tbReadTableName.Text.Trim();
            this.dataGridView1.Rows[0].Cells[1].Value = this.tbReadPK.Text.Trim();
            this.dataGridView1.Rows[0].Cells[2].Value = this.cbReadSX.SelectedValue.ToString();

            this.dataGridView1.SelectionChanged += dataGridView1_SelectionChanged;
        }

        /// <summary>
        /// 修改
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btReadUpdate_Click(object sender, EventArgs e)
        {
            UpdateData();
            int row = this.dataGridView1.CurrentCell.RowIndex;
            this.dataGridView1.Rows[row].Cells[0].Value = this.tbReadTableName.Text.Trim();
            this.dataGridView1.Rows[row].Cells[1].Value = this.tbReadPK.Text.Trim();
            this.dataGridView1.Rows[row].Cells[2].Value = this.cbReadSX.SelectedValue.ToString();
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btReadDel_Click(object sender, EventArgs e)
        {
            DelData();
            int row = this.dataGridView1.CurrentCell.RowIndex;
            this.dataGridView1.Rows.RemoveAt(row);
        }
        #endregion

        private void cbReadIsDown_CheckedChanged(object sender, EventArgs e)
        {
            ClsReadTxtInfo.IsDownLoad = this.cbReadIsDown.Checked;
            ClsXmlHelper.UpdateXmlTxt("cb");
        }

        #region Txt读取 Xml操作
        private void btReadSave_Click(object sender, EventArgs e)
        {
            SetReadTxtClass();
            if (ClsXmlHelper.UpdateXmlTxt(""))
            {
                MessageBox.Show("保存成功！");
            }
            else
            {
                MessageBox.Show("保存失败！");
            }
        }

        private void SetReadTxt()
        {
            this.tbReadDate.Text = ClsReadTxtInfo.LoadDateTime;
            this.tbReadInterval.Text = ClsReadTxtInfo.Interval;
            this.tbReadFilePath.Text = ClsReadTxtInfo.LocalFilePath;
            this.tbReadSeparator.Text = ClsReadTxtInfo.Separator;

            this.cbReadIsDown.Checked = Convert.ToBoolean(ClsReadTxtInfo.IsDownLoad);
            this.tbReadIntervalInfo.Text = ClsReadTxtInfo.Interval;
            this.tbReadLastDateInfo.Text = ClsReadTxtInfo.LastDateTime;
            this.tbReadDateInfo.Text = ClsReadTxtInfo.LoadDateTime;
            this.tbReadFilePathInfo.Text = ClsReadTxtInfo.LocalFilePath;
            this.readLasttime.Text = ClsReadTxtInfo.LastDateTime;
        }

        private void SetReadTxtClass()
        {
            ClsReadTxtInfo.LoadDateTime = this.tbReadDate.Text;
            ClsReadTxtInfo.Interval = this.tbReadInterval.Text;
            ClsReadTxtInfo.LocalFilePath = this.tbReadFilePath.Text;
            ClsReadTxtInfo.Separator = this.tbReadSeparator.Text;
            ClsReadTxtInfo.LastDateTime = this.readLasttime.Text;
        }
        #endregion

        /// <summary>
        /// 读取数据打开文件夹
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btReadFile_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog dialog = new FolderBrowserDialog();
            dialog.Description = "请选择文件路径";
            if (dialog.ShowDialog() == DialogResult.OK)
            {
                string foldPath = dialog.SelectedPath;
                this.tbReadHandFile.Text = foldPath;
            }
        }

        /// <summary>
        /// 手动读取数据
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btReadHand_Click(object sender, EventArgs e)
        {
            string path = this.tbReadHandFile.Text;
            if (string.IsNullOrEmpty(path))
            {
                MessageBox.Show("请选择要读取的文件路径!");
                return;
            }

            //需要手动读取的表
            List<string> handTable = new List<string>();

            if (this.cblistReadTable.CheckedItems.Count != 0)
            {
                for (int i = 0; i <= cblistReadTable.CheckedItems.Count - 1; i++)
                {
                    handTable.Add(cblistReadTable.GetItemText(cblistReadTable.CheckedItems[i]));
                }
            }

            if (handTable.Count <= 0)
            {
                MessageBox.Show("请选需要读取的表！");
                return;
            }

            m_dt = GetDataSource(false);

            this.lbReadTxtInfo.Text = string.Empty;
            this.lbReadTxtInfo.Visible = true; //显示正在加载数据
            this.lbReadTxtInfo.Text = "正在读取数据请稍后...";
            this.btReadHand.Enabled = false;   //读取数据按钮不可用
            ThreadTotalCount = 0;
            ThreadSucessCount = 0;
            ThreadErrorCount = 0;
            this.pbReadTxt.Value = 0;
            this.pbReadTxt.Maximum = 1;
            ReadFileTable(handTable, path);    
               
 
        }

        private void ReadFileTable(List<string> tableList, string path)
        {
            //路径下所有的txt文件名称
            Dictionary<string, string> allTable = new Dictionary<string, string>();
            //List<string> allTableDistinct = new List<string>();

            DirectoryInfo folder = new DirectoryInfo(path);

            foreach (FileInfo file in folder.GetFiles("*.txt"))
            {
                string tablename = ClsCom.GetTableForArr(file.Name);
                string filenamedate = ClsCom.GetDateForArr(file.Name);
                DataRow[] dr = m_dt.Select("TXT_TABLENAME='" + tablename.ToUpper() + "'");
                if (dr != null && dr.Length > 0)
                {
                    if ((string.Compare((dr[0]["DLDATE"] ?? "00000000").ToString(), filenamedate) < 0) && tableList.Contains(tablename))
                    {
                        allTable.Add(file.Name, file.FullName);
                        //if (!allTableDistinct.Contains(tablename))
                        //{
                        //    allTableDistinct.Add(tablename);
                        //}
                        
                    }
                }
            }

            if (allTable.Count <= 0)
            {
                this.lbReadTxtInfo.Visible = false; //显示正在加载数据
                this.btReadHand.Enabled = true;   //读取数据按钮不可用
                LHSM.Logs.Log.Logger.Error("ReadFileTable():文档已转换或TXT文档不存在");
                //MessageBox.Show("文档已转换或TXT文档不存在！");
                return;
            }
            ReadTxt(tableList, SortDictionary_Asc(allTable));

        }

        private void ReadTxt(List<string> tableList, Dictionary<string, string> allTable)
        {
            ThreadTotalCount = allTable.Count; //txt文件的个数           
            lbText = "共" + ThreadTotalCount + "个文件";
            this.pbReadTxt.Maximum = ThreadTotalCount;

            foreach (string tablename in tableList)
            {
                foreach (KeyValuePair<string, string> item in allTable)
                {
                    if (item.Key.Contains(tablename))
                    {

                        ClsTxtReadHelper TxtReadHelper = new ClsTxtReadHelper();

                        TxtReadHelper.threadStartEvent += TxtReadHelper_threadStartEvent;
                        //线程结束事件
                        TxtReadHelper.threadEndEvent += TxtReadHelper_threadEndEvent;

                        TxtReadHelper.TableName = tablename;
                        TxtReadHelper.FilePath = item.Value;
                        TxtReadHelper.dr = m_dt.Select("TXT_TABLENAME='" + tablename + "'");

                        int tablenamecount = DictionaryCount(tablename, allTable);
                        if (tablenamecount > 1)
                        {
                            TxtReadHelper.ReadTxtInsert();
                        }
                        else
                        {
                            ThreadStart threadStart = new ThreadStart(TxtReadHelper.ReadTxtInsert);
                            Thread thread = new Thread(threadStart);
                            thread.Start(); //启动新线程
                        }
                    }
                }
            }
        }

        void TxtReadHelper_threadStartEvent(object sender, EventArgs e)
        {

        }

        private delegate void MyDelegateUI();

        void TxtReadHelper_threadEndEvent(object sender, EventArgs e)
        {
            if (!this.btReadHand.Enabled)
            {
                MyDelegateUI pb = delegate
                {
                    this.pbReadTxt.Value = this.pbReadTxt.Value + 1;
                };
                this.pbReadTxt.Invoke(pb);

                if (sender == null)
                {
                    ThreadSucessCount++;

                }
                else
                {
                    ThreadErrorCount++;
                }

                MyDelegateUI lb = delegate
                {
                    this.lbReadTxtInfo.Visible = true; //显示正在加载数据   
                    this.lbReadTxtInfo.Text = lbText + ",成功：" + ThreadSucessCount + "个，" + "失败" + ThreadErrorCount + "个。";
                    this.btReadHand.Enabled = true;
                };
                this.lbReadTxtInfo.Invoke(lb);

                ThreadTotalCount--;
                if (ThreadTotalCount == 0)
                {
                    MyDelegateUI bt = delegate
                    {
                        this.btReadHand.Enabled = true;
                    };
                    this.btReadHand.Invoke(bt);
                }
            }

        }

        /// <summary>
        /// Dictionary排序
        /// </summary>
        /// <param name="dic"></param>
        /// <returns></returns>
        private Dictionary<string, string> SortDictionary_Asc(Dictionary<string, string> dic)
        {
            List<KeyValuePair<string, string>> myList = new List<KeyValuePair<string, string>>(dic);
            myList.Sort(delegate(KeyValuePair<string, string> s1, KeyValuePair<string, string> s2)
            {
                return s1.Key.CompareTo(s2.Key);
            });
            dic.Clear();
            foreach (KeyValuePair<string, string> pair in myList)
            {
                dic.Add(pair.Key, pair.Value);
            }
            return dic;
        }

        private int DictionaryCount(string tablename, Dictionary<string, string> dic)
        {
            int retcount = 0;
            foreach (KeyValuePair<string, string> item in dic)
            {
                if (item.Key.Contains(tablename))
                {
                    retcount++;
                }
            }

            return retcount;
        }

        #endregion

        #region 转换操作

        #region 配置信息
        private void btConvSave_Click(object sender, EventArgs e)
        {
            SetConventClass();
            if (ClsXmlHelper.UpdateXmlConvert(""))
            {
                MessageBox.Show("保存成功！");
            }
            else
            {
                MessageBox.Show("保存失败！");
            }
        }

        /// <summary>
        /// 根据xml配置设置显示页面
        /// </summary>
        private void SetConventText()
        {
            //昨日今日
            if (ClsConvertInfo.ToDay == "1")
            {
                this.rbLastDay.Checked = true;
                this.rbLastDayInfo.Checked = true;
            }
            else
            {
                this.rbToDay.Checked = true;
                this.rbToDayInfo.Checked = true;
            }
            this.tbConvInterval.Text = ClsConvertInfo.Interval;
            this.tbConvIntervalInfo.Text = ClsConvertInfo.Interval;

            //账务库存分析
            if (Convert.ToBoolean(ClsConvertInfo.ZWKC))
            {
                this.cbZWKCFX.Checked = true;
                this.cbZWKCInfo.Checked = true;
            }
            else
            {
                this.cbZWKCFX.Checked = false;
                this.cbZWKCInfo.Checked = false;
            }
            this.tbConvZWKCDate.Text = ClsConvertInfo.ZWKCDate;
            this.tbConvZWKCDateInfo.Text = ClsConvertInfo.ZWKCDate;

            //实物库存分析
            if (Convert.ToBoolean(ClsConvertInfo.SWKC))
            {
                this.cbSWKCFX.Checked = true;
                this.cbSWKCInfo.Checked = true;
            }
            else
            {
                this.cbSWKCFX.Checked = false;
                this.cbSWKCInfo.Checked = false;
            }
            this.tbConvSWKCDate.Text = ClsConvertInfo.SWKCDate;
            this.tbConvSWKCDateInfo.Text = ClsConvertInfo.SWKCDate;

            //入库情况分析
            if (Convert.ToBoolean(ClsConvertInfo.RK))
            {
                this.cbRKFX.Checked = true;
                this.cbRKInfo.Checked = true;
            }
            else
            {
                this.cbRKFX.Checked = false;
                this.cbRKInfo.Checked = false;
            }
            this.tbConvRKDate.Text = ClsConvertInfo.RKDate;
            this.tbConvRKDateInfo.Text = ClsConvertInfo.RKDate;

            //出库情况分析
            if (Convert.ToBoolean(ClsConvertInfo.CK))
            {
                this.cbCKFX.Checked = true;
                this.cbCKInfo.Checked = true;
            }
            else
            {
                this.cbCKFX.Checked = false;
                this.cbCKInfo.Checked = false;
            }
            this.tbConvCKDate.Text = ClsConvertInfo.CKDate;
            this.tbConvCKDateInfo.Text = ClsConvertInfo.CKDate;

            //入库金额分析
            if (Convert.ToBoolean(ClsConvertInfo.RKJE))
            {
                this.cbRKJEFX.Checked = true;
                this.cbRKJEInfo.Checked = true;
            }
            else
            {
                this.cbRKJEFX.Checked = false;
                this.cbRKJEInfo.Checked = false;
            }
            this.tbConvRKJEDate.Text = ClsConvertInfo.RKJEDate;
            this.tbConvRKJEDateInfo.Text = ClsConvertInfo.RKJEDate;

            //出库金额分析
            if (Convert.ToBoolean(ClsConvertInfo.CKJE))
            {
                this.cbCKJEFX.Checked = true;
                this.cbCKJEInfo.Checked = true;
            }
            else
            {
                this.cbCKJEFX.Checked = false;
                this.cbCKJEInfo.Checked = false;
            }
            this.tbConvCKJEDate.Text = ClsConvertInfo.CKJEDate;
            this.tbConvCKJEDateInfo.Text = ClsConvertInfo.CKJEDate;

            //出库金额分析
            if (Convert.ToBoolean(ClsConvertInfo.BGY))
            {
                this.cbBGYFX.Checked = true;
                this.cbBGYInfo.Checked = true;
            }
            else
            {
                this.cbBGYFX.Checked = false;
                this.cbBGYInfo.Checked = false;
            }
            this.tbConvBGYDate.Text = ClsConvertInfo.BGYDate;
            this.tbConvBGYDateInfo.Text = ClsConvertInfo.BGYDate;

            this.cbConvIsDown.Checked = ClsConvertInfo.IsDownLoad;
            this.ConventLastTime.Text = ClsConvertInfo.LastDateTime;
        }

        /// <summary>
        /// 获取页面信息
        /// </summary>
        private void SetConventClass()
        {
            //昨日今日
            if (this.rbLastDay.Checked)
            {
                ClsConvertInfo.ToDay = "1";
            }
            else
            {
                ClsConvertInfo.ToDay = "0";
            }
            ClsConvertInfo.Interval = this.tbConvInterval.Text;

            //账务库存分析
            ClsConvertInfo.ZWKC = this.cbZWKCFX.Checked.ToString();
            ClsConvertInfo.ZWKCDate = this.tbConvZWKCDate.Text;

            //实物库存分析
            ClsConvertInfo.SWKC = this.cbSWKCFX.Checked.ToString();
            ClsConvertInfo.SWKCDate = this.tbConvSWKCDate.Text;

            //入库情况分析
            ClsConvertInfo.RK = this.cbRKFX.Checked.ToString();
            ClsConvertInfo.RKDate = this.tbConvRKDate.Text;

            //出库情况分析
            ClsConvertInfo.CK = this.cbCKFX.Checked.ToString();
            ClsConvertInfo.CKDate = this.tbConvCKDate.Text;

            //入库J金额分析
            ClsConvertInfo.RKJE = this.cbRKJEFX.Checked.ToString();
            ClsConvertInfo.RKJEDate = this.tbConvRKJEDate.Text;

            //出库金额分析
            ClsConvertInfo.CKJE = this.cbCKJEFX.Checked.ToString();
            ClsConvertInfo.CKJEDate = this.tbConvCKJEDate.Text;

            //保管员工作量分析
            ClsConvertInfo.BGY  = this.cbBGYFX.Checked.ToString();
            ClsConvertInfo.BGYDate = this.tbConvBGYDate.Text;

            ClsConvertInfo.LastDateTime = this.ConventLastTime.Text;
        }

        #endregion

        /// <summary>
        /// 手动转换模型数据
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btConvHand_Click(object sender, EventArgs e)
        {
            this.pbConvData.Value = 0;
            this.pbConvData.Maximum = 1;

            this.lbConvInfo.Text = string.Empty;
            this.lbConvInfo.Visible = true; //显示正在加载数据
            int ConvCount = GetConvCount();  //需要转化的数量

            this.pbConvData.Maximum = ConvCount;
            string lbConvText = "共" + ConvCount.ToString() + "个模型";
            int ConvSucessCount = 0;
            int ConvErrorCount = 0;
            bool ret = false;

            string strAEDAT = this.dtConvDataSD.Value.ToString("yyyyMMdd").Trim();
            if (cbConvZWKCSD.Checked)
            {
                //参数，日期、单位等
                string[] strPara = new string[] { strAEDAT, "" };

                ret = ClsSapOperate.SapLoadExecute("ZWKC", strPara);

                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }
   
            

            if (cbConvSWKCSD.Checked)
            {
                
                string[] strPara = new string[] { strAEDAT, "" };
                ret= ClsSapOperate.SapLoadExecute("SWKC", strPara);

                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }

            if (cbConvRKSD.Checked)
            {
                string[] strPara = new string[] { strAEDAT, "" };
                ret = ClsSapOperate.SapLoadExecute("RK", strPara);
                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }

            if (cbConvCKSD.Checked)
            {
                string[] strPara = new string[] { strAEDAT, "" };
                ret = ClsSapOperate.SapLoadExecute("CK", strPara);

                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }
            if (cbConvRKJESD.Checked)
            {
                string[] strPara = new string[] { strAEDAT, "" };
                ret = ClsSapOperate.SapLoadExecute("RKJE", strPara);

                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }
            if (cbConvCKJESD.Checked)
            {
                string[] strPara = new string[] { strAEDAT, "" };
                ret = ClsSapOperate.SapLoadExecute("CKJE", strPara);

                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }
            if (cbConvBGYSD.Checked)
            {
                string[] strPara = new string[] { strAEDAT, "" };
                ret = ClsSapOperate.SapLoadExecute("BGY", strPara);

                this.pbConvData.Value = this.pbConvData.Value + 1;
                if (ret)
                {
                    ConvSucessCount++;
                }
                else
                {
                    ConvErrorCount++;
                }
                this.lbConvInfo.Text = lbConvText + ",成功：" + ConvSucessCount + "个，" + "失败" + ConvErrorCount + "个。";
            }
        }

        private void cbConvIsDown_CheckedChanged(object sender, EventArgs e)
        {

            ClsConvertInfo.IsDownLoad = this.cbConvIsDown.Checked;
            ClsXmlHelper.UpdateXmlConvert("cb");
        }
        /// <summary>
        /// 转化的中间表数量
        /// </summary>
        /// <returns></returns>
        private int GetConvCount()
        {
            int count = 0;
            if (cbConvZWKCSD.Checked)
            {
                count++;
            }

            if (cbConvSWKCSD.Checked)
            {
                count++;
            }

            if (cbConvRKSD.Checked)
            {
                count++;
            }

            if (cbConvCKSD.Checked)
            {
                count++;
            }
            if (cbConvRKJESD.Checked)
            {
                count++;
            }

            if (cbConvCKJESD.Checked)
            {
                count++;
            }
            if (cbConvBGYSD.Checked)
            {
                count++;
            }
            return count;
        }

        #endregion

        private void btRefresh_Click(object sender, EventArgs e)
        {
            SetReadTxt();
            SetConventText();
        }

        private void btStop_Click(object sender, EventArgs e)
        {
            stopTime();
            this.btStart.Enabled = true;
        }

        private void lklAll_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)//全选
        {
            for (int i = 0; i < this.cblistReadTable.Items.Count; i++)
            {
                this.cblistReadTable.SetItemChecked(i, true);
            }
        }

        private void lklNone_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)//反选
        {
        
            for (int i = 0; i < this.cblistReadTable.Items.Count; i++)
            {
                if (cblistReadTable.GetItemChecked(i))
                {
                    cblistReadTable.SetItemChecked(i, false);
                }
                else
                {
                    cblistReadTable.SetItemChecked(i, true);
                }
            }
        }

        private void frmLHSMJOB_Load(object sender, EventArgs e)
        {
            CheckForIllegalCrossThreadCalls = false;
        }
        /// <summary>
        /// 保存ftp配置
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btFtpInfoSave_Click_1(object sender, EventArgs e)
        {
            SetFtpClass();
            if (ClsXmlHelper.UpdateXmlFtp())
            {
                MessageBox.Show("保存成功！");
            }
            else {
                MessageBox.Show("保存失败！");
            }
        }
       

        private void cbFtpIsDown_CheckedChanged_1(object sender, EventArgs e)
        {

        }
        private void deleteFile(string srcPath) {
            //string srcPath = "E:\\OracleTxtData\\DemoTxt\\mm\\";
            DirectoryInfo dir = new DirectoryInfo(srcPath);
            FileSystemInfo[] fileinfo = dir.GetFileSystemInfos();  //返回目录中所有文件和子目录
            foreach (FileSystemInfo i in fileinfo)
            {
                if (i is DirectoryInfo)            //判断是否文件夹
                {
                   // DirectoryInfo subdir = new DirectoryInfo(i.FullName);
                   // subdir.Delete(true);          //删除子目录和文件
                }
                else
                {
                    if (i.Name.Contains("ZC10MMDG072_GZ") || i.Name.Contains("ZC10MMDG078_GZ"))
                    {
                        //如果 使用了 streamreader 在删除前 必须先关闭流 ，否则无法删除 sr.close();ZC10MMDG078_GZ
                        File.Delete(i.FullName);      //删除指定文件
                        ClsLogInfo.WriteSapLog("3", "DELETE_FTP_GZ", DateTime.Now.ToString("yyyyMMdd HH:mm:ss"), "删除出入库通知单ftp_GZ文件成功\t\n");
                    }

                }
            }
        }
    }

}
