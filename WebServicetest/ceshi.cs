using LHSM.HB.ObjSapForRemoting;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Timers;

namespace WebServicetest
{
    public partial class ceshi : Form
    {
        public ceshi()
        {
            InitializeComponent();
        }

        private void ceshi_Load(object sender, EventArgs e)
        {
        }

        private void button2_Click(object sender, EventArgs e)
        {
            string strAEDAT = this.dateTimePicker1.Value.ToString("yyyy").Trim();
            //参数，日期、单位等
            string[] strPara = new string[] { strAEDAT, "" };

            ClsSapOperate.SapLoadExecute("XMTZ", strPara);

            GetLog();
        }

        private void GetLog()
        {
            string strSql = " SELECT 发生日期,接口代码,转化日期,日志内容 FROM (SELECT LOG_DATEGET as 发生日期,plan_code as 接口代码,LOG_DATE as 转化日期, LOG_REMARK as 日志内容 FROM LOGINFO  ORDER BY LOG_DATEGET DESC) WHERE ROWNUM <= 20 ";
            DataTable dt = ClsUtility.GetSelectTable(strSql);

            dataGridView1.DataSource = null;
            dataGridView1.DataSource = dt;
        }

        private void button3_Click(object sender, EventArgs e)
        {
            GetLog();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            string strAEDAT = this.dateTimePicker1.Value.ToString("yyyyMM").Trim();
            //参数，日期、单位等
            string[] strPara = new string[] { strAEDAT, "" };

            ClsSapOperate.SapLoadExecute("UA", strPara);

            GetLog();
        }

        private void button4_Click(object sender, EventArgs e)
        {
            string strAEDAT = this.dateTimePicker1.Value.ToString("yyyyMM").Trim();
            //参数，日期、单位等
            string[] strPara = new string[] { strAEDAT, "" };

            ClsSapOperate.SapLoadExecute("XMFW", strPara);

            GetLog();
        }

        private void button5_Click(object sender, EventArgs e)
        {
            string strAEDAT = this.dateTimePicker1.Value.ToString("yyyyMM").Trim();
            //参数，日期、单位等
            string[] strPara = new string[] { strAEDAT, "" };

            ClsSapOperate.SapLoadExecute("XMZJ", strPara);

            GetLog();
        }

        private void button6_Click(object sender, EventArgs e)
        {
            string strAEDAT = this.dateTimePicker1.Value.ToString("yyyyMM").Trim();
            //参数，日期、单位等
            string[] strPara = new string[] { strAEDAT, "" };

            ClsSapOperate.SapLoadExecute("CG", strPara);

            GetLog();
        }

        private void button7_Click(object sender, EventArgs e)
        {
            string strAEDAT = this.dateTimePicker1.Value.ToString("yyyyMM").Trim();
            //参数，日期、单位等
            string[] strPara = new string[] { strAEDAT, "" };

            ClsSapOperate.SapLoadExecute("CGSJ", strPara);

            GetLog();
        }

    }
}
