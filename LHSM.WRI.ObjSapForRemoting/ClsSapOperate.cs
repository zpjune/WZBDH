using System;
using System.Collections.Generic;
using System.Text;

namespace LHSM.HB.ObjSapForRemoting
{
    /// <summary>
    /// 工程模块：LHSM.HB.ObjSapForRemoting
    /// 功能：数据执行
    /// 编写时间：20141014
    /// 编写人：孙冰
    /// </summary>
    public static class ClsSapOperate
    {
        /// <summary>
        /// 数据获取执行
        /// </summary>
        /// <param name="p_SapName">执行接口名称</param>
        /// <param name="p_SapPara">参数数据0.创建时间，1.单位</param>
        /// <param name="p_SapPara">选择接口内的数据，如果为NULL 说明不是选择具体接口，如果存在说明需要获取具体接口信息</param>
        public static bool SapOperateExecute(string p_SapName, string[] p_SapPara,string [] p_SapSelect)
        {
            bool Result = true;

            //生成对象
            ISAPInterface sap = ClsUtility.GetInter(p_SapName);
            ClsSAPDataParameter Sap_Para = ClsUtility.GetSapConParameter(p_SapPara);
            Sap_Para.Sap_Select = p_SapSelect;
                
            //完成日志
            if (sap.GetSAPData(Sap_Para))
            {
                ClsLogInfo.WriteSapLog("0", p_SapName, Sap_Para.Sap_AEDAT, "申请开始下载！");
            }
            else
            {
                Result = false;
                ClsLogInfo.WriteSapLog("0", p_SapName, Sap_Para.Sap_AEDAT, "申请下载失败！");
            }

            return Result;
        }

        /// <summary>
        /// 数据
        /// </summary>
        /// <param name="p_SapName">接口类型名称</param>
        /// <param name="p_SapPara">参数数据0.创建时间，1.单位</param>
        public static bool SapLoadExecute(string p_SapName, string[] p_SapPara)
        {
            bool Result = true;

            //生成对象
            ISAPLoadInterface sap = ClsUtility.GetLoadInter(p_SapName);
            ClsSAPDataParameter Sap_Para = ClsUtility.GetSapConParameter(p_SapPara);

            //完成日志
            if (sap.SAPLoadData(Sap_Para))
            {
                ClsLogInfo.WriteSapLog("1", p_SapName, Sap_Para.Sap_AEDAT, "申请开始加载！");
            }
            else
            {
                Result = false;
                ClsLogInfo.WriteSapLog("1", p_SapName, Sap_Para.Sap_AEDAT, "申请加载失败！");
            }

            return Result;
        }


        /// <summary>
        /// EMAIL验证登录
        /// </summary>
        /// <param name="p_User">用户</param>
        /// <param name="p_Pass">密码</param>
        /// <returns></returns>
        public static bool SapLogMail(string p_User, string p_Pass)
        {
            bool Result = true;
            ClsLogMail log = new ClsLogMail();
            Result = log.MailConnect(p_User, p_Pass);
            return Result;
        }


    }
}
