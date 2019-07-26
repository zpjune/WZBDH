using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace WebServicetest
{
    public static class ClsCom
    {
        public static string GetTableForArr(string TableFullName)
        {
            string[] filename = TableFullName.Split('_');
            string tableName = filename[0];
            for (int i = 1; i < filename.Length; i++)
            {
                Regex r = new Regex("^([0-9]+)([0-9]{1,2})([0-9]{1,2})$");
                Match m = r.Match(filename[i]);
                if (m.Success)
                {
                    break;
                }
                else
                {
                    tableName += "_" + filename[i];
                }
            }

            return tableName;
        }
        public static string GetDateForArr(string TableFullName)
        {
            string tableDate = string.Empty;
            string[] filename = TableFullName.Split('_');
            for (int i = 1; i < filename.Length; i++)
            {
                Regex r = new Regex("^([0-9]+)([0-9]{1,2})([0-9]{1,2})$");
                Match m = r.Match(filename[i]);
                if (m.Success) {
                    tableDate = filename[i];
                    break;
                }
            }

            return tableDate;
        }
       /// <summary>
       /// 对于SAP中形式如123.00-的负数进行判断
       /// </summary>
       /// <param name="tableNumberCol"></param>
       /// <returns></returns>

        public static string GetNumberForArr(string tableNumberCol)
        {
            string numberCol = tableNumberCol;
             Regex r = new Regex(@"\d{1,}\.\d{1,}-$");
             Match m = r.Match(numberCol);
             if (m.Success)
              {
                  numberCol=numberCol.Substring(0, numberCol.Length - 1);
                  numberCol = "-" + numberCol;
                  return numberCol;
              }
             else
             {
                 return numberCol;
             }
             
        }
    }
}
