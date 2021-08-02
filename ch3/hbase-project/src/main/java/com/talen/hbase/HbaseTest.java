package com.talen.hbase;

import com.talen.hbase.util.HBaseUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HbaseTest {
    public static final String nameSpace = "huangxiong";
    public static final String tabeName = "student";

    public static void main(String[] args) {
        HBaseUtil.init("jikehadoop01,jikehadoop02,jikehadoop03");
        try {
            String[] colFamily = new String[]{"name", "info", "score"};
            //HBaseUtil.createNameSpace("huangxiong");
            //HBaseUtil.createTableIfNotExist(nameSpace, tabeName, colFamily);
            //HBaseUtil.addRecord(nameSpace, tabeName, buildData());
            //System.out.println(HBaseUtil.getRow(nameSpace + ":" + tabeName, "20210000000002".getBytes()));
            HBaseUtil.dropTable(nameSpace, tabeName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<RowBean> buildData() {
        List<RowBean> rowBeanList = new ArrayList<RowBean>();
        RowBean rowBean = new RowBean();
        rowBean.setRowKey("20210000000004");
        HashMap<String, List<ColBean>> columnFamilys = new HashMap<>();
        ColBean colBean = new ColBean();
        colBean.setColValue("Rose");
        List<ColBean> colBeanList = new ArrayList<>();
        colBeanList.add(colBean);
        columnFamilys.put("name", colBeanList);
        ColBean colBean1 = new ColBean();
        colBean1.setColName("student_id");
        colBean1.setColValue("20210000000004");
        ColBean colBean2 = new ColBean();
        colBean2.setColName("class");
        colBean2.setColValue("2");
        List<ColBean> colBeanList1 = new ArrayList<>();
        colBeanList1.add(colBean1);
        colBeanList1.add(colBean2);
        columnFamilys.put("info", colBeanList1);
        ColBean colBean3 = new ColBean();
        colBean3.setColName("understanding");
        colBean3.setColValue("60");
        ColBean colBean4 = new ColBean();
        colBean4.setColName("programming");
        colBean4.setColValue("61");
        List<ColBean> colBeanList2 = new ArrayList<>();
        colBeanList2.add(colBean3);
        colBeanList2.add(colBean4);
        columnFamilys.put("score", colBeanList2);
        rowBean.setColumnFamilys(columnFamilys);
        rowBeanList.add(rowBean);
        return rowBeanList;
    }
}
