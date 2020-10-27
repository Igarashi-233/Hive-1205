package com;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF extends GenericUDTF {

    private List<String> dataList = new ArrayList<>();

    //定义输出数据的列名和输出类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //定义输出数据的列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");

        //定义输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public void process(Object[] args) throws HiveException {

        //获取数据
        String data = args[0].toString();

        //获取分隔符
        String splitKey = args[1].toString();

        //切分数据
        String[] words = data.split(splitKey);

        //遍历写出
        for (String word : words) {

            //将数据放入集合
            dataList.clear();
            dataList.add(word);

            //写出数据的操作
            forward(dataList);
        }

    }

    public void close() throws HiveException {

    }
}
