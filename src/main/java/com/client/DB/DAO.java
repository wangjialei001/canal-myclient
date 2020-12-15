package com.client.DB;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DAO<T> {
    ResultSet res = null;
    Connection conn = null;
    PreparedStatement pre = null;

    //给sql语句设置参数值
    private void setParameter(String... parameter){
        for (int i=0; i<parameter.length; i++){
            try {
                pre.setObject(i+1, parameter[i]);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //从数据库获取值到ResultSet
    public List<T> query(Class<T> cls , String sql, String... parameter){
        List<T> list = new ArrayList<T>();
        try {
            conn = DBUtil.getConnection();
            pre = conn.prepareStatement(sql);
            setParameter(parameter); // 设置参数值
            res = pre.executeQuery();

            while(res.next()){
                T obj = cls.newInstance(); //创建这个类的新的实例
                setData(cls, obj); //给obj的属性设置值
                list.add(obj);//添加到list中
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            DBUtil.closeAll(res, pre, conn);
        }
        return list;
    }

    /**插入/删除**/
    public int Update(String sql, String... parameter){
        int num = 0 ;
        try {
            conn = DBUtil.getConnection();
            pre = conn.prepareStatement(sql);
            setParameter(parameter); // 设置参数值
            num = pre.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            DBUtil.closeAll(res, pre, conn);
        }
        return num; // 受影响的行数
    }



    //将结果集给javebean对象设置值 , 通过对象属性的set方法设置值
    private void setData(Class<T> cls, T obj){
        /**
         * Field 对象属性 类，
         * 通过方法 set(Object obj, Object value) ； 给obj设置其value值
         * 通过ResultSet对象 得到ResultSetMetaData接口的对象， 可以获得数据库字段属性
         */
        try {
            ResultSetMetaData rsmd = res.getMetaData();
            int col = rsmd.getColumnCount();
            for (int i=1; i<=col; i++){
                String DBField = rsmd.getColumnLabel(i);
                Field field = cls.getDeclaredField(DBField);
                field.setAccessible(true); //私有可见
                field.set(obj, res.getObject(i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
