package com.pbs.dataimitate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.*;
import java.sql.*;

public class JDBC {

	//主程序
	public static void main(String[] args) {
		System.out.println("=====主程序");
		timer();
	}
	
	
	/**
	 * 周期性定时器，每5秒执行一次程序
	 */
	public static void timer() {
		System.out.println("====定时器运行");
		//设置定时器
		Timer timer = new Timer();
        timer.schedule(new TimerTask() {  
            public void run() {  
            	start();
            }  
        },1000,5000);//启动1秒后程序执行，而后每次程序执行完成后每5秒再执行一次
	}
	
	
	
	
	/**
	 * 数据库连接
	 */
	public static void start() {
		 ResultSet rs  = null;
		 Statement state = null;
		try {
			//加载驱动
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/pbssystem";
			String user = "root";
			String password = "root";
			Connection conn = DriverManager.getConnection(url, user, password);
			
			//验证数据库连接成功与否
			if(!conn.isClosed()){
				System.out.println("====数据库连接成功");
			}
			
//			//如果pbs_node_info表格存在，删除
//			String sql = "drop table if exists pbs_node_info";
//			state = conn.createStatement();
//			state.execute(sql);
//			System.out.println("=====pbs_node_info旧表删除成功^-^");
//			
//			//新建pbs_node_info表格
//			sql = "create table pbs_node_info ( "
//					+ "id integer(20) primary key auto_increment,"
//					+ " zh integer(10),"
//					+ " zt varchar(10),"
//					+ " bm varchar(50),"
//					+ " ss integer(20),"
//					+ " zd integer(20));";
//			state.execute(sql);
//			System.out.println("=====pbs_node_info新表创建成功^@^");

			//新建一个PbsnodeInfo用于存储数据
			PbsRentInfo pbsRent = new PbsRentInfo();
			
			String sql = "select id,max_number from pbs_rent_info group by id";
			
			state = conn.createStatement();
			
			rs = state.executeQuery(sql);
			
			String sql2 = "update pbs_rent_info set OPERNum = ? where id = ?";
			System.out.println("====sql2下面");
			PreparedStatement ps = conn.prepareStatement(sql2);
			
			//开始时间
			long start = System.currentTimeMillis();
			//关闭自动提交事务
			conn.setAutoCommit(false);
			
			while(rs.next()){
				//打桩输出
				System.out.println("====while()里面");
				
				//从结果集中获取max，id
				int id = rs.getInt("id");
				int max_number = rs.getInt("max_number");
				
				//随机生成max
				Random random = new Random();
				int max = 0;
				if(max_number > 0){
					max = random.nextInt(max_number);
				}
				
				//获取max，id
				pbsRent.setOperNum(max);
				pbsRent.setId(id);
				
				//设置max，id
				ps.setInt(1,pbsRent.getOperNum());
				ps.setInt(2,pbsRent.getId());
				
				//执行事务
				ps.execute();
			}
			System.out.println("=====表格更新结束");
			
			long end = System.currentTimeMillis();
			long time = (end-start);
			System.out.println("运行时间："+time);
			//事务提交
			conn.commit();
			//设置自动提交为true
			conn.setAutoCommit(true);
			
			//关闭连接
			rs.close();
			state.close();
			ps.close();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
