package org.whut.DCS.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DCSUtils {
	
	public static final String ENTRY_SEPARATOR=",";
	
	public static final int REDUCE_CONCURRENCY_VALUE=2;
	public static final int CRAWLER_DEEP=7;
	public static final long WAIT_JOB_FINISHED_MILLISECOND=5000;
	public static final int BLOOM_FILTER_VECTOR_SIZE=100000000;
	public static final int BLOOM_FILTER_FUNC_NUM=6;
	
	public static final String SEED_FILES_DIR="crawler/in";
	public static final String SEED_FILES_TMP_DIR="crawler/tmp_in";
	
	public static final String CRAWLER_RESULT_TMP_DIR="crawler/crawler_tmp_dir";
	public static final String UPDATE_BLOOM_FILTER_JOB_OUTPUT_DIR="crawler/ubf_outputdir";
	public static final String CRAWLER_RESULT_DIR="crawler/doc";
	public static final String BLOOM_FILTER_FILE_NAME="crawler/bloomfilter";
	public static final String COOCURRENCE_RESULT_DIR="crawler/crd";
	public static final String CRAWLER_STAND_RESULT_DIR="crawler/csrd";
	
	public static final String MYSQL_HOSTNAME="127.0.0.1";
	public static final String MYSQL_PORT="3306";
	public static final String MYSQL_USERNAME="root";
	public static final String MYSQL_PASSWORD="123qaz";
	
	public static final String MYSQL_DATABASE_NAME="entrymanagement";
	public static final String ENTRIES_TABLE_NAME="entries";
	
	public static final String insertEntrySQL="INSERT INTO "+ENTRIES_TABLE_NAME+" (entry) VALUES (?);";
	public static final String selectEntryIdSQL="SELECT id from "+ENTRIES_TABLE_NAME+" WHERE entry=?;";
	
	public static String decoder(String url){				
		try {
			return URLDecoder.decode(url, "UTF-8");
		} catch (UnsupportedEncodingException e) {			
			e.printStackTrace();		
		}
		return null;
	}
	
	public static Connection getConn(String hostname,String port,String databaseName,String username,String password){
		String url="jdbc:mysql://"+hostname+":"+port+"/"+databaseName+"?characterEncoding=UTF-8";
		
		
		Connection conn=null;
		 try {   
	            Class.forName("com.mysql.jdbc.Driver" );   
	            return DriverManager.getConnection( url,username, password );   
	            }  
	         catch ( ClassNotFoundException cnfex ) {  
	     
	             cnfex.printStackTrace();   
	         }   

	         catch (SQLException sqlex ) {  
	         
	             sqlex.printStackTrace();   
	         }
		return conn;  		
	}
	
	public static void deconnSQL(Connection conn) {  
        try {  
            if (conn != null)  
                conn.close();  
        } catch (Exception e) {  
           
            e.printStackTrace();  
        }  
    }
	
	public static int getEntryId(Connection conn,String entry){
		 PreparedStatement statement = null; 
		 try{
			 statement=conn.prepareStatement(selectEntryIdSQL);
			 statement.setString(1, entry);
			 ResultSet result=statement.executeQuery();
			 if(result.next()){
				 return result.getInt("id");
			 }else{
				 return -1;
			 }
		 }catch(SQLException e){
		 }
		 return -1;
	}
	
	public static void insertEntry(Connection conn,String entry){
		PreparedStatement statement = null; 
		try {
			statement=conn.prepareStatement(insertEntrySQL);
			statement.setString(1, entry);
			statement.executeUpdate();  
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}	
}
