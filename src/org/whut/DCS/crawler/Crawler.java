package org.whut.DCS.crawler;

import static org.whut.DCS.utils.DCSUtils.*;

import java.sql.Connection;
import java.util.Map.Entry;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawler {
	
	private Text documentURL;
	private Text entry; 
	private MapWritable result=new MapWritable();
	
	public Crawler(Text documentURL, Text entry) {
		super();
		this.documentURL = documentURL;
		this.entry = entry;
	}

	public Text getDocumentURL() {
		return documentURL;
	}

	public Text getEntry() {
		return entry;
	}

	public MapWritable getResult() {
		return result;
	}	
	public void run(String documentURL) throws Exception{				
		
	}
	
	
	public boolean start() {		
		
		try{
		Document doc=Jsoup.connect(documentURL.toString()).get();
		Elements ps=doc.getElementsByTag("p");
		for(Element p:ps){
			Elements urls=p.select("a[href]");			
			for(Element url:urls){				
				if(url.hasClass("new"))				
					continue;					
				url.setBaseUri("http://en.wikipedia.org");
				String u=url.baseUri()+decoder(url.attr("href"));
				if(u.contains("#"))
					continue;
				String entry=url.text();			
				result.put(new Text(u),new Text(entry));				
			}
		}
			return true;
		}catch (Exception e) {
			// TODO: handle exception
		}
		return false;		
	}
	
	public static void main(String args[]){
		Crawler crawler=new Crawler(new Text("http://en.wikipedia.org/wiki/C_%28programming_language%29"),new Text("C (programming language)"));
		crawler.start();
		MapWritable map=crawler.getResult();
		for(Entry<Writable, Writable> set:map.entrySet()){
			Connection conn=getConn(MYSQL_HOSTNAME, MYSQL_PORT, MYSQL_DATABASE_NAME, MYSQL_USERNAME, MYSQL_PASSWORD);
			insertEntry(conn, set.getValue().toString());
		}
	}
}
