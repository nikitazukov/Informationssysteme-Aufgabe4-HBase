package hbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.csvreader.CsvReader;




public class TableOperations{
	//vars
	private Configuration config;
	private HTable table;
	private Scan scan;
	private ResultScanner scanner;
	private Put put;
	
	//readCsv - insertData vars
	private String filmfamily ="filmfamily";
	private String film ="film";
	private String characterfamily ="characterfamily";
	private String character ="character";
	private String counterfamily="counterfamily";
	private String counter="counter";
	
	//find actor with most roles
	private int count_max_roles=0;
	private List<KeyValue> actor_max_roles;
	private int empty_count=0;
	private int err_count=0;
	

	   // Aufgabe 2 d -> insert some data into created table 'test' 
	   public void insertData(String tab, String row, String family, String qualifier, String value) {
		   try {
			   
			   config  = HBaseConfiguration.create();
			   
			   table = new HTable(config, tab);
			   
		       put = new Put(Bytes.toBytes(row)); 

		       put.add(Bytes.toBytes(family),
				Bytes.toBytes(qualifier),Bytes.toBytes(value));
		       
		       table.put(put);
		      
		       table.close();
		   
		   }catch(IOException e) {
			   System.out.println("Fehler bei insertData");
		   }
	   }
	
	   //Aufgabe 2 e -> show all data (rows & their input) from created table 'test'
	   public void completeTable(String tab){

		   try {

			   config  = HBaseConfiguration.create();
			   
			   table = new HTable(config, tab);

			   scan = new Scan();
		
		       scanner = table.getScanner(scan);
		      
		      // Reading values from scan result
		      for (Result result = scanner.next(); result != null; result = scanner.next())
		    	  
		      System.out.println("Row : " + result);
		      
		      //closing the scanner
		      scanner.close();
	      
		   }catch(IOException e) {
			   System.out.println("Fehler bei completeTable");
		   }
	   }
   
	   //Aufgabe 3 a read data from file and put it into hbase or count the data
   public void readCsvToInputDataOrCount(String data_or_count) {
	   try {
		  
			
			CsvReader items = new CsvReader("/home/nikita/Schreibtisch/eclipse/performance.csv");
			items.readHeaders();
			
			config  = HBaseConfiguration.create();
			table = new HTable(config, "actors");
			
			while (items.readRecord())
			{	err_count++;
			
				String actor = items.get("actor");
				String film_value = items.get("film");
				String character_value= items.get("character");
				
				if(data_or_count.equals("data")) {
					insertData("actors", actor, filmfamily, film, film_value, characterfamily, character, character_value );
				}else if(data_or_count.equals("count")) { 
					getAllCharacterVersionsOneActor("actors", actor,"characterfamily","character");
				}
				
			}
	
			items.close();
			table.close();
			
			System.out.println(actor_max_roles);
			System.out.println("Schauspieler mit den meisten Rollen: " + count_max_roles);
		
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println(err_count);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(err_count);
		}
   }
   
   //Aufgabe 3 a insert data into hbase
   public void insertData(String tab, String row,String family, String qualifier, String value, String family2, String qualifier2, String value2) {
	  
	   try {
			
		   //Fehlerhandling -> if row(actor) not existing we use a count var
		   empty_count++;
		   if(row.equals("")) {
			   row = String.valueOf(empty_count);
		   }
	       put = new Put(Bytes.toBytes(row)); 

	       //check if values (filmtitle or role of actor) of families not null
	     
	       put.addColumn(Bytes.toBytes(family),
			Bytes.toBytes(qualifier),Bytes.toBytes(value));
	      
	       
	       if(!value2.equals("")) {
	       put.addColumn(Bytes.toBytes(family2),
	   			Bytes.toBytes(qualifier2),Bytes.toBytes(value2));
	       }
	       
	       table.put(put);
	      
	       
	   
	   }catch(IOException e) {
		   System.out.println("Fehler bei insertData");
	   }
   }
   
   // Aufgabe 3 b get the versions of the character from one actor and save it as as counter (roles)
   public void getAllCharacterVersionsOneActor(String tab, String row, String family, String qualifier) {
		  
	   try {

		   Get q= new Get(Bytes.toBytes(row));
		   q.setMaxVersions(1000);
		   
		   Result res = table.get(q);
		   byte[] b = res.getValue(Bytes.toBytes(family),
			Bytes.toBytes(qualifier));
		   List<KeyValue> kv = res.getColumn(Bytes.toBytes(family),
			Bytes.toBytes(qualifier));
		   
		   //Aufgabe 3 c counting the versions of roles and saves the max counter
		   int count=0;
		   for(KeyValue kvitems : kv) {
			   count++;
		   }
		   
		   if(count>count_max_roles) {
			   count_max_roles = count;
			   actor_max_roles = kv;
		   }
		   
		   insertCountData(tab, row, counterfamily, counter, count);		   
	       
	   
	   }catch(IOException e) {
		   System.out.println("Fehler bei insertData");
	   }
   }
   
  
   // 3 b insert counted versions of actor roles and put it into the count column of the actor
   public void insertCountData(String tab, String row,String family, String qualifier, int value) {
		  
	   try {
	       put = new Put(Bytes.toBytes(row)); 
	     
	       put.addColumn(Bytes.toBytes(family),
			Bytes.toBytes(qualifier),Bytes.toBytes(value));
	       
	       table.put(put);
	      	   
	   }catch(IOException e) {
		   System.out.println("Fehler bei insertCountData");
	   }
   }
}