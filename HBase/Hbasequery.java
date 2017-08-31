package org.wse.webapp.resource;

import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.QueryParam;
import javax.inject.Singleton;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.WebApplicationException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareOp;
import org.apache.hadoop.security.UserGroupInformation;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;

import org.glassfish.jersey.server.mvc.Template;

@Path("/")
@Singleton
	@Template
@Produces(MediaType.APPLICATION_JSON)
	@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
	public class Hbasequery {

		private String name;
		private String retjson;
		private long lastRefreshTime;
		public  Configuration config;
		public  HTable FactTotalRegTable;
		public  HTable TMDeviceTable;
		public  HTable TMLicenseTable;
		public  HTable TMAccountTable;

		public enum Tables {
			INSERT,
				TMDEVICE,
				TMLICENSE,
				TMACCOUNT
		}

		private boolean RefreshConnection() {
			try {
				lastRefreshTime = System.currentTimeMillis();
				//System.out.println("RefreshConnection("+lastRefreshTime+")");
				config = HBaseConfiguration.create();
				TMDeviceTable = new HTable(config, "TMDevice");
				TMLicenseTable = new HTable(config, "TMLicense");
				TMAccountTable = new HTable(config, "TMAccount");
				return true;
			}catch(Exception e){
				System.out.println(e.toString());
				return false;
			}
		}

		public Hbasequery() {

			System.out.println("StandResource Initializa");
			System.setProperty("java.security.auth.login.config", "/usr/lib/hbase/conf/jaas-client.conf");
			RefreshConnection();
		}

		@GET
			@Path("{tableName}/{getparameter}")
			@Produces(MediaType.APPLICATION_JSON)
			public String getFactTotalReg(
					@PathParam(value="getparameter") String getparameter,
					@PathParam(value="tableName") String tableName,
					@QueryParam(value="q") String q) throws JSONException, IOException {

				CompareOp cc = CompareOp.valueOf("EQUAL");
				JSONObject jsonBody = new JSONObject();
				try {
					Get g = new Get(Bytes.toBytes(getparameter));
					Result r;
					long startTime = System.currentTimeMillis();
					if ( startTime - lastRefreshTime > 24*60*60*1000 ){
						RefreshConnection();
						System.out.println("Refresh Connection, Time:" + lastRefreshTime);
					}
					switch (Tables.valueOf(tableName.toUpperCase())) {
						case TMDEVICE: 
							r = TMDeviceTable.get(g);
							break;
						case TMLICENSE: 
							r = TMLicenseTable.get(g);
							break;
						case TMACCOUNT: 
							r = TMAccountTable.get(g);
							break;
						default: 
							r = TMDeviceTable.get(g);
							System.out.println("Unknown table name");
							break;
					}
					long endTime = System.currentTimeMillis();
					System.out.println("Hbase Data Fetch From " + startTime + " To "+ endTime + " Total Requires " + (endTime - startTime) + " Milliseconds");

					for(KeyValue kv : r.raw()){
						if (new String(kv.getFamily()).equals("UBM")){

							String[] items = new String(kv.getValue()).split(",");
							for (String item:items){	
								item = item.replaceAll("[\\[\\]{}()]","");
								String[] subItems = item.split("#");
								if ( 2 == subItems.length ){
									jsonBody.put((String) "UBM:" + subItems[0],subItems[1]);
								}
								else{
									System.out.println("ERROR: string format; " + new String(kv.getValue()));
								}
							}
						}
						else{
							jsonBody.put( new String(kv.getFamily()) + ":" + new String(kv.getQualifier()), new String(kv.getValue()));
						}
					}

				}catch(Exception e){
					System.out.println(e.toString());
				}
				if (jsonBody.toString().equals("{}")){
					ResponseBuilder builder = Response.status(204);
					builder.entity("Rowkey not found");
					Response response = builder.build();	
					throw new WebApplicationException(response);
				}

				return jsonBody.toString();
			}

		@PUT
			@Path("{tableName}")
			@Produces(MediaType.APPLICATION_JSON)
			public String insertData(@PathParam(value="tableName") String tableName, InputStream requestBodyStream) {
				JSONObject json = new JSONObject();
				try {

					JSONObject jsonInputStream = new JSONObject(this.convertRequestToString(requestBodyStream));
					JSONArray rows = jsonInputStream.getJSONArray("rows");
					List<Put> rowList = new ArrayList<Put>();

					for(int i = 0; i < rows.length(); i++){
						JSONObject row = rows.getJSONObject(i);
						String rowKey = row.getString("rk");
						String colfam = row.getString("cf");
						String qulifier = row.getString("qf");
						String value = row.getString("val");
						Put p = new Put(Bytes.toBytes(rowKey));
						p.add(Bytes.toBytes(colfam), Bytes.toBytes(qulifier), Bytes.toBytes(value));
						rowList.add(p);
					}

					Result[] results;

					switch (Tables.valueOf(tableName.toUpperCase())) {
						case TMDEVICE:
							TMDeviceTable.put(rowList);
							break;
						case TMLICENSE:
							TMLicenseTable.put(rowList);
							break;
						case TMACCOUNT:
							TMAccountTable.put(rowList);
							break;
						default:
							TMDeviceTable.put(rowList);
							System.out.println("Unknown table name");
							break;
					}
					/*					for(int i = 0; i < rows.length(); i++){

										String row = rows.getString(i);
										JSONObject jsonBody = new JSONObject();

					//for(KeyValue kv : results[i].raw()){
					//jsonBody.put( new String(kv.getFamily()) + ":" + new String(kv.getQualifier()), new String(kv.getValue()));

					jsonBody.put( "a","b" );
					//}
					json.put(row, jsonBody);
					}
					*/
					return "Insert Successfully!";
				} catch (Exception e) {

					System.out.println(e.toString());
					return "Fail to insert: " + e.toString();
				}
			}

		@POST
			@Path("{tableName}")
			@Produces(MediaType.APPLICATION_JSON)
			public String posttFactTotalReg(@PathParam(value="tableName") String tableName, InputStream requestBodyStream) {
				JSONObject json = new JSONObject();
				try {

					JSONObject jsonInputStream = new JSONObject(this.convertRequestToString(requestBodyStream));
					JSONArray keys = jsonInputStream.getJSONArray("keys");
					List<Get> queryRowList = new ArrayList<Get>();

					for(int i = 0; i < keys.length(); i++){
						String key = keys.getString(i);
						queryRowList.add(new Get(Bytes.toBytes(key)));
					}

					Result[] results;
					long startTime = System.currentTimeMillis();
					if ( startTime - lastRefreshTime > 24*60*60*1000 ){
						RefreshConnection();
						System.out.println("Refresh Connection, Time:" + lastRefreshTime);
					}
					switch (Tables.valueOf(tableName.toUpperCase())) {
						case TMDEVICE: 
							results = TMDeviceTable.get(queryRowList);
							break;
						case TMLICENSE: 
							results = TMLicenseTable.get(queryRowList);
							break;
						case TMACCOUNT: 
							results = TMAccountTable.get(queryRowList);
							break;
						default: 
							results = TMDeviceTable.get(queryRowList);
							System.out.println("Unknown table name");
							break;
					}
					long endTime = System.currentTimeMillis();
					System.out.println("Hbase Data Fetch From " + startTime + " To "+ endTime + " Total Requires " + (endTime - startTime) + " Milliseconds");

					for(int i = 0; i < keys.length(); i++){

						String key = keys.getString(i);
						JSONObject jsonBody = new JSONObject();

						for(KeyValue kv : results[i].raw()){
							if (new String(kv.getFamily()).equals("UBM")){

								String[] items = new String(kv.getValue()).split(",");
								for (String item:items){	
									item = item.replaceAll("[\\[\\]{}()]","");
									String[] subItems = item.split("#");
									if ( 2 == subItems.length ){
										jsonBody.put((String) "UBM:" + subItems[0],subItems[1]);
									}
									else{
										System.out.println("ERROR: string format; " + new String(kv.getValue()));
									}
								}
							}
							else{
								jsonBody.put( new String(kv.getFamily()) + ":" + new String(kv.getQualifier()), new String(kv.getValue()));
							}
						}
						json.put(key, jsonBody);
					}

				} catch (Exception e) {

					System.out.println(e.toString());
				}

				return json.toString();
			}

		//convert request inputstream to string 
		private String convertRequestToString(InputStream requestBodyStream){
			StringBuffer buffer = new StringBuffer();
			int bufferContent = 0;
			do{
				try {
					bufferContent = requestBodyStream.read();
					if(bufferContent > 0)
						buffer.append((char) bufferContent);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}while(bufferContent > 0 );

			return buffer.toString();
		}	
	}
