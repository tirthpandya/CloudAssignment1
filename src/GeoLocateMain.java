import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.xml.validation.Schema;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Table;
import org.hsqldb.lib.Iterator;
import org.json.*; //taken from json.org


public class GeoLocateMain {

	public static String GOOGLE_GEOCODE_API_URL = "https://maps.googleapis.com/maps/api/geocode/";
	public static String GOOGLE_GEOCODE_API_RESP_FORMAT = "json";
	public static String GOOGLE_GEOCODE_API_KEY = "AIzaSyCdTIuspmFfTAI7U4L6r2y9mXD5NovK8t8";
	public static double EARTH_RADIUS = 3979d;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Map<String,Object> location = new HashMap<String,Object>();
			
			/*
			 * Take input from console.
			 */
			Map<String, Object> addressMapFromConsole = getConsoleAddressInput();
			if(!addressMapFromConsole.isEmpty())
			{
				System.out.println(addressMapFromConsole.get("address"));
			String addressFromConsole = (String)addressMapFromConsole.get("address");
			//Need better error handling - Pending
			location = getLocationFromAddress(addressFromConsole);
			System.out.println(location.get("lat"));
			System.out.println(location.get("lng"));
			}else{
				//error handling --pending
			}
			
		
		
		/**
		 * Read the csv file to query all the addresses that are close +/-2 Zips
		 * Override update - read only the entries that belong the one zip
		 */
		
		File csvFile = null;
		String zipForQuery = "";
		InputStream inputStream= null;
		BufferedReader br= null;
		try {
			//URL csvfile= new URL("http://www.cs.cornell.edu/Courses/CS5412/2015sp/_cuonly/restaurants_all.csv");
			csvFile = new File("C:/Users/Tirth/Downloads/restaurants_all.csv");
			//URLConnection csvConn = csvfile.openConnection();
			inputStream = new FileInputStream(csvFile);
			//inputStream = csvfile.openStream();
			System.out.println("file opened");
			//br = new BufferedReader(inputStream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		CsvConfiguration configuration = new CsvConfiguration(1);
		
        DataContext dataContext = DataContextFactory.createCsvDataContext(inputStream, configuration);
      //  Schema schema = (Schema) dataContext.getSchemaNames()DefaultSchema();
      //  org.apache.metamodel.schema.Schema[] schema = dataContext.getSchemas();
       // System.out.println(dataContext.getDefaultSchema().getName());
        Table[] tables = dataContext.getDefaultSchema().getTables();
       // System.out.println(tables.length);
         Table table = tables[0];
         System.out.println(table.getColumnNames()[8]);
         //Need to handle the first row query since there are no headers in the csv
         if(location.get("zip").toString().substring(0, 1).equals("0"))
        	 zipForQuery = location.get("zip").toString().substring(1, location.get("zip").toString().length());
         else
        	 zipForQuery = location.get("zip").toString();
        System.out.println(zipForQuery);
         DataSet dataSet = dataContext.query().from(table).selectAll().where("29115").eq(zipForQuery).execute();
       // DataSet dataset = dataContext.query();
         List<Row> rows = dataSet.toRows();
         //Row row = rows.get(0);
        // System.out.println("before bulk query"+row.getValue(4)+ (String)row.getValue(5)+row.getValue(6));
       // Schema schema = csvContext.getDefaultSchema();
         
         List<HashMap> bulkLocationReturn = getBulkLocation(rows);
         System.out.println(bulkLocationReturn.get(10).get("address"));
         List<HashMap> bulkDistanceReturn = getLocationDistance(bulkLocationReturn,location);
         
         //Print the output by comparing the distance values
         @SuppressWarnings("rawtypes")
		ListIterator<HashMap> it = bulkDistanceReturn.listIterator();
         while(it.hasNext())
         {
        	 Map<String, Object> item = (HashMap<String, Object>)it.next();
        	 if ((Double)item.get("distance") <= (double)addressMapFromConsole.get("radius")) {
				System.out.println(item.get("address")+"\t"+item.get("distance")+" miles");
			}
         }
	}

	private static List<HashMap> getLocationDistance(
			List<HashMap> bulkLocationReturn, Map queryLocation) {
		// TODO Auto-generated method stub
		
		Double radMultiplier = Math.PI / 180 ;
		//ListIterator it = bulkLocationReturn.listIterator();
		//while(it.hasNext())
		for(int i=0;i<bulkLocationReturn.size();i++)
		{
			Map<String, Object> locationItem = bulkLocationReturn.get(i);
			//System.out.println(locationItem.get("lat").toString()+"===>"+locationItem.get("address"));
			double distance = (Math.acos(
					Math.sin((Double)queryLocation.get("lat")*radMultiplier)* Math.sin((Double)locationItem.get("lat")*radMultiplier)
				+   Math.cos((Double)queryLocation.get("lat")*radMultiplier)* Math.cos((Double)locationItem.get("lat")*radMultiplier)
				*   Math.cos((Double)queryLocation.get("lng")*radMultiplier - (Double)locationItem.get("lng")*radMultiplier))) * EARTH_RADIUS;
			locationItem.put("distance", distance);
			bulkLocationReturn.set(i, (HashMap) locationItem);
		}
		
		return bulkLocationReturn;
	}

	private static List<HashMap> getBulkLocation(List<Row> rows) {
		// TODO Auto-generated method stub
		
		BufferedReader in = null;
		
		List<HashMap> returnVal = new ArrayList<HashMap>();
		int i = 0;
		Row row = null;
		String line = "";
		String strForParsing = "";
		String inBuffer = "http://www.mapquestapi.com/geocoding/v1/batch?key=Fmjtd%7Cluu8216tl1%2Cal%3Do5-942s1f&callback=renderBatch";
		StringBuilder urlString = new StringBuilder();
		urlString.append("http://www.mapquestapi.com/geocoding/v1/batch?key=Fmjtd%7Cluu8216tl1%2Cal%3Do5-942s1f&callback=renderBatch");
		
		ListIterator it =  rows.listIterator();
		while(it.hasNext())
		{
			row = (Row)it.next();
			
			//urlString.append(i++);
			inBuffer += ("&location="+row.getValue(4)+" "+row.getValue(5)+" "+row.getValue(6));
			urlString.append("&location=").append(row.getValue(4).toString()).append(row.getValue(5).toString()).append(row.getValue(6).toString());
			//System.out.println(inBuffer);
		}
		
		//System.out.println(inBuffer);
		//The address string from the console will have blank spaces in between.
		// Need to replace them with "+" sign in the URL
		//String urlFinalStr = urlString.toString().replace(" ", "%20");
		String urlFinalStr = inBuffer.replace(" ", "%20");
		try {
			URL url = new URL(urlFinalStr);
			try {
				 in = new BufferedReader(new InputStreamReader(url.openStream()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				while((line = in.readLine()) != null)
				{
					strForParsing += line+"\n";
					
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println(strForParsing.substring(12, strForParsing.length()-1));
			JSONObject locationObject = null;
		JSONObject object  = new JSONObject(strForParsing.substring(12, strForParsing.length()-1));
		JSONArray bulkResultArray = object.getJSONArray("results");
		for(i=0;i<bulkResultArray.length();i++)
		{
			Map<String, Object> locationMap = new HashMap<String,Object>();
			locationObject = bulkResultArray.getJSONObject(i).getJSONArray("locations").getJSONObject(0).getJSONObject("latLng");
			locationMap.put("lat",Double.parseDouble(locationObject.get("lat").toString()));
			locationMap.put("lng",Double.parseDouble(locationObject.get("lng").toString()));
			locationMap.put("address",bulkResultArray.getJSONObject(i).getJSONObject("providedLocation").get("location").toString());
			returnVal.add((HashMap<String,Object>) locationMap);
		}
		
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); //better error handling - pending
		}
		
		return returnVal;
	}

	private static Map<String,Object> getConsoleAddressInput() {
		// TODO Auto-generated method stub
		String line = "";
		Map<String,Object> returnVal = new HashMap<String,Object>();
		BufferedReader is = new BufferedReader(new InputStreamReader(System.in));
		try {
			line = is.readLine();
			returnVal.put("address", line);
			line = ""; //Clear the previous value.
			line = is.readLine();
			returnVal.put("radius",Double.parseDouble(line));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); // Better error handling --pending
		}
		return returnVal;
	}

	/**
	 * The method is static so that it could be used otherwise as well.
	 * There are no instance level dependencies in this method
	 * @param address
	 * @return MAP with keys lat lng and zip(for cases when we dont get one from console input
	 */
	public static Map<String,Object> getLocationFromAddress(String address)
	{
		Map<String,Object> returnVal = new HashMap<String,Object>();
		BufferedReader in = null;
		String line = "";
		String strForParsing = "";
		StringBuilder inBuffer = new StringBuilder();
		StringBuilder urlString = new StringBuilder();
		urlString.append(GOOGLE_GEOCODE_API_URL).append(GOOGLE_GEOCODE_API_RESP_FORMAT)
				 .append("?address=").append(address).append("key").append(GOOGLE_GEOCODE_API_KEY);
		//The address string from the console will have blank spaces in between.
		// Need to replace them with "+" sign in the URL
		String urlFinalStr = urlString.toString().replace(" ", "+");
		try {
			URL url = new URL(urlFinalStr);
			try {
				 in = new BufferedReader(new InputStreamReader(url.openStream()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				while((line = in.readLine()) != null)
				{
					strForParsing += line+"\n";
					
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		JSONObject object  = new JSONObject(strForParsing);
		JSONObject results = (JSONObject)object.getJSONArray("results").get(0);
		returnVal.put("zip", (String)results.getJSONArray("address_components").getJSONObject(6).getString("long_name"));
		//System.out.println(results.getJSONArray("address_components").getJSONObject(6).getString("long_name"));
		returnVal.put("lat", (Double)results.getJSONObject("geometry").getJSONObject("location").getDouble("lat"));
		returnVal.put("lng", (Double)results.getJSONObject("geometry").getJSONObject("location").getDouble("lng"));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); //better error handling - pending
		}
		return returnVal;
	}
}
