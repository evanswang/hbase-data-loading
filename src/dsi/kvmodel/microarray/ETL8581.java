package dsi.kvmodel.microarray;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ETL8581 extends HBaseM {

	
	private Map<String, String> sample2subjectMap;
	private String sqlUrl;

	public ETL8581(String table, String ip, String port, String user, String pass) {
		super(table);
		this.sqlUrl = "jdbc:postgresql://" + ip + ":" + port
				+ "/transmart?user=" + user + "&password=" + pass
				+ "&autoReconnect=true";
	}
	
	public void loadMappingFile(String mapFileName) {
		System.out.println("******* loading map file");
		BufferedReader mapIn = null;
		String line = null;
		this.sample2subjectMap = new HashMap<String, String>();
		try {
			mapIn = new BufferedReader(new FileReader(mapFileName));
			// remove header
			mapIn.readLine();
			while ((line = mapIn.readLine()) != null) {
					// study_id	site_id	subject_id	SAMPLE_ID	PLATFORM	
					// TISSUETYPE	ATTR1	ATTR2	category_cd
				
					//STUDY_ID SITE_ID SUBJECT_ID SAMPLE_CD PLATFORM
					//TISSUETYPE      ATTRITBUTE_1    ATTRITBUTE_2    
					//CATEGORY_CD     SOURCE_CD
					String[] items= line.split("\t");
					String study_id = items[0];
					//String site_id = stin.nextToken();
					String subject_id = items[2];
					String sample_id = items[3];
					//String platform = stin.nextToken();
					//String tissue_type = stin.nextToken();
					//String attr1 = stin.nextToken();
					//String attr2 = stin.nextToken();
					//String category_cd = stin.nextToken();
					
					sample2subjectMap.put(sample_id, study_id + ":" + subject_id);		
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				mapIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private List<String> getSampleList(String header) {
		System.out.println("******** get sample list");
		List<String> sampleList = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(header, "\t");
		st.nextToken();
		while (st.hasMoreTokens()) {
			sampleList.add(st.nextToken());
		}
		return sampleList;
	}
	
	private List<String> getPatientList(List<String> sampleList) {
		System.out.println("******** get patient list");
		List<String> patientList = new ArrayList<String>();
		for (String sample : sampleList) {
			String study_subject = sample2subjectMap.get(sample);
			StringTokenizer stin = null;
			try {
				stin = new StringTokenizer(study_subject, ":");
			} catch (Exception e1) {
				System.err.println("sample is " + sample);
				e1.printStackTrace();
			}
			String study = stin.nextToken();
			String subject_id = stin.nextToken();
			
			Connection conn = null;
			Statement stmt = null;
			ResultSet rs = null;
			
			try {
				conn = DriverManager.getConnection(sqlUrl);
				stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				//stmt.setFetchSize(Integer.MIN_VALUE);
				String sql = "SELECT patient_id, concept_code from deapp.de_subject_sample_mapping where trial_name = '"
						+ study + "' and subject_id = '" + subject_id + "' and sample_cd = '" + sample + "'";
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String patient_id = rs.getLong("patient_id") + "";
					String concept_code = rs.getString("concept_code");
					patientList.add(study + ":" + patient_id + ":" + concept_code);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return patientList;
	}
	
	public void loadOmicsData (String folderName) {
		loadOmicsData(folderName, 30000);
	}
	
	public void loadOmicsData (String folderName, long cachesize) {
		System.out.println("******** loadOmicsData");
		for (File fileEntry : new File(folderName).listFiles()) {
	        if (fileEntry.isDirectory()) {
	            continue;
	        } else {
	            if (!fileEntry.getName().startsWith("GSE8581"))
	            	continue;
	            BufferedReader omicsIn = null;
	    		String line = null;
	    		try {
	    			System.out.println(fileEntry.getName());
	    			omicsIn = new BufferedReader(new FileReader(fileEntry));
	    			String header = omicsIn.readLine();
	    			List<String> sampleList = getSampleList(header);
	    			List<String> patientList = getPatientList(sampleList);
	    			List<Put> putList = new ArrayList<Put>();
    				
    				int count = 0;
	    			while ((line = omicsIn.readLine()) != null) {
	    				int i = 0;
	    				StringTokenizer st = new StringTokenizer(line, "\t");
	    				String probe = st.nextToken();
	    				while(st.hasMoreTokens()) {
	    					//add insert
	    					String raw = st.nextToken();
	    					String log = null;
	    					if (raw.equals(".") || raw.equals("0")) {
	    						raw = "0";
	    					}
	    					count ++;
	    					    
	    					Put p = new Put(Bytes.toBytes(patientList.get(i++)));
	    					p.add(Bytes.toBytes(COL_FAMILY_RAW),
	    							Bytes.toBytes(probe), Bytes.toBytes(raw));
	    					p.add(Bytes.toBytes(COL_FAMILY_LOG),
	    							Bytes.toBytes(probe), Bytes.toBytes(raw));
	    					p.add(Bytes.toBytes(COL_FAMILY_ZSCORE),
	    							Bytes.toBytes(probe), Bytes.toBytes(raw));
	    					putList.add(p);
	    					if (count % cachesize == 0) {
	    						System.out.println(count);
	    						MicroarrayTable.put(putList);
	    						putList.clear();
	    						try {
	    							Thread.sleep(100);
	    							System.gc();
	    						} catch (InterruptedException e) {
	    							e.printStackTrace();
	    						}
	    					}
	    				}
	    				//System.out.println("count is " + count);
	    			}
	    			MicroarrayTable.put(putList);
	    			System.out.println("count is " + count);
	    		} catch (FileNotFoundException e) {
	    			e.printStackTrace();
	    		} catch (IOException e) {
	    			e.printStackTrace();
	    		} finally {
	    			try {
	    				omicsIn.close();
	    			} catch (IOException e) {
	    				e.printStackTrace();
	    			}
	    		}
	        }
	    }
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ETL8581 etl = new ETL8581("microarray", "localhost", "5432", "postgres", "postgres");
		etl.init("microarray");
	
		etl.loadMappingFile("/data/transmart-data/samples/studies/GSE8581/expression/subject_sample_mapping.tsv");
		etl.loadOmicsData(args[5]);	
	}

}
