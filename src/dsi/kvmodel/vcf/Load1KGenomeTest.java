///@date 19/01/2016
///@author wsc
/// loading 1000 Genome VCF data

package dsi.kvmodel.vcf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.hbase.client.Put;


public class Load1KGenomeTest {
	protected static final String COL_FAMILY_ALT = "alt";
	protected static final String COL_FAMILY_FORMAT = "format";
	protected static final String COL_FAMILY_GENOTYPE = "genotype";
	protected static final String COL_FAMILY_INFO = "info";
	protected static final String COL_FAMILY_OTHER = "other";
	

	
	public Load1KGenomeTest(String tablename) {}
	
	public static void initHeaderTable() {}
	
	public static void initDataTable(String tablename) {}

	private void loadHeader(String header, String study) throws IOException {
		System.out.println("******************* Test loadHeader *******************");
		StringTokenizer tokenizer = new StringTokenizer(header, "#=<>,");
		String htc = tokenizer.nextToken();
		htc = htc.toLowerCase();
		
		
		if (htc.equals("info") || htc.equals("alt") || htc.equals("format")) {
			// data sample
			// ##INFO=<ID=LDAF,Number=1,Type=Float,Description="MLE Allele Frequency Accounting for LD">
			String firstElem = tokenizer.nextToken();
			if (firstElem.equals("ID")) {
				String id = tokenizer.nextToken();
				System.out.println("key:" + study);

				System.out.println("family:" + htc);
				System.out.println("qualifier:" + id);
				System.out.println("value:" + header.split("[<>]")[1]);
				System.out.println();

			} 
		} else {
			// data sample
			// ##fileformat=VCFv4.1
			String value = tokenizer.nextToken();
			System.out.println("key:" + study);
			System.out.println("family:" + COL_FAMILY_OTHER);
			System.out.println("qualifier:" + htc);
			System.out.println("value:" + value);
			System.out.println();
		}
	}
	
	private void loadColName(String line, List<String> subList) {
		System.out.println("******************* Test loadColName *******************");
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		tokenizer.nextToken();//String chrom = tokenizer.nextToken();
		tokenizer.nextToken();//String pos = tokenizer.nextToken();
		tokenizer.nextToken();//String rs = tokenizer.nextToken();
		tokenizer.nextToken();//String ref = tokenizer.nextToken();
		tokenizer.nextToken();//String alt = tokenizer.nextToken();
		tokenizer.nextToken();//String qual = tokenizer.nextToken();
		tokenizer.nextToken();//String filter = tokenizer.nextToken();
		tokenizer.nextToken();//String info = tokenizer.nextToken();
		tokenizer.nextToken();//String format = tokenizer.nextToken();
		while (tokenizer.hasMoreTokens()) {
			subList.add(tokenizer.nextToken());
		}
	}
	
	private void addSNP2SubjectPutList(String line, String study, List<String> subList, List<Put> putList) {
		System.out.println("******************* Test addSNP2SubjectPutList *******************");
		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String info = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		
		System.out.println("key:" + study + ":" + chrom + ":" + String.format("%8d", Long.parseLong(pos)));
		System.out.println("family:" + COL_FAMILY_INFO);
		System.out.println("qualifier:rs");
		System.out.println("value:" + rs);
		System.out.println("qualifier:ref");
		System.out.println("value:" + ref);
		System.out.println();
			
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();
			System.out.println("key:" + study + ":" + subList.get(i++));
			System.out.println("family:" + COL_FAMILY_GENOTYPE);
			System.out.println("qualifier:" + chrom + ":" + String.format("%8d", Long.parseLong(pos)));
			System.out.println("value:" + value);
		}	
	}
	
	private void addSNP2PositionPutList(String line, String study, List<String> subList, List<Put> putList) {
		System.out.println("******************* Test addSNP2PositionPutList *******************");
		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String info = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		System.out.println("key:" + study + ":" + chrom + ":" + String.format("%8d", Long.parseLong(pos)));
	
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();
			System.out.println("key:" + study + ":" + chrom + ":" + String.format("%8d", Long.parseLong(pos)));
			System.out.println("family:" + COL_FAMILY_GENOTYPE);
			System.out.println("qualifier:" + subList.get(i++));
			System.out.println("value:" + value);
		}	
	}
	
	private void addSNP2CrossPutList(String line, String study, List<String> subList, List<Put> putList) {
		System.out.println("******************* Test addSNP2CrossPutList *******************");
		// allele data
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");
		String chrom = tokenizer.nextToken();
		String pos = tokenizer.nextToken();
		String rs = tokenizer.nextToken();
		String ref = tokenizer.nextToken();
		String alt = tokenizer.nextToken();
		String qual = tokenizer.nextToken();
		String filter = tokenizer.nextToken();
		String info = tokenizer.nextToken();
		String format = tokenizer.nextToken();
		
		System.out.println("key:" + chrom + ":" + String.format("%8d", Long.parseLong(pos))  + ":" + study);
				
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			String value = tokenizer.nextToken();

		}	
	}
	
	public void insert(String study, String filename) {
		insert(study, filename, 500);
	}
	
	public void insert(String study, String filename, int cachesize) {
		System.out.println("******************* Test insert *******************");
		BufferedReader br = null;
		GZIPInputStream gzip = null;
		String line = null;
		System.out.println(filename);
		long ts1 = System.currentTimeMillis();
		System.out.println("start inserting main table at " + ts1);
		int count = 0;
		try {				
			List<String> subList = new ArrayList<String>();
			List<Put> subjectPutList = new ArrayList<Put>();
			List<Put> posPutList = new ArrayList<Put>();
			List<Put> crossPutList = new ArrayList<Put>();
			gzip = new GZIPInputStream(new FileInputStream(filename));
			br = new BufferedReader(new InputStreamReader(gzip));
			while ((line = br.readLine()) != null) {
				if (line.startsWith("##")) {
					loadHeader(line, study);
				} else if (line.startsWith("#")) {
					loadColName(line, subList);	
				} else {
					addSNP2SubjectPutList(line, study, subList, subjectPutList);
					addSNP2PositionPutList(line, study, subList, subjectPutList);
					addSNP2CrossPutList(line, study, subList, subjectPutList);
					count++;
					if (count % cachesize == 0) {
						subjectPutList.clear();

						posPutList.clear();

						crossPutList.clear();
					}
					if (count % 10000 == 0)
						System.out.println(count);
				}

				subjectPutList.clear();

				posPutList.clear();

				crossPutList.clear();
			}
			long ts2 = System.currentTimeMillis();
			System.out.println("finish time is " + (ts2 - ts1));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("final count is " + count);
			try {
				br.close();
				gzip.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("please input an argument");
			System.out.println("init for create new tables");
			//System.out.println("scan for scan a table and you also need input the table name, start row name, stop row name, patient number");
			System.out.println("insert for insert data into the tables, parameter table name and input data file name");
			//System.out.println("get for getting record");
			return;
		}
		
		if (args[0].equals("init")) {
			Load1KGenomeTest.initDataTable(args[1]);
			Load1KGenomeTest.initHeaderTable();
		} else if (args[0].equals("insert")) {
			Load1KGenomeTest loader = new Load1KGenomeTest(args[1]);
			loader.insert(args[2], args[3]);
			
		}
	}
}
