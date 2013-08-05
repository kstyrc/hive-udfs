package com.swisscom.bigdata.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.com.bytecode.opencsv.CSVReader;

public class PatternReader {
	
	protected static Log log = LogFactory.getLog(PatternReader.class);
	
	public List<Entry<Pattern, String>> readPatterns(String path) {
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(path));
			return readPatterns(reader);
			
		} catch (FileNotFoundException e) {
			String error = String.format("Mapping file not found [%s]", path);
			log.error(error, e);
			throw new RuntimeException(error, e);
			
		} catch (IOException e) {
			String error = String.format("Error while reading mapping file [%s]", path);
			log.error(error, e);
			throw new RuntimeException(error, e);
			
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				log.warn(String.format("Unable to close mapping file [%s", path), e);
			}
		}
	}

	private List<Entry<Pattern, String>> readPatterns(CSVReader reader) throws IOException {
		List<Entry<Pattern, String>> patterns = new ArrayList<Entry<Pattern,String>>();
		
		String[] values = null;
		while ((values = reader.readNext()) != null) {
			String mapping = values[0];
			for (int i = 1; i < values.length; i++) {
				log.info(String.format("Compiling entry: <%s, %s>", values[i], mapping));
				Pattern pattern = Pattern.compile(values[i]);
				patterns.add(new SimpleEntry<Pattern, String>(pattern, mapping));
			}
		}
		return patterns;
	}
}
