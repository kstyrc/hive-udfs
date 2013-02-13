package com.swisscom.bigdata.hive;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import au.com.bytecode.opencsv.CSVReader;


/**
 * Hive UDF to map input values into classes using (regex, class-name) pairs
 * specified in CSV file. A user specifies a CSV file with the following 
 * structure:
 * "<class-name1>", "<regex1-1>", "<regex1-2>", ...
 * "<class-name2>", "<regex2-1>", "<regex2-2>", ...
 * ... ... ... ...
 * 
 * If the input string matches ANY regex, the associated class-name is
 * returned, otherwise the original value is returned.
 * is returned.
 * 
 * Note the quotes around class-names and regexes - it is important, as
 * regex can contain commas.
 * 
 * Regex are in Java language format.
 * 
 * @author kstyrc
 *
 */
@Description(name = "regex_to_class", 
value = "_FUNC_(r, v) - Classifies object v into matching class according to the specified" +
		" array of tuples (regex, class) r")
public class RegexToValueUDF extends GenericUDF {
	
	protected Log log = LogFactory.getLog(getClass());
	
	
	protected ObjectInspectorConverters.Converter inputConverter;
	protected ObjectInspectorConverters.Converter fileConverter;
	
	protected ArrayList<SimpleEntry<Pattern, String>> patterns = null;
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		
		// defensive check - UDF takes exactly two string arguments!
		if (args.length != 2 || 
			!(args[0].getCategory() == ObjectInspector.Category.PRIMITIVE) ||
			!(args[1].getCategory() == ObjectInspector.Category.PRIMITIVE)) {
			
			throw new UDFArgumentException(
					"regex_to_class() takes two arguments: a string filename containing (regex, value) mappings " +
					"and a primitive input string");
		}
		
		// building converters for retrieving string values from the DeferredObject objects
		fileConverter = ObjectInspectorConverters.getConverter(args[0],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		inputConverter = ObjectInspectorConverters.getConverter(args[1],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}
	
	@Override
	public Text evaluate(DeferredObject[] args) throws HiveException {
		
		// defensive check - as in other UDF examples
		if (args[0].get() == null || args[1].get() == null)
			return null;
		
		// retrieving arguments using converters
		Text path = (Text) fileConverter.convert(args[0].get());
		String input = ((Text) inputConverter.convert(args[1].get())).toString();
		
		// lazily initialize compiled table of tuples (regex, class-name)
		if (patterns == null) {
			readPatternsFromCSV(path);
			log.info("read patterns from file: " + path);
		}
		
		// check input value against every pattern retrieved from the CSV file
		for (SimpleEntry<Pattern, String> entry : patterns) {
			if (entry.getKey().matcher(input).matches())
				return new Text(entry.getValue());
		}

		// if no match, return the original string
		return new Text(input);
	}

	/**
	 * Read mapping tuples (regex, class-name) from the specified CSV file
	 * The CSV file format is explained in the comments for the class.
	 * 
	 * @param path
	 * 			csv file path
	 */
	private void readPatternsFromCSV(Text path) {

		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(path.toString()));
			patterns = new ArrayList<SimpleEntry<Pattern,String>>();

			String[] values = null;
			
			while ((values = reader.readNext()) != null) {
				
				String mapping = values[0];
				for (int i = 1; i < values.length; i++) {
					log.info("Compiling entry: <" + values[i] + ", " + mapping + ">");
					patterns.add(new AbstractMap.SimpleEntry<Pattern, String>(Pattern.compile(values[i]), mapping));
				}
			}
			
		} catch (FileNotFoundException e) {
			log.warn("File was not found: " + path, e);
			
		} catch (IOException e) {
			log.warn("Exception while reading the file: " + path, e);
			
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				log.warn("Unable to close the reader of the file:" + path, e);
			}
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return "regex_to_class(" + children[0] + "," + children[1] + ")";
	}
}
