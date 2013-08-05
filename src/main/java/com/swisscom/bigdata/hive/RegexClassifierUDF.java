package com.swisscom.bigdata.hive;

import static com.swisscom.bigdata.utils.HiveUtils.isPrimitive;

import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.swisscom.bigdata.utils.PatternReader;
import com.swisscom.bigdata.utils.ValidationUtils;


/**
 * 
 * Regex classifier matches input string values into classes. It takes
 * CSV file with (regex, class_name) pairs as the following:
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
@Description(
	name = "regex_classifier",
	value = "_FUNC_(r, v) - Classifies object v into matching class according to the specified" +
			" array of tuples (regex, class) r"
)
public class RegexClassifierUDF extends GenericUDF {
	
	protected ObjectInspectorConverters.Converter inputConverter;
	protected ObjectInspectorConverters.Converter fileConverter;
	
	protected List<Entry<Pattern, String>> patterns = null;
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (invalidArgs(args)) {
			throw new UDFArgumentException(
					"regex_classifier() takes two arguments: a string filename containing (regex, value) mappings " +
					"and a primitive input string");
		}
		
		fileConverter = ObjectInspectorConverters.getConverter(args[0],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		inputConverter = ObjectInspectorConverters.getConverter(args[1],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	private boolean invalidArgs(ObjectInspector[] args) {
		return args.length != 2 || 
				!isPrimitive(args[0]) ||
				!isPrimitive(args[1]);
	}
	
	@Override
	public Text evaluate(DeferredObject[] args) throws HiveException {
		Object mappingFile = args[0].get();
		Object value = args[1].get();
		
		if (ValidationUtils.isAnyNull(mappingFile, value)) {
			return null;
		}
		
		return evaluate(mappingFile, value);
	}

	private Text evaluate(Object mappingFile, Object value) throws HiveException {
		String path  = ((Text) fileConverter.convert(mappingFile)).toString();
		String input = ((Text) inputConverter.convert(value)).toString();
		
		lazilyInitPatterns(path);
		return classify(input);
	}

	private Text classify(String input) {
		for (Entry<Pattern, String> entry : patterns) {
			if (entry.getKey().matcher(input).matches())
				return new Text(entry.getValue());
		}
		return new Text(input);
	}

	private void lazilyInitPatterns(String path) {
		if (patterns == null) {
			PatternReader reader = new PatternReader();
			patterns = reader.readPatterns(path);
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return String.format("regex_classifier(%s, %s)", children[0], children[1]);
	}
}
