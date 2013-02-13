package com.swisscom.bigdata.hive;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(name = "regex_to_class", 
value = "_FUNC_(string_column, hash_type, hash_salt) - returns hash value for the input string")
public class HashUDF extends GenericUDF {

	protected ObjectInspectorConverters.Converter inputConverter;
	protected ObjectInspectorConverters.Converter hashTypeConverter;
	protected ObjectInspectorConverters.Converter hashSaltConverter;
	
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		// defensive check - as in other UDF examples
		if (args[0].get() == null || args[1].get() == null || args[2].get() == null)
			return null;
		
		// retrieving arguments using converters
		String text     = ((Text)    inputConverter.convert(args[0].get())).toString();
		String hashType = ((Text) hashTypeConverter.convert(args[1].get())).toString().toUpperCase();
		String hashSalt = ((Text) hashSaltConverter.convert(args[2].get())).toString();
		
		// hash the value
		String input = text + hashSalt;
		if (hashType.equalsIgnoreCase("md5")) {
			return new Text(DigestUtils.md5Hex(input));
		} else if (hashType.equalsIgnoreCase("sha1")) {
			return new Text(DigestUtils.shaHex(input));
		} else if (hashType.equalsIgnoreCase("sha256")) {
			return new Text(DigestUtils.sha256Hex(input));
		} else if (hashType.equalsIgnoreCase("sha512")) {
			return new Text(DigestUtils.sha256Hex(input));
		} else {
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return "hash(" + children[0] + "," + children[1] + "," + children[2] + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 3 || 
				!(args[0].getCategory() == ObjectInspector.Category.PRIMITIVE) ||
				!(args[1].getCategory() == ObjectInspector.Category.PRIMITIVE) ||
				!(args[2].getCategory() == ObjectInspector.Category.PRIMITIVE)) {
				
				throw new UDFArgumentException(
						"hash() takes three arguments: a string column to hash, hash type and salt");
		}
		
		inputConverter = ObjectInspectorConverters.getConverter(args[0],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		hashTypeConverter = ObjectInspectorConverters.getConverter(args[1],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		hashSaltConverter = ObjectInspectorConverters.getConverter(args[2],
		          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

}
