package com.swisscom.bigdata.hive;

import static com.swisscom.bigdata.utils.HiveUtils.isPrimitive;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.swisscom.bigdata.utils.ValidationUtils;

@Description(
	name = "hash", 
	value = "_FUNC_(string_column, hash_type, hash_salt) - returns hash (with salt) value for the input string"
)
public class HashUDF extends GenericUDF {

	protected ObjectInspectorConverters.Converter inputConverter;
	protected ObjectInspectorConverters.Converter hashTypeConverter;
	protected ObjectInspectorConverters.Converter hashSaltConverter;
	
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		Object text = args[0].get();
		Object hashType = args[1].get();
		Object hashSalt = args[2].get();
		
		// defensive check - as in other UDF examples
		if (ValidationUtils.isAnyNull(text, hashType, hashSalt))
			return null;
		
		return evaluate(text, hashType, hashSalt);
	}

	private Object evaluate(Object textObj, Object hashTypeObj, Object hashSaltObj) {
		String text     = ((Text)    inputConverter.convert(textObj)).toString();
		String hashType = ((Text) hashTypeConverter.convert(hashTypeObj)).toString().toUpperCase();
		String hashSalt = ((Text) hashSaltConverter.convert(hashSaltObj)).toString();
		
		return hash(text, hashType, hashSalt);
	}

	private Object hash(String text, String hashType, String hashSalt) {
		String input = text + hashSalt;
		if (hashType.equalsIgnoreCase("md5")) {
			return new Text(DigestUtils.md5Hex(input));
		} else if (hashType.equalsIgnoreCase("sha1")) {
			return new Text(DigestUtils.shaHex(input));
		} else if (hashType.equalsIgnoreCase("sha256")) {
			return new Text(DigestUtils.sha256Hex(input));
		} else if (hashType.equalsIgnoreCase("sha512")) {
			return new Text(DigestUtils.sha512Hex(input));
		} else {
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return String.format("hash(%s, %s, %s)", children[0], children[1], children[2]);
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (invalidArgs(args)) {
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

	private boolean invalidArgs(ObjectInspector[] args) {
		return args.length != 3 || 
				!isPrimitive(args[0]) ||
				!isPrimitive(args[1]) ||
				!isPrimitive(args[2]
		);
	}
}
