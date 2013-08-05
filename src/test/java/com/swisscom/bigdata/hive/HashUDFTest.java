package com.swisscom.bigdata.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class HashUDFTest {
	
	@Test (expected = UDFArgumentException.class)
	public void shouldNotAllowNonPrimitiveCategory() throws Exception {
		// given 
		ObjectInspector inspector1 = mock(ObjectInspector.class);
		ObjectInspector inspector2 = mock(ObjectInspector.class);
		ObjectInspector inspector3 = mock(ObjectInspector.class);
		
		ObjectInspector[] inspectors = new ObjectInspector[] { inspector1, inspector2, inspector3 };
		when(inspector1.getCategory()).thenReturn(PRIMITIVE);
		when(inspector2.getCategory()).thenReturn(PRIMITIVE);
		when(inspector3.getCategory()).thenReturn(STRUCT);
		
		// when & then
		HashUDF hudf = new HashUDF();
		hudf.initialize(inspectors);
	}
	
	@Test (expected = UDFArgumentException.class)
	public void shouldRequireThreeArgs() throws Exception {
		// given 
		ObjectInspector inspector1 = mock(ObjectInspector.class);
		ObjectInspector inspector2 = mock(ObjectInspector.class);
		
		ObjectInspector[] inspectors = new ObjectInspector[] { inspector1, inspector2 };
		when(inspector1.getCategory()).thenReturn(PRIMITIVE);
		when(inspector2.getCategory()).thenReturn(PRIMITIVE);
		
		// when & then
		HashUDF hudf = new HashUDF();
		hudf.initialize(inspectors);
	}

	@Test
	public void shouldEvaluateNullIfAnyArgIsNull() throws Exception {
		// given
		DeferredObject obj1 = mock(DeferredObject.class);
		DeferredObject obj2 = mock(DeferredObject.class);
		DeferredObject obj3 = mock(DeferredObject.class);
		
		DeferredObject[] objs = new DeferredObject[] { obj1, obj2, obj3 };
		when(obj1.get()).thenReturn(new Object());
		when(obj2.get()).thenReturn(new Object());
		when(obj3.get()).thenReturn(null);
		
		// when & then
		HashUDF hudf = new HashUDF();
		assertEquals(null, hudf.evaluate(objs));
	}
	
	@Test
	public void shouldReturnCorrectHash() throws Exception {
		// given
		ObjectInspector inspector1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector inspector2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector inspector3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector[] inspectors = new ObjectInspector[] { inspector1, inspector2, inspector3};
		
		HashUDF hudf = new HashUDF();
		hudf.initialize(inspectors);
		
		DeferredObject text = mock(DeferredObject.class);
		DeferredObject hashType = mock(DeferredObject.class);
		DeferredObject hashSalt = mock(DeferredObject.class);
		DeferredObject[] objs = new DeferredObject[] { text, hashType, hashSalt };
		
		when(text.get()).thenReturn("abc");
		when(hashType.get()).thenReturn("md5");
		when(hashSalt.get()).thenReturn("");
		Text md5Hash = (Text) hudf.evaluate(objs);
		
		assertEquals(
				"900150983cd24fb0d6963f7d28e17f72",
				md5Hash.toString());
		
		when(text.get()).thenReturn("abc");
		when(hashType.get()).thenReturn("sha1");
		when(hashSalt.get()).thenReturn("");
		Text sha1Hash = (Text) hudf.evaluate(objs);
		
		assertEquals(
				"a9993e364706816aba3e25717850c26c9cd0d89d",
				sha1Hash.toString());
		
		when(text.get()).thenReturn("abc");
		when(hashType.get()).thenReturn("sha256");
		when(hashSalt.get()).thenReturn("");
		Text sha256Hash = (Text) hudf.evaluate(objs);
		
		assertEquals(
				"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
				sha256Hash.toString());
		
		when(text.get()).thenReturn("abc");
		when(hashType.get()).thenReturn("sha512");
		when(hashSalt.get()).thenReturn("");
		Text sha512Hash = (Text) hudf.evaluate(objs);
		
		assertEquals(
				"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
				sha512Hash.toString());
		
		when(text.get()).thenReturn("abc");
		when(hashType.get()).thenReturn("xxx");
		when(hashSalt.get()).thenReturn("");
		Text unsupportedHash = (Text) hudf.evaluate(objs);
		
		assertEquals(
				null,
				unsupportedHash);
		
		
	}
}
