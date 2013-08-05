package com.swisscom.bigdata.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class RegexClassifierUDFTest {
	
	@Test (expected = UDFArgumentException.class)
	public void shouldNotAllowNonPrimitiveCategory() throws Exception {
		// given 
		ObjectInspector inspector1 = mock(ObjectInspector.class);
		ObjectInspector inspector2 = mock(ObjectInspector.class);
		
		ObjectInspector[] inspectors = new ObjectInspector[] { inspector1, inspector2 };
		when(inspector1.getCategory()).thenReturn(PRIMITIVE);
		when(inspector2.getCategory()).thenReturn(STRUCT);
		
		// when & then
		RegexClassifierUDF rcudf = new RegexClassifierUDF();
		rcudf.initialize(inspectors);
	}
	
	@Test (expected = UDFArgumentException.class)
	public void shouldRequireTwoArgs() throws Exception {
		// given 
		ObjectInspector inspector1 = mock(ObjectInspector.class);
		
		ObjectInspector[] inspectors = new ObjectInspector[] { inspector1 };
		when(inspector1.getCategory()).thenReturn(PRIMITIVE);
		
		// when & then
		RegexClassifierUDF rcudf = new RegexClassifierUDF();
		rcudf.initialize(inspectors);
	}
	
	@Test
	public void shouldEvaluateNullIfAnyArgIsNull() throws Exception {
		// given
		DeferredObject obj1 = mock(DeferredObject.class);
		DeferredObject obj2 = mock(DeferredObject.class);
		
		DeferredObject[] objs = new DeferredObject[] { obj1, obj2 };
		when(obj1.get()).thenReturn(new Object());
		when(obj2.get()).thenReturn(null);
		
		// when & then
		RegexClassifierUDF rcudf = new RegexClassifierUDF();
		assertEquals(null, rcudf.evaluate(objs));
	}
	
	@Test
	public void shouldClassifyCorrectly() throws Exception {
		// given
		ObjectInspector inspector1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector inspector2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector[] inspectors = new ObjectInspector[] { inspector1, inspector2 };
		
		RegexClassifierUDF rcudf = new RegexClassifierUDF();
		rcudf.initialize(inspectors);
		
		DeferredObject mappingFile = mock(DeferredObject.class);
		DeferredObject text = mock(DeferredObject.class);
		DeferredObject[] objs = new DeferredObject[] { mappingFile, text };
		
		// preparing mapping file
		File tmp = File.createTempFile("regex_classifier", ".csv");
		tmp.deleteOnExit();
		FileWriter writer = new FileWriter(tmp);
		writer.append("\"mail.google.com\",\"mail.google.com\"\n");
		writer.append("\"google.com\",\".*[.]google[.]com\",\".*[.]google[.]pl\"\n");
		writer.append("\"yahoo.com\",\"yahoo.com\"\n");
		writer.flush();
		writer.close();
		when(mappingFile.get()).thenReturn(tmp.getAbsolutePath());
		
		when(text.get()).thenReturn("news.google.com");
		assertEquals(new Text("google.com"), rcudf.evaluate(objs));
		
		when(text.get()).thenReturn("news.google.pl");
		assertEquals(new Text("google.com"), rcudf.evaluate(objs));
		
		when(text.get()).thenReturn("yahoo.com");
		assertEquals(new Text("yahoo.com"), rcudf.evaluate(objs));
		
		when(text.get()).thenReturn("xxx.abc.com");
		assertEquals(new Text("xxx.abc.com"), rcudf.evaluate(objs));
	}
}
