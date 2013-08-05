package com.swisscom.bigdata.utils;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class HiveUtils {
	
	public static boolean isPrimitive(ObjectInspector inspector) {
		return inspector.getCategory() == PRIMITIVE;
	}
}
