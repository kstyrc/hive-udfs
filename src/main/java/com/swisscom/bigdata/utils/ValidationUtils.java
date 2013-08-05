package com.swisscom.bigdata.utils;

public class ValidationUtils {
	
	public static boolean isNull(Object obj) {
		return obj == null;
	}
	
	public static boolean isNotNull(Object obj) {
		return !isNull(obj);
	}
	
	public static boolean isAnyNull(Object... objs) {
		for (Object o : objs) {
			if (isNull(o)) {
				return true;
			}
		}
		return false;
	}
}
