package dk.langli.esso;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;

class JacksonVisibilityChecker implements VisibilityChecker<JacksonVisibilityChecker> {
	private VisibilityChecker.Std visibilityChecker = new VisibilityChecker.Std(JsonAutoDetect.Visibility.PUBLIC_ONLY);
	
	public JacksonVisibilityChecker with(JsonAutoDetect ann) {
		visibilityChecker = visibilityChecker.with(ann);
		return this;
	}

	public JacksonVisibilityChecker with(Visibility v) {
		visibilityChecker = visibilityChecker.with(v);
		return this;
	}

	public JacksonVisibilityChecker withVisibility(PropertyAccessor method, Visibility v) {
		visibilityChecker = visibilityChecker.withVisibility(method, v);
		return this;
	}

	public JacksonVisibilityChecker withGetterVisibility(Visibility v) {
		visibilityChecker = visibilityChecker.withGetterVisibility(v);
		return this;
	}

	public JacksonVisibilityChecker withIsGetterVisibility(Visibility v) {
		visibilityChecker = visibilityChecker.withIsGetterVisibility(v);
		return this;
	}

	public JacksonVisibilityChecker withSetterVisibility(Visibility v) {
		visibilityChecker = visibilityChecker.withSetterVisibility(v);
		return this;
	}

	public JacksonVisibilityChecker withCreatorVisibility(Visibility v) {
		visibilityChecker = visibilityChecker.withCreatorVisibility(v);
		return this;
	}

	public JacksonVisibilityChecker withFieldVisibility(Visibility v) {
		visibilityChecker = visibilityChecker.withFieldVisibility(v);
		return this;
	}

	public boolean isGetterVisible(Method m) {
		return visibilityChecker.isGetterVisible(m) && checkMethod(m);
	}

	public boolean isGetterVisible(AnnotatedMethod m) {
		return visibilityChecker.isGetterVisible(m) && checkMethod(m.getMember());
	}

	public boolean isIsGetterVisible(Method m) {
		return visibilityChecker.isIsGetterVisible(m) && checkMethod(m);
	}

	public boolean isIsGetterVisible(AnnotatedMethod m) {
		return visibilityChecker.isIsGetterVisible(m) && checkMethod(m.getMember());
	}

	public boolean isSetterVisible(Method m) {
		return visibilityChecker.isSetterVisible(m) && checkMethod(m);
	}

	public boolean isSetterVisible(AnnotatedMethod m) {
		return visibilityChecker.isSetterVisible(m) && checkMethod(m.getMember());
	}

	public boolean isCreatorVisible(Member m) {
		return false; //visibilityChecker.isCreatorVisible(m);
	}

	public boolean isCreatorVisible(AnnotatedMember m) {
		return false; //visibilityChecker.isCreatorVisible(m);
	}

	public boolean isFieldVisible(Field f) {
		return false; //visibilityChecker.isFieldVisible(f) && !Modifier.isTransient(f.getModifiers())
	}

	public boolean isFieldVisible(AnnotatedField f) {
		return false; //visibilityChecker.isFieldVisible(f) && !Modifier.isTransient(f.getModifiers())
	}
	
	private boolean checkMethod(Method m) {
		boolean checked = checkModifiers(m.getModifiers());
		if(checked) {
			String property = getPropertyName(m);
			if(property.startsWith("_")) {
				Class<?> type = m.getDeclaringClass();
				if(Idable.class.isAssignableFrom(type)) {
					if(property.equals("_id")) {
						checked = false;
					}
					if(checked && Revisable.class.isAssignableFrom(type)) {
						if(property.equals("_version")) {
							checked = false;
						}
					}
				}
			}
		}
		return checked;
	}
	
	private String getPropertyName(Method method) {
		String name = method.getName();
		name = name.startsWith("get") || name.startsWith("set") ? name.substring(3) : name.startsWith("is") ? name.substring(2) : null;
		if(name != null && name.length() > 0) {
			name = name.substring(0, 1).toLowerCase()+name.substring(1);
		}
		else {
			name = null;
		}
		return name;
	}
	
	private boolean checkModifiers(int mod) {
		boolean checked = Modifier.isPublic(mod);
		checked = checked && !Modifier.isTransient(mod);
		checked = checked && !Modifier.isAbstract(mod);
		checked = checked && !Modifier.isStatic(mod);
		return checked;
	}
}
