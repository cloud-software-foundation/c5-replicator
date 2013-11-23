package ohmdb.interfaces;

import ohmdb.messages.generated.ControlMessages;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ModuleTypeBinding {
    ControlMessages.ModuleType value();
}
