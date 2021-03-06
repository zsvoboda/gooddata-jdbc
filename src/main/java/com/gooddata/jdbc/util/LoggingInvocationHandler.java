package com.gooddata.jdbc.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Logs invocation of delegate object's methods
 */
public class LoggingInvocationHandler implements InvocationHandler {

    private final static Logger LOGGER = Logger.getLogger(LoggingInvocationHandler.class.getName());

    private final Object delegate;

    /**
     * Constructor
     * @param delegate delegated objects
     */
    public LoggingInvocationHandler(final Object delegate) {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String argsText = "";
        if(args != null)
            argsText = Arrays.asList(args).toString();
        LOGGER.info(String.format("INVOKING class: '%s' method: '%s' args: %s",
                method.getDeclaringClass().getName(),
                method.getName(),
                argsText));
        try {
            Object r = method.invoke(delegate, args);
            String cls = "null";
            if(r != null)
                cls = r.getClass().getName();
            LOGGER.info(String.format("INVOKED class: '%s' method: '%s' args: %s returns: %s class: %s",
                    method.getDeclaringClass().getName(),
                    method.getName(),
                    argsText,
                    r,
                    cls));
            return r;
        } catch(Throwable e) {
            LOGGER.info(String.format("EXCEPTION %s",e.toString()));
            throw e;
        }
    }

}
