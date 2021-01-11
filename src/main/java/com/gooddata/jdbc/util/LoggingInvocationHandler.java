package com.gooddata.jdbc.util;

import com.gooddata.jdbc.driver.Driver;
import com.gooddata.jdbc.driver.ResultSetTableMetaData;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggingInvocationHandler implements InvocationHandler {

    private final static Logger LOGGER = Logger.getLogger(LoggingInvocationHandler.class.getName());

    private final Object delegate;

    public LoggingInvocationHandler(final Object delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String argsText = "";
        if(args != null)
            argsText = Arrays.asList(args).toString();
        LOGGER.info(String.format("INVOKING class: '%s' method: '%s' args: %s",
                method.getDeclaringClass().getName(),
                method.getName(),
                argsText));
        Object r = method.invoke(delegate, args);
        LOGGER.info(String.format("INVOKED class: '%s' method: '%s' args: %s returns: %s",
                method.getDeclaringClass().getName(),
                method.getName(),
                argsText,
                r));
        return r;
    }

}
