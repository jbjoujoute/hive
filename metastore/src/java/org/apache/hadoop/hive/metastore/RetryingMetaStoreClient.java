/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.annotation.NoReconnect;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

/**
 * RetryingMetaStoreClient. Creates a proxy for a IMetaStoreClient
 * implementation and retries calls to it on failure.
 * If the login user is authenticated using keytab, it relogins user before
 * each call.
 *
 */
@Public
public class RetryingMetaStoreClient implements InvocationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RetryingMetaStoreClient.class.getName());

  private final IMetaStoreClient base;
  private final int retryLimit;
  private final long retryDelaySeconds;
  private final ConcurrentHashMap<String, Long> metaCallTimeMap;
  private final long connectionLifeTimeInMillis;
  private long lastConnectionTime;
  private boolean localMetaStore;


  protected RetryingMetaStoreClient(HiveConf hiveConf, Class<?>[] constructorArgTypes,
      Object[] constructorArgs, ConcurrentHashMap<String, Long> metaCallTimeMap,
      Class<? extends IMetaStoreClient> msClientClass) throws MetaException {

    this.retryLimit = hiveConf.getIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES);
    this.retryDelaySeconds = hiveConf.getTimeVar(
        HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
    this.metaCallTimeMap = metaCallTimeMap;
    this.connectionLifeTimeInMillis = hiveConf.getTimeVar(
        HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME, TimeUnit.MILLISECONDS);
    this.lastConnectionTime = System.currentTimeMillis();
    String msUri = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
    localMetaStore = (msUri == null) || msUri.trim().isEmpty();

    reloginExpiringKeytabUser();
    this.base = (IMetaStoreClient) MetaStoreUtils.newInstance(
        msClientClass, constructorArgTypes, constructorArgs);
  }

  public static IMetaStoreClient getProxy(
      HiveConf hiveConf, boolean allowEmbedded) throws MetaException {
    return getProxy(hiveConf, new Class[]{HiveConf.class, HiveMetaHookLoader.class, Boolean.class},
        new Object[]{hiveConf, null, allowEmbedded}, null, HiveMetaStoreClient.class.getName()
    );
  }

  public static IMetaStoreClient getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader,
      String mscClassName) throws MetaException {
    return getProxy(hiveConf, hookLoader, null, mscClassName, true);
  }

  /**
   * Added for compatibility with Spark SQL
   */
  public static IMetaStoreClient getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader,
      Map<String, Long> metaCallTimeMap, String mscClassName) throws MetaException {

    return getProxy(hiveConf,
            new Class[] {HiveConf.class, HiveMetaHookLoader.class},
            new Object[] {hiveConf, hookLoader},
            new ConcurrentHashMap(metaCallTimeMap),
            mscClassName
    );
  }

  public static IMetaStoreClient getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader,
      ConcurrentHashMap<String, Long> metaCallTimeMap, String mscClassName, boolean allowEmbedded)
          throws MetaException {

    return getProxy(hiveConf,
        new Class[] {HiveConf.class, HiveMetaHookLoader.class, Boolean.class},
        new Object[] {hiveConf, hookLoader, allowEmbedded},
        metaCallTimeMap,
        mscClassName
    );
  }

  /**
   * This constructor is meant for Hive internal use only.
   * Please use getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader) for external purpose.
   */
  public static IMetaStoreClient getProxy(HiveConf hiveConf, Class<?>[] constructorArgTypes,
      Object[] constructorArgs, String mscClassName) throws MetaException {
    return getProxy(hiveConf, constructorArgTypes, constructorArgs, null, mscClassName);
  }

  /**
   * This constructor is meant for Hive internal use only.
   * Please use getProxy(HiveConf hiveConf, HiveMetaHookLoader hookLoader) for external purpose.
   */
  public static IMetaStoreClient getProxy(HiveConf hiveConf, Class<?>[] constructorArgTypes,
      Object[] constructorArgs, ConcurrentHashMap<String, Long> metaCallTimeMap,
      String mscClassName) throws MetaException {

    @SuppressWarnings("unchecked")
    Class<? extends IMetaStoreClient> baseClass =
        (Class<? extends IMetaStoreClient>)MetaStoreUtils.getClass(mscClassName);

    RetryingMetaStoreClient handler =
        new RetryingMetaStoreClient(hiveConf, constructorArgTypes, constructorArgs,
            metaCallTimeMap, baseClass);
    return (IMetaStoreClient) Proxy.newProxyInstance(
        RetryingMetaStoreClient.class.getClassLoader(), baseClass.getInterfaces(), handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object ret = null;
    int retriesMade = 0;
    TException caughtException = null;

    boolean allowReconnect = ! method.isAnnotationPresent(NoReconnect.class);
    boolean allowRetry = true;
    Annotation[] directives = method.getDeclaredAnnotations();
    if(directives != null) {
      for(Annotation a : directives) {
        if(a instanceof RetrySemantics.CannotRetry) {
          allowRetry = false;
        }
      }
    }

    while (true) {
      try {
        reloginExpiringKeytabUser();

        if (allowReconnect) {
          if (retriesMade > 0 || hasConnectionLifeTimeReached(method)) {
            base.reconnect();
            lastConnectionTime = System.currentTimeMillis();
          }
        }

        if (metaCallTimeMap == null) {
          ret = method.invoke(base, args);
        } else {
          // need to capture the timing
          long startTime = System.currentTimeMillis();
          ret = method.invoke(base, args);
          long timeTaken = System.currentTimeMillis() - startTime;
          addMethodTime(method, timeTaken);
        }
        break;
      } catch (UndeclaredThrowableException e) {
        throw e.getCause();
      } catch (InvocationTargetException e) {
        Throwable t = e.getCause();
        if (t instanceof TApplicationException) {
          TApplicationException tae = (TApplicationException)t;
          switch (tae.getType()) {
          case TApplicationException.UNSUPPORTED_CLIENT_TYPE:
          case TApplicationException.UNKNOWN_METHOD:
          case TApplicationException.WRONG_METHOD_NAME:
          case TApplicationException.INVALID_PROTOCOL:
            throw t;
          default:
            // TODO: most other options are probably unrecoverable... throw?
            caughtException = tae;
          }
        } else if ((t instanceof TProtocolException) || (t instanceof TTransportException)) {
          // TODO: most protocol exceptions are probably unrecoverable... throw?
          caughtException = (TException)t;
        } else if ((t instanceof MetaException) && t.getMessage().matches(
            "(?s).*(JDO[a-zA-Z]*|TProtocol|TTransport)Exception.*") &&
            !t.getMessage().contains("java.sql.SQLIntegrityConstraintViolationException")) {
          caughtException = (MetaException)t;
        } else {
          throw t;
        }
      } catch (MetaException e) {
        if (e.getMessage().matches("(?s).*(IO|TTransport)Exception.*") &&
            !e.getMessage().contains("java.sql.SQLIntegrityConstraintViolationException")) {
          caughtException = e;
        } else {
          throw e;
        }
      }


      if (retriesMade >= retryLimit || base.isLocalMetaStore() || !allowRetry) {
        throw caughtException;
      }
      retriesMade++;
      LOG.warn("MetaStoreClient lost connection. Attempting to reconnect (" + retriesMade + " of " +
          retryLimit + ") after " + retryDelaySeconds + "s. " + method.getName(), caughtException);
      Thread.sleep(retryDelaySeconds * 1000);
    }
    return ret;
  }

  private void addMethodTime(Method method, long timeTaken) {
    String methodStr = getMethodString(method);
    while (true) {
      Long curTime = metaCallTimeMap.get(methodStr), newTime = timeTaken;
      if (curTime != null && metaCallTimeMap.replace(methodStr, curTime, newTime + curTime)) break;
      if (curTime == null && (null == metaCallTimeMap.putIfAbsent(methodStr, newTime))) break;
    }
  }

  /**
   * @param method
   * @return String representation with arg types. eg getDatabase_(String, )
   */
  private String getMethodString(Method method) {
    StringBuilder methodSb = new StringBuilder(method.getName());
    methodSb.append("_(");
    for (Class<?> paramClass : method.getParameterTypes()) {
      methodSb.append(paramClass.getSimpleName());
      methodSb.append(", ");
    }
    methodSb.append(")");
    return methodSb.toString();
  }

  private boolean hasConnectionLifeTimeReached(Method method) {
    if (connectionLifeTimeInMillis <= 0 || localMetaStore) {
      return false;
    }

    boolean shouldReconnect =
        (System.currentTimeMillis() - lastConnectionTime) >= connectionLifeTimeInMillis;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reconnection status for Method: " + method.getName() + " is " + shouldReconnect);
    }
    return shouldReconnect;
  }

  /**
   * Relogin if login user is logged in using keytab
   * Relogin is actually done by ugi code only if sufficient time has passed
   * A no-op if kerberos security is not enabled
   * @throws MetaException
   */
  private void reloginExpiringKeytabUser() throws MetaException {
    if(!UserGroupInformation.isSecurityEnabled()){
      return;
    }
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      //checkTGT calls ugi.relogin only after checking if it is close to tgt expiry
      //hadoop relogin is actually done only every x minutes (x=10 in hadoop 1.x)
      if(ugi.isFromKeytab()){
        ugi.checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      String msg = "Error doing relogin using keytab " + e.getMessage();
      LOG.error(msg, e);
      throw new MetaException(msg);
    }
  }

}
