/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hadoop;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Set;

public class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  private Util() {
  }

  public static FileSystem getFs(Path path, Configuration conf) {
    // get krb.enabled from configuration
    boolean krbEnabled = conf.getBoolean(KerberosLoginUtil.KERBEROS_ENABLED, false);
    boolean isLogin;
    UserGroupInformation ugi = null;
    try {
      System.setProperty("HADOOP_USER_NAME", "sloth/dev@BDMS.163.COM");
      isLogin = UserGroupInformation.isLoginKeytabBased();
      boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
      LOG.info("krbEnabled: {}, ugi isLogin: {}, security: {}, hasKrbCredentials: {}.",
          krbEnabled, isLogin, securityEnabled, UserGroupInformation.getLoginUser().hasKerberosCredentials());
      if (krbEnabled) {
        if (!isLogin) {
          KerberosLoginUtil.initKerberosEnv(conf);
        }
        Token token = UserGroupInformation.getLoginUser().doAs((PrivilegedAction<Token>) () -> {
          Token tk = null;
          try {
            FileSystem fs = FileSystem.get(conf);
            tk = fs.getDelegationToken(UserGroupInformation.getLoginUser().getShortUserName());
          } catch (IOException e) {
            e.printStackTrace();
          }
          return tk;
        });
        LOG.info("token:{}.", token);
        Credentials cre = new Credentials();
        ugi = UserGroupInformation.getLoginUser();
        cre.addToken(new Text(ugi.getShortUserName()), token);
        ugi.addCredentials(cre);


        FileSystem fs = UserGroupInformation.getLoginUser().doAs((PrivilegedAction<FileSystem>) () -> {
          try {
            UserGroupInformation.getLoginUser().reloginFromKeytab();
            return path.getFileSystem(conf);
          } catch (IOException e) {
            LOG.error("Failed to get file system for path: {}. {}", path, e);
            return null;
          }
        });
        if (fs == null) {
          throw new RuntimeIOException("Failed to get file system for path: %s", path);
        } else {
          return fs;
        }
      }
    } catch (IOException | KrbException e) {
      LOG.error("Failed to get file system for path: {}, krb.enabled: {}, error:{}.", path, krbEnabled, e);
      throw new RuntimeIOException("Failed to get file system for path: %s", path);
    }

    try {
      FileSystem fs = path.getFileSystem(conf);
      UserGroupInformation cUser = UserGroupInformation.getCurrentUser();
      String current = cUser.getUserName();
      LOG.info("login user: {}, short name: {}, current user: {}.",
          ugi.getUserName(), ugi.getShortUserName(),
          current);
      AccessControlContext context = AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      LOG.info("current user is keytab: {}, hasKerberosCredentials: {}.",
          cUser.isFromKeytab(), cUser.hasKerberosCredentials());
      LOG.info("subject == null ? {}.", subject ==  null);
      if (subject != null) {
        LOG.info("subject principal size: {}.", subject.getPrincipals().size());
        subject.getPrincipals().forEach(principal -> LOG.info("principal: {}, toString: {}, class: {}.",
            principal.getName(), principal.toString(), principal.getClass().toString()));
      }

      Token token = fs.getDelegationToken(ugi.getShortUserName());
      LOG.info("token: {}.", token);
      return path.getFileSystem(conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    }
  }

  public static String[] blockLocations(CombinedScanTask task, Configuration conf) {
    Set<String> locationSets = Sets.newHashSet();
    for (FileScanTask f : task.files()) {
      Path path = new Path(f.file().path().toString());
      try {
        FileSystem fs = path.getFileSystem(conf);
        for (BlockLocation b : fs.getFileBlockLocations(path, f.start(), f.length())) {
          locationSets.addAll(Arrays.asList(b.getHosts()));
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to get block locations for path {}", path, ioe);
      }
    }

    return locationSets.toArray(new String[0]);
  }
}
