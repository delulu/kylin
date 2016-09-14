/*
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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.mail.Session;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.kylin.common.KylinConfig;

/**
 * @author xduo
 */
public class MailService {

    private Boolean enabled = Boolean.TRUE;
    private String host;
    private int port;
    private String username;
    private String password;
    private String sender;

    private static final Log logger = LogFactory.getLog(MailService.class);

    public MailService(KylinConfig config) {
        this(config.isMailEnabled(), config.getMailHost(),config.getMailPort(), config.getMailUsername(), config.getMailPassword(), config.getMailSender());
    }

    private MailService(boolean enabled, String host, int port, String username, String password, String sender) {
        this.enabled = enabled;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.sender = sender;
        if (enabled) {
            if (host.isEmpty()) {
                throw new RuntimeException("mail service host is empty");
            }
        }
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content) {
        return sendMail(receivers, subject, content, true);
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content, boolean isHtmlMsg) {

        if (!enabled) {
            logger.info("Email service is disabled; this mail will not be delivered: " + subject);
            logger.info("To enable mail service, set 'mail.enabled=true' in kylin.properties");
            return false;
        }

        Email email = new HtmlEmail();
        email.setHostName(host);
        email.setSmtpPort(port);
        if (username != null && username.trim().length() > 0) {
            logger.info("username is "+username);
            email.setAuthentication(username, password);
        }
        email.setDebug(true);
        try {
            for (String receiver : receivers) {
                email.addTo(receiver);
            }

            email.setFrom(sender);
            email.setSubject(subject);
            email.setCharset("UTF-8");
            if (isHtmlMsg) {
                ((HtmlEmail) email).setHtmlMsg(content);
            } else {
                ((HtmlEmail) email).setTextMsg(content);
            }
            email.send();
            email.getMailSession();

        } catch (EmailException e) {
            logger.error(e.getLocalizedMessage(), e);
            return false;
        }

        return true;
    }
}
