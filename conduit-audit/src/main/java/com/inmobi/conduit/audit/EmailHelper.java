package com.inmobi.conduit.audit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class EmailHelper {
  private static final Log LOG = LogFactory.getLog(EmailHelper.class);

  public static int sendMail(String mailMessage, List<String> emailIdList) {

    Properties props = System.getProperties();
    props.put("mail.smtp.host", "localhost");
    props.put("mail.smtp.port", "25");

    Session session = Session.getDefaultInstance(props, null);
    String host = "";
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    try {
      MimeMessage message = new MimeMessage(session);
      for (String emailId : emailIdList) {
        LOG.info("Adding email address " + emailId + " to recipient list");
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailId));
      }
      message.setFrom(new InternetAddress("ConduitStreamsLatencies@Weekly"));
      message.setSubject("Conduit stream latencies " + host);
      message.setSentDate(new Date());
      //message.setText(mailMessage);
      Multipart mp = new MimeMultipart();

      MimeBodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(mailMessage, "text/html");
      mp.addBodyPart(htmlPart);

      MimeBodyPart attachment = new MimeBodyPart();
      attachment.setFileName("manual.pdf");
      attachment.setContent("", "application/pdf");
      mp.addBodyPart(attachment);

      message.setContent(mp);
      LOG.info("Sending email" + message);
      Transport.send(message);
    } catch (MessagingException e) {
      LOG.error(e);
      LOG.info("Email sending failed");
      return -1;
    }
    LOG.info("Email sent");
    return 1;
  }
}
