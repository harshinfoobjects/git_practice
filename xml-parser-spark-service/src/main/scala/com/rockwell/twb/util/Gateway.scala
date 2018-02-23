package com.rockwell.twb.util

import org.apache.http.impl.client.AutoRetryHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.params.HttpConnectionParams
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy
import org.apache.http.client.methods.HttpDelete
import java.io.StringWriter
import java.io.PrintWriter
import com.sendgrid.SendGrid
import com.sendgrid.Request
import com.sendgrid.Response
import com.sendgrid.Method
import com.sendgrid.Email
import com.sendgrid.Content
import com.sendgrid.Mail

object Gateway {

  private def buildHttpClient(): AutoRetryHttpClient = {
    val retryStrategy = new DefaultServiceUnavailableRetryStrategy(3, 2000)
    val httpClient = new AutoRetryHttpClient(retryStrategy)
    val httpParams = httpClient.getParams
    HttpConnectionParams.setConnectionTimeout(httpParams, 120000);
    HttpConnectionParams.setSoTimeout(httpParams, 120000);
    httpClient
  }

  def getRestContent(url: String): String = {
    val httpClient = buildHttpClient()
    try {

      val httpResponse = httpClient.execute(new HttpGet(url))
      val entity = httpResponse.getEntity()
      var content = ""
      if (entity != null) {
        val inputStream = entity.getContent()
        content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close
      }

      return content
    } finally {
      httpClient.getConnectionManager().shutdown()
    }

  }

  def sendEmail(to: String, from: String, subject: String, body: Exception, sendGridKey: String) = {
    try {

      val sg: SendGrid = new SendGrid(sendGridKey);

      val mail = buildMailObject(to, from, subject, body);

      val request = new Request();
      request.setMethod(Method.POST);
      request.setEndpoint("mail/send");
      request.setBody(mail.build())
      val response = sg.api(request);
      println(response.getStatusCode());
      println(response.getBody());
      println(response.getHeaders());

      println("Mail sent to: " + to)
    } catch {
      case e: Exception => println("Exception while sending mail" + e.printStackTrace())
    }

  }

  def buildMailObject(to: String, from: String, subject: String, body: Exception): Mail = {

    val fromAddress = new Email(from);

    val toAddress = new Email(to)

    val content = new Content();
    content.setType("text/plain");
    content.setValue(body.getStackTraceString);

    return new Mail(fromAddress, subject + (if(body != null) " | " + body.toString() else ""), toAddress, content);
  }

  def deleteContent(url: String) {
    val httpClient = buildHttpClient()
    try {
      val httpResponse = httpClient.execute(new HttpDelete(url))
    } finally {
      httpClient.getConnectionManager().shutdown()
    }

  }

  def getPostContent(post: HttpPost): String = {
    val httpClient = buildHttpClient()
    try {

      val httpResponse = httpClient.execute(post)
      val entity = httpResponse.getEntity()
      var content = ""
      if (entity != null) {
        val inputStream = entity.getContent()
        content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close
      }

      return content

    } finally {
      httpClient.getConnectionManager().shutdown()
    }
  }

}
