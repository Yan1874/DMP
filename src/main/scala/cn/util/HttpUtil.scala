package cn.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils


object HttpUtil {
  def get(url: String) = {
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    val response: CloseableHttpResponse = client.execute(httpGet)
    EntityUtils.toString(response.getEntity,"UTF-8")
  }

}
