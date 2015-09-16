package com.github.lucastorri.moca.store.content

import java.io._
import java.net.URL
import java.nio.file.Files

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.{S3ClientOptions, AmazonS3Client}
import com.amazonaws.services.s3.model._
import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.store.content.S3ContentRepo._
import com.github.lucastorri.moca.store.content.serializer.ContentSerializer
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.Try

/**
 * Right now, all files are made public in order to make S3ContentLinksTransfer serializable
 */
class S3ContentRepo(config: Config, serializer: ContentSerializer) extends ContentRepo with StrictLogging {

  val client = {
    val credentials = new BasicAWSCredentials(config.getString("access-key"), config.getString("secret-key"))
    val conf = new ClientConfiguration()
    val client = new AmazonS3Client(credentials, conf)
    if (config.hasPath("endpoint")) {
      client.setEndpoint(config.getString("endpoint"))
      client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true))
    }
    client
  }

  //TODO throws exception if bucket exists
  val bucket = client.createBucket(config.getString("bucket-name"))

  override def apply(task: Task): TaskContentRepo = new S3TaskContentRepo(client, bucket, task, serializer)

  override def links(task: Task): ContentLinksTransfer = {

    val file = Files.createTempFile("moca-", "-s3-content").toFile
    logger.trace(s"Putting metadata for task ${task.id} in $file")
    val fos = new FileOutputStream(file)

    @tailrec
    def add(list: ObjectListing): Unit = {
      val summaries = list.getObjectSummaries
      logger.trace(s"Got ${summaries.size()} files for task ${task.id}")
      summaries.foreach { summary =>
        val obj = client.getObject(bucket.getName, summary.getKey)

        val in = obj.getObjectContent
        val (url, uri, depth, hash) = readMeta(in)
        in.close()

        client.getResourceUrl(bucket.getName, urlKey(task, url, depth))

        writeMeta(fos, url, uri, depth, hash)
      }

      if (list.isTruncated) add(client.listNextBatchOfObjects(list))
    }

    add(client.listObjects(bucket.getName, metaPrefix(task)))

    fos.flush()
    fos.close()

    val put = new PutObjectRequest(bucket.getName, listKey(task), file)
      .withCannedAcl(CannedAccessControlList.PublicRead)
    client.putObject(put)

    S3ContentLinkTransfer(client.getResourceUrl(bucket.getName, listKey(task)))
  }

}

object S3ContentRepo {

  def urlKey(task: Task, url: Url, depth: Int): String = s"${task.id}/content/${url.id}-$depth"

  def metaKey(task: Task, url: Url, depth: Int): String = s"${metaPrefix(task)}/${url.id}-$depth"

  def metaPrefix(task: Task) = s"${task.id}/meta"

  def listKey(task: Task): String = s"${task.id}/list"

  def writeMeta(out: OutputStream, url: Url, uri: String, depth: Int, hash: String): Unit = {
    val meta = new DataOutputStream(out)
    meta.writeUTF(url.toString)
    meta.writeUTF(uri)
    meta.writeInt(depth)
    meta.writeUTF(hash)
    meta.flush()
  }

  def readMeta(in: InputStream): (Url, String, Int, String) = {
    val meta = new DataInputStream(in)
    (Url(meta.readUTF()), meta.readUTF(), meta.readInt(), meta.readUTF())
  }

}

class S3TaskContentRepo(client: AmazonS3Client, bucket: Bucket, task: Task, serializer: ContentSerializer) extends TaskContentRepo {

  override def save(url: Url, depth: Int, content: Try[Content]): Future[Unit] = {
    try {

      val putContent = {
        val serialized = serializer.serialize(url, content).array()
        val metadata = new ObjectMetadata()
        metadata.setContentLength(serialized.length)
        new PutObjectRequest(bucket.getName, urlKey(task, url, depth), new ByteArrayInputStream(serialized), metadata)
          .withCannedAcl(CannedAccessControlList.PublicRead)
      }

      val putMetadata = {
        val serialized = {
          val buffer = new ByteArrayOutputStream()
          val meta = new DataOutputStream(buffer)
          writeMeta(buffer, url, client.getResourceUrl(bucket.getName, urlKey(task, url, depth)), depth, content.map(_.hash).getOrElse(""))
          meta.close()
          buffer.toByteArray
        }
        val metadata = new ObjectMetadata()
        metadata.setContentLength(serialized.length)
        new PutObjectRequest(bucket.getName, metaKey(task, url, depth), new ByteArrayInputStream(serialized), metadata)
          .withCannedAcl(CannedAccessControlList.PublicRead)
      }

      client.putObject(putContent)
      client.putObject(putMetadata)

      Future.successful(())
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

}

case class S3ContentLinkTransfer(listUrl: String) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = {
    val in = new URL(listUrl).openStream()

    def next: Stream[ContentLink] = {
      if (in.available() == 0) {
        in.close()
        Stream.empty
      } else {
        val (url, uri, depth, hash) = readMeta(in)
        ContentLink(url, uri, depth, hash) #:: next
      }
    }

    next
  }

}
