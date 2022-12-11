package container

package com.decathlon.data.datafactory.cdu
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.nio.file.Path
import scala.collection.JavaConverters.mapAsJavaMapConverter
import _root_.com.dimafeng.testcontainers.GenericContainer
import _root_.com.amazonaws.services.s3.AmazonS3
import _root_.com.amazonaws.services.s3.model.CreateBucketRequest
import _root_.com.amazonaws.auth.BasicAWSCredentials
import _root_.com.amazonaws.ClientConfiguration
import _root_.com.amazonaws.services.s3.AmazonS3ClientBuilder
import _root_.com.amazonaws.auth.AWSStaticCredentialsProvider
import _root_.com.amazonaws.client.builder.AwsClientBuilder

trait MinioContainer extends BeforeAndAfterAll {
  self: Suite =>
  protected var minio: GenericContainer = _
  protected var tmpFolder: Path = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    minio = GenericContainer(
      "minio/minio",
      Seq(9000)
    )
    minio.container.withCommand(
      "server /data"
    )
    minio.container.withEnv(
      Map[String, String](
        "MINIO_ROOT_USER" -> "minioadmin",
        "MINIO_ROOT_PASSWORD" -> "minioadmin"
      ).asJava
    )


    minio.start()

  }

  override def afterAll(): Unit ={
  }

  def createBucket(bucketName: String) : Unit = {
    val s3: AmazonS3 = getS3Client

    if (!s3.doesBucketExistV2(bucketName)) {
      val response = s3.createBucket(new CreateBucketRequest(bucketName))
      print(response)
    }
  }

  def deleteBucket(bucketName: String) : Unit = {
    val s3: AmazonS3 = getS3Client
    if (s3.doesBucketExistV2(bucketName)) {
      s3.deleteBucket(bucketName)
    }
  }


  def getS3Client = {
    val port = minio.container.getMappedPort(9000)
    val credentials = new BasicAWSCredentials("minioadmin", "minioadmin")
    var clientConfiguration = new ClientConfiguration()
    clientConfiguration.setSignerOverride("AWSS3V4SignerType")


    val s3 = AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s"http://localhost:${port}", "us-east-1"))
      .withClientConfiguration(clientConfiguration)
      .withPathStyleAccessEnabled(true)
      .build()
    s3
  }
}

