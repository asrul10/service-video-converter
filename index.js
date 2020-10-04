#!/usr/bin/env node
"use strict"

const amqp = require('amqplib')
const ffmpeg = require('fluent-ffmpeg')
const cliProgress = require('cli-progress')
const minio = require('minio')

const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
const args = process.argv.slice(2)
const queue = "Hello"

const downloader = async () => {
  const minioClient = new minio.Client({
    endPoint: 'localhost',
    port: 9000,
    useSSL: false,
    accessKey: 'minio',
    secretKey: 'miniosecret'
  })

  // Test upload
  const file = 'source/1mb.mp4'
  var metaData = {
    'example': 5678
  }
  minioClient.fPutObject('video-source', '1mb.mp4', file, metaData, function (err, etag) {
    if (err) return console.log(err)
    console.log('File uploaded successfully.')
  })
}

// Publisher
const publisher = async (fileName) => {
  try {
    const conn = await amqp.connect('amqp://rabbit:password@localhost')
    const ch = await conn.createChannel()

    ch.assertQueue(queue, {
      durable: false
    })
    ch.sendToQueue(queue, Buffer.from(JSON.stringify({ fileName: fileName, time: Date.now() })))
    console.log(" [✔] Message sent!")

    setTimeout(() => {
      conn.close()
      process.exit(0)
    }, 500)
  } catch (error) {
    throw error
  }
}

// Consumer
const consumer = async () => {
  try {
    const conn = await amqp.connect('amqp://rabbit:password@localhost')
    const ch = await conn.createChannel()

    ch.assertQueue(queue, {
      durable: false
    })
    ch.prefetch(1)

    console.log(" [⏳] Waiting for messages in %s. To exit press CTRL+C", queue)

    ch.consume(queue, async (msg) => {
      try {
        const message = JSON.parse(msg.content)
        await convert(message.fileName)
        ch.ack(msg)
        console.log(" [✔] Finish converting!", message)
      } catch (error) {
        throw error
      }
    })
  } catch (error) {
    throw error
  }
}

const convert = (fileName) => {
  bar1.start(100, 0)

  return new Promise(function (resolve, reject) {
    ffmpeg('source/' + fileName)
      .output('dist/' + fileName + '-480p.mp4')
      .audioCodec('copy')
      .size('720x480')

      .output('dist/' + fileName + '-720p.mp4')
      .audioCodec('copy')
      .size('1280x720')

      .output('dist/' + fileName + '-1080p.mp4')
      .audioCodec('copy')
      .size('1920x1080')

      .on('progress', function (progress) {
        bar1.update(Math.round(progress.percent))
      })
      .on('error', function (err) {
        reject(err)
      })
      .on('end', function () {
        bar1.stop()
        resolve("Success converted!")
      })
      .run()
  })
}

if (args[0] === "publisher") {
  if (typeof args[1] === "undefined") {
    console.log("file name required!")
    return
  }
  publisher(args[1])
  return
}
if (args[0] === "consumer") {
  consumer()
  return
}
if (args[0] === "downloader") {
  downloader()
  return
}

console.log("Invalid arguments!\nAvailable arguments:\n  - publisher\n  - consumer")