#!/usr/bin/env node
"use strict"

const amqp = require('amqplib')
const ffmpeg = require('fluent-ffmpeg')
const cliProgress = require('cli-progress')

const args = process.argv.slice(2)
const amqpHost = 'amqp://localhost'
const queue = "video-converter"
const bucketSource = '/data/video-source/'
const bucketTarget = '/data/video-prod/'
const progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

// Publisher
const publisher = async (fileName) => {
  try {
    const conn = await amqp.connect(amqpHost)
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
    const conn = await amqp.connect(amqpHost)
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
  progressBar.start(100, 0)

  return new Promise(function (resolve, reject) {
    ffmpeg(bucketSource + fileName)
      .output(bucketTarget + fileName + '-480p.mp4')
      .audioCodec('copy')
      .size('720x480')

      .output(bucketTarget + fileName + '-720p.mp4')
      .audioCodec('copy')
      .size('1280x720')

      .output(bucketTarget + fileName + '-1080p.mp4')
      .audioCodec('copy')
      .size('1920x1080')

      .on('progress', function (progress) {
        progressBar.update(Math.round(progress.percent))
      })
      .on('error', function (err) {
        reject(err)
      })
      .on('end', function () {
        progressBar.stop()
        resolve(`${fileName} Success converted!`)
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

console.log("Invalid arguments!\nAvailable arguments:\n  - publisher\n  - consumer")