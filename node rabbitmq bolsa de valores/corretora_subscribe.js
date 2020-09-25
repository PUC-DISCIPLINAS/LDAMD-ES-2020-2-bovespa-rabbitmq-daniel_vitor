#!/usr/bin/env node

require('dotenv').config();
// RabbitMQ connection string
const messageQueueConnectionString = process.env.CLOUDAMQP_URL;

// AMQP
// var amqp = require('amqplib/callback_api');
let amqp = require('amqplib');
const exchange = 'BROKER';
const queue = 'BROKER';

var args = process.argv.slice(2);

if (args.length == 0) {
  console.log("Usage: receive_logs_topic.js <facility>.<severity>");
  process.exit(1);
}

async function setup() {
  // connect to RabbitMQ Instance
  let connection = await amqp.connect(messageQueueConnectionString);
  // create a channel
  let channel = await connection.createChannel();
  // create exchange
  await channel.assertExchange(exchange, 'topic', {
    durable: false
  });
  //create queues
  await channel.assertQueue(queue, { exclusive: true });
  console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);

  await args.forEach((key) => {
     channel.bindQueue(queue, exchange, key);
  });
  await consume({ connection, channel, queue });
}

// Consume messages from RabbitMQ
function consume({ connection, channel, queue }) {
  return new Promise((resolve, reject) => {
    channel.consume(queue, async function (msg) {
      console.log(" [x] %s:'%s'", msg.fields.routingKey, msg.content);
      // acknowledge message as received
      await channel.ack(msg);
    });

    // handle connection closed
    connection.on("close", (err) => {
      return reject(err);
    })

    connection.on("error", (err) => {
      return reject(err);
    });
  });
}

setup();