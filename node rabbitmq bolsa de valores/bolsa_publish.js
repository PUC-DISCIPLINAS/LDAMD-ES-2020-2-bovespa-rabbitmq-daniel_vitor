#!/usr/bin/env node

require('dotenv').config();
// RabbitMQ connection string
const messageQueueConnectionString = process.env.CLOUDAMQP_URL;

async function enviaTransacao(key, msg) {
  let amqp = require('amqplib');
  const exchange = 'BROKER';
    let connection = await amqp.connect(messageQueueConnectionString);
    let channel = await connection.createChannel();
    await channel.assertExchange(exchange, 'topic', { 
      durable: false 
    });
    await publishToChannel(channel, { exchangeName: exchange, routingKey: key, data: msg });
    console.log(" [x] Sent %s:'%s'", key, msg);
}

function publishToChannel(channel, { exchangeName, routingKey, data }) {
  return new Promise((resolve, reject) => {
    channel.publish(exchangeName, routingKey, Buffer.from(data), function(err, ok) {
      if(err) {
        return reject(err);
      }
      resolve();
    })
  });
}
// Exportar modulo
module.exports = enviaTransacao
