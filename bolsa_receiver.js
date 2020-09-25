#!/usr/bin/env node
require('dotenv').config();
// RabbitMQ connection string
const messageQueueConnectionString = process.env.CLOUDAMQP_URL;
const amqp = require('amqplib/callback_api');
const Livro = require('./livro')

var args = process.argv.slice(2);

var myJSON = '{"venda": [], "compra": []}';
var myObj = JSON.parse(myJSON);

// Log de estado
if (args.length == 0) {
  console.log("Usage: receive_logs_topic.js <facility>.<severity>");
  process.exit(1);
}

// Abrir conexão AMQP
amqp.connect(messageQueueConnectionString, function(error0, connection) {
  if (error0) {
    throw error0;
  }
  connection.createChannel(function(error1, channel) {
    if (error1) {
      throw error1;
    }

    const exchange = 'BOLSADEVALORES';
    channel.assertExchange(exchange, 'topic', {
      durable: false
    });

    const queue = 'BOLSADEVALORES';
    
    channel.assertQueue(queue, {
      exclusive: true
    }, function(error2, q) {
      if (error2) {
        throw error2;
      }
      console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
      let livroObj = new Livro()

      args.forEach(function(key) {
        channel.bindQueue(q.queue, exchange, key);
      });

      channel.consume(q.queue, function(msg) {
        console.log(" [x] %s:'%s'", msg.fields.routingKey, msg.content.toString())

        let sentido = msg.fields.routingKey.split(".")[0]
        let mensagem = JSON.parse(msg.content.toString())
        mensagem.ativo = msg.fields.routingKey.split(".")[1]


        channel.assertExchange('BROKER', 'topic', {
          durable: false
        });
        channel.publish('BROKER', msg.fields.routingKey, Buffer.from(livroObj.adicionaOrdem(sentido, mensagem).toString()));

      }, {
        noAck: true
      });
    });
  });
});
