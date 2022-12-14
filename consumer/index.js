const express = require("express");
const amqp = require("amqplib");
const app = express();

app.use(express.json());

var channel, connection;

connectQueue(); // call the connect function

async function connectQueue() {
  try {
    const QUEUE_NAME = "square";
    const QUEUE_NAME2 = "multiply-by-two";

    connection = await amqp.connect("amqp://localhost:5672");
    channel = await connection.createChannel();

    await channel.consume("main-queue", (msg) => {
      console.log("msg");
      console.log(
        "🚀 ~ file: index.js:22 ~ newPromise ~ msg.fields.redelivered",
        msg.fields.redelivered
      );

      return new Promise((resolve, reject) => {
        if (msg.fields.redelivered) {
          reject("Message was redelivered, so something wrong happened");
          return;
        }

        resolve();
        // reject();
        // throw new Error('Something wrong with handler');
      })
        .then(() => {
          console.log("msg ack");
          channel.ack(msg);
        })
        .catch(handleRejectedMsg);

      function handleRejectedMsg(reasonOfFail) {
        return _sendMsgToRetry({
          msg,
          queue: "main-queue",
          channel,
          reasonOfFail,
        });
      }
      // if (msg.fields.redelivered) {
      //   channel.reject(msg, false);
      // }

      // return _sendMsgToRetry({
      //   msg,
      //   queue: "main-queue",
      //   channel,
      //   reasonOfFail: "Message was redelivered, so something wrong happened",
      // });
      // console.log("message ack1");
      // channel.ack(msg);
      // return (
      //   new Promise((resolve, reject) => {
      //

      //     // resolve();
      //     // reject("Something wrong with handler");
      //     // throw new Error('Something wrong with handler');
      //     return channel.reject(msg, false);
      //   })
      //     .then(() => {
      //
      //     })
      //     // catch here allows us handle all the varieties of fails:
      //     // - exceptions in handlers
      //     // - rejects in handlers
      //     // - redeliveries (server was down or something else)
      //     .catch((reasonOfFail) => {
      //       return _sendMsgToRetry({
      //         msg,
      //         queue: "main-queue",
      //         channel,
      //         reasonOfFail,
      //       });
      //     })
      // );
    });
  } catch (error) {
    console.log(error);
  }
}

const data = {};

function _sendMsgToRetry(args) {
  const channel = args.channel;
  const queue = args.queue;
  const msg = args.msg;
  const attempts_total = 3;
  // ack original msg
  channel.ack(msg);

  // Unpack content, update and pack it back
  function getAttemptAndUpdatedContent(msg) {
    data.content = JSON.parse(msg.content.toString("utf8"));

    // "exchange" field should exist, but who knows. in the other case we would have endless loop
    // cos native msg.fields.exchange will be changed after walking through DLX
    data.exchange = data.exchange || msg.fields.exchange;
    data.try_attempt = ++data.try_attempt || 1;
    console.log(
      "🚀 ~ file: index.js:77 ~ getAttemptAndUpdatedContent ~ data.try_attempt",
      data.try_attempt
    );
    // we don't rely on x-death, so write counter for sure
    const attempt = data.try_attempt;

    data.content = Buffer.from(JSON.stringify(data.content), "utf8");

    return data;
  }

  const { try_attempt, content } = getAttemptAndUpdatedContent(msg);
  console.log("🚀 ~ file: index.js:127 ~ _sendMsgToRetry ~ content", content);
  console.log(
    "🚀 ~ file: index.js:127 ~ _sendMsgToRetry ~ try_attempt",
    try_attempt
  );

  if (try_attempt <= attempts_total) {
    // const ttlxName = _getTTLXName({ queue });
    // console.log(
    //   "🚀 ~ file: index.js:111 ~ _sendMsgToRetry ~ ttlxName",
    //   ttlxName
    // );
    const routingKey = _getTTLRoutingKey({ attempt: try_attempt });
    console.log(
      "🚀 ~ file: index.js:99 ~ _sendMsgToRetry ~ routingKey",
      routingKey
    );
    const options = {
      contentEncoding: "utf8",
      contentType: "application/json",
      persistent: true,
    };

    // trying to reproduce original message
    // including msg.properties.messageId and such
    // but excluding msg.fields.redelivered
    Object.keys(msg.properties).forEach((key) => {
      options[key] = msg.properties[key];
    });

    return channel.publish("ttl-exchange", routingKey, content, options);
  }

  return resolve();
}

function _getTTLRoutingKey(options) {
  console.log("🚀 ~ file: index.js:158 ~ _getTTLRoutingKey ~ options", options);
  const attempt = options.attempt || 1;

  return `retry-${attempt}`;
}

const PORT = process.env.PORT || 4002;
app.listen(PORT, () => console.log("Server running at port " + PORT));

// await channel.assertQueue("test-queue");

// channel.consume("test-queue", (data) => {
//   console.log(`${Buffer.from(data?.content)}`);
//   channel.ack(data);
// });
// channel.consume(QUEUE_NAME, (m) => {
//   const number = parseInt(m.content.toString());
//   const square = number * number;
//   console.log("---->square", square);
//   channel.ack(m);
// });

// channel.consume(QUEUE_NAME2, (m) => {
//   const number = parseInt(m.content.toString());
//   const multiply = number * 2;
//   console.log("---->multiplied by 2", multiply);
//   channel.ack(m);
// });

// channel.consume("mainexchangequeue", (message) => {
//   const number = parseInt(message.content.toString());
//   console.log("🚀 ~ channel.consuming ");
//   const multiply = number * 2;
//   console.log("---->multiplied by 2", multiply);
//   channel.nackAll();
//   // channel.reject(message, false);
// });

// channel.consume("deadletterqueue", (message) => {
//   const number = parseInt(message.content.toString());
//   const square = number * number;
//   console.log("---->square", square);
//   channel.ack(message);
// });
