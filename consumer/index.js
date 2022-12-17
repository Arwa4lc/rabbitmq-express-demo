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
