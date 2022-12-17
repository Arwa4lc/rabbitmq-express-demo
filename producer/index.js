const express = require("express");
const amqp = require("amqplib");

const app = express();
app.use(express.json());

var channel, connection; //global variables
async function connectQueue() {
  try {
    const QUEUE_NAME = "square";
    const QUEUE_NAME2 = "multiply-by-two";
    const EXCHANGE_TYPE = "fanout";
    const EXCHANGE_NAME = "main";
    const KEY = "my.Routing.pattern";
    const KEY2 = "Key2";
    const routingPattern = "my.*.pattern";
    const numbers = ["1", "2", "3", "4", "5"];
    const number = "1";

    connection = await amqp.connect("amqp://localhost:5672");
    channel = await connection.createChannel();

    channel.assertExchange("main-exchange", "fanout", { durable: true });
    channel.assertExchange("dlx-exchange", "fanout", { durable: true });
    channel.assertExchange("ttl-exchange", "direct", { durable: true });

    channel.assertQueue("main-queue", { durable: true });
    channel.assertQueue("mainQueue-retry-1-30s", {
      durable: true,
      deadLetterExchange: "dlx-exchange",
      messageTtl: 30000,
    });
    channel.assertQueue("mainQueue-retry-2-1m", {
      durable: true,
      deadLetterExchange: "dlx-exchange",
      messageTtl: 60000,
    });
    channel.assertQueue("mainQueue-retry-3-3m", {
      durable: true,
      deadLetterExchange: "dlx-exchange",
      messageTtl: 180000,
    });

    channel.bindQueue("main-queue", "main-exchange");
    channel.bindQueue("main-queue", "dlx-exchange");
    channel.bindQueue("mainQueue-retry-1-30s", "ttl-exchange", "retry-1");
    channel.bindQueue("mainQueue-retry-2-1m", "ttl-exchange", "retry-2");
    channel.bindQueue("mainQueue-retry-3-3m", "ttl-exchange", "retry-3");

    const context = channel.publish(
      "main-exchange",
      undefined,
      Buffer.from(number)
    );
    console.log("🚀 ~ file: index.js:55 ~ connectQueue ~ context", context);
  } catch (error) {
    console.log(error);
  }
}
connectQueue();

async function sendData(data) {
  // send data to queue
  await channel.sendToQueue("test-queue", Buffer.from(JSON.stringify(data)));

  // close the channel and connection
  await channel.close();
  await connection.close();
}

app.get("/send-msg", (req, res) => {
  const data = {
    title: "Six of Crows",
    author: "Leigh Burdugo",
  };
  sendData(data); // pass the data to the function we defined
  console.log("A message is sent to queue");
  res.send("Message Sent"); //response to the API request
});

const PORT = process.env.PORT || 4004;
app.listen(PORT, () => console.log("Server running at port " + PORT));
