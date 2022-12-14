const AmqpClient = require("amqplib");
const Promise = require("bluebird");
const contentTypeJson = "application/json";
const contentEncoding = "utf8";
const config = {
  exchanges: [
    { name: "A_COMMENT_CREATED", type: "fanout" },
    { name: "A_COMMENT_DELETED", type: "fanout" },
    { name: "A_COMMENT_UPDATED", type: "fanout" },
  ],
  bindings: [
    { exchange: "A_COMMENT_CREATED", target: "comments" },
    { exchange: "A_COMMENT_DELETED", target: "comments" },
    { exchange: "A_COMMENT_UPDATED", target: "comments" },
  ],
  queues: [{ name: "comments" }],
  retry_ttl_queues: [
    { name: "<queue_name>-retry-1-30s", delay: 30000 },
    { name: "<queue_name>-retry-2-10m", delay: 600000 },
    { name: "<queue_name>-retry-3-48h", delay: 195840000 },
  ],
};
let connectionPromise;
const amqpService = initAmqp();

amqpService.pConnect();
amqpService.pAssert().then(() => {
  amqpService.pConsume("comments", handleMsg).catch(console.error);

  // Do something important with our messages
  function handleMsg(msg, channel) {
    console.log("handleMsg");
    return Promise.resolve();
    // return Promise.reject();
    // throw new Error('Something wrong with handler');
  }
});

function initAmqp() {
  //   const port_str = config.port ? `:${config.port}` : "";
  //   const vhost_str = config.vhost ? `/${encodeURIComponent(config.vhost)}` : "";
  const url = "amqp://localhost:5672";
  const amqp = {
    // Init all the queues and exchanges if not exists
    pAssert() {
      return connectionPromise
        .catch(console.error)
        .then(({ channel }) => {
          // Make it in sync order (for clarity):
          // 1) assert exchanges
          // 2) assert queues
          // 3) bind exchanges to queues
          return assertExchanges()
            .then(assertQueues)
            .then(bindExchangesToQueues);

          function assertExchanges() {
            return Promise.all(
              []
                // "real" payload exchanges
                .concat(
                  config.exchanges.map((exchange) => {
                    return channel.assertExchange(
                      exchange.name,
                      exchange.type,
                      {
                        durable: true,
                      }
                    );
                  })
                )

                // DLX (one per "real" queue)
                .concat(
                  config.queues.map(({ name: queue }) => {
                    const dlxName = amqp._getDLXName({ queue });

                    return channel.assertExchange(dlxName, "fanout", {
                      durable: true,
                    });
                  })
                )

                // TTLX (one per "real" payload queue) - failed msgs goes to
                // this exchange first and than redirected to corresponding ttlq using
                // corresponding routing keys
                .concat(
                  config.queues.map(({ name: queue }) => {
                    const ttlxName = amqp._getTTLXName({ queue });

                    return channel.assertExchange(ttlxName, "direct", {
                      durable: true,
                    });
                  })
                )
            );
          }
          function assertQueues() {
            return Promise.all(
              [].concat(
                config.queues.map(({ name: queue }) => {
                  const dlxName = amqp._getDLXName({ queue });
                  return Promise.all(
                    []
                      // "real" payload queue
                      .concat(channel.assertQueue(queue, { durable: true }))

                      // a few ttl queues per one "real" queue
                      .concat(
                        config.retry_ttl_queues.map((ttl_queue, index) => {
                          const attempt = index + 1;
                          const ttlQueueName = amqp._getTTLQName({
                            queue,
                            attempt,
                          });

                          return channel.assertQueue(ttlQueueName, {
                            durable: true,
                            deadLetterExchange: dlxName, // x-dead-letter-exchange
                            messageTtl: ttl_queue.delay, // x-message-ttl
                            // we can use this key for decreasing queues amount:
                            // deadLetterRoutingKey: dlxName
                          });
                        })
                      )
                  );
                })
              )
            );
          }
          function bindExchangesToQueues() {
            console.log("binding");

            return Promise.all(
              []
                // bind "real" payload exchanges to "real" payload queues
                .concat(
                  config.bindings.map((bind) =>
                    channel.bindQueue(bind.target, bind.exchange)
                  )
                )

                // bind DLX and TTLX for retries
                .concat(
                  config.queues.map(({ name: queue }) => {
                    console.log(
                      "🚀 ~ file: amqp.js:177 ~ config.queues.map ~ queue",
                      queue
                    );

                    const dlxName = amqp._getDLXName({ queue });
                    console.log(
                      "🚀 ~ file: amqp.js:147 ~ config.queues.map ~ dlxName",
                      dlxName
                    );
                    const ttlxName = amqp._getTTLXName({ queue });
                    console.log(
                      "🚀 ~ file: amqp.js:149 ~ config.queues.map ~ ttlxName",
                      ttlxName
                    );

                    return Promise.all(
                      []
                        // DLX to "real" payload exchange
                        .concat(channel.bindQueue(queue, dlxName))

                        // TTLX to ttl queues
                        .concat(
                          config.retry_ttl_queues.map((ttl_queue, index) => {
                            const attempt = index + 1;
                            const ttlqName = amqp._getTTLQName({
                              queue,
                              attempt,
                            });
                            const routingKey = amqp._getTTLRoutingKey({
                              attempt,
                            });

                            return channel.bindQueue(
                              ttlqName,
                              ttlxName,
                              routingKey
                            );
                          })
                        )
                    );
                  })
                )
            );
          }
        })
        .catch(console.error);
    },
    pConsume(queue, handler, options = {}) {
      return connectionPromise
        .catch(amqp._connectionErrorHandler)
        .then(({ channel }) =>
          channel.consume(
            queue,
            (msg) => {
              return (
                new Promise((resolve, reject) => {
                  console.log("consume");
                  if (msg.fields.redelivered) {
                    console.log("rejeted");

                    reject(
                      "Message was redelivered, so something wrong happened"
                    );
                    return;
                  }

                  handler(msg, channel).then(resolve).catch(reject);
                })
                  .then(() => {
                    console.log("ack message");
                    channel.ack(msg);
                  })
                  // catch here allows us handle all the varieties of fails:
                  // - exceptions in handlers
                  // - rejects in handlers
                  // - redeliveries (server was down or something else)
                  .catch(handleRejectedMsg)
              );

              function handleRejectedMsg(reasonOfFail) {
                console.log("handleRejectedMsg");
                return amqp._sendMsgToRetry({
                  msg,
                  queue,
                  channel,
                  reasonOfFail,
                });
              }
            },
            options
          )
        );
    },
    pConnect() {
      connectionPromise = AmqpClient.connect(url)
        .catch(console.error)
        .then((cnx) =>
          cnx
            .createChannel()
            .catch(console.error)
            .then((channel) => {
              return { channel, connection: cnx };
            })
        )
        .catch(console.error);
      return connectionPromise;
    },

    // Ack original msg, create new one with TTL and send
    // to corresponding ttl queue where msg will be expired,
    // die and through DLX goes to next retry
    _sendMsgToRetry(args) {
      const channel = args.channel;
      const queue = args.queue;
      const msg = args.msg;
      const attempts_total = config.retry_ttl_queues.length;
      console.log(
        "🚀 ~ file: amqp.js:263 ~ _sendMsgToRetry ~ attempts_total",
        attempts_total
      );

      // ack original msg
      channel.ack(msg);

      // Unpack content, update and pack it back
      function getAttemptAndUpdatedContent(msg) {
        let content = JSON.parse(msg.content.toString(contentEncoding));
        console.log(
          "🚀 ~ file: amqp.js:271 ~ getAttemptAndUpdatedContent ~ content",
          content
        );

        // "exchange" field should exist, but who knows. in the other case we would have endless loop
        // cos native msg.fields.exchange will be changed after walking through DLX
        content.exchange = content.exchange || msg.fields.exchange;
        content.try_attempt = ++content.try_attempt || 1;
        // we don't rely on x-death, so write counter for sure
        const attempt = content.try_attempt;
        console.log(
          "🚀 ~ file: amqp.js:279 ~ getAttemptAndUpdatedContent ~ attempt",
          attempt
        );

        content = Buffer.from(JSON.stringify(content), contentEncoding);

        return { attempt, content };
      }

      const { attempt, content } = getAttemptAndUpdatedContent(msg);

      if (attempt <= attempts_total) {
        const ttlxName = amqp._getTTLXName({ queue });
        const routingKey = amqp._getTTLRoutingKey({ attempt });
        const options = {
          contentEncoding,
          contentType: contentTypeJson,
          persistent: true,
        };

        // trying to reproduce original message
        // including msg.properties.messageId and such
        // but excluding msg.fields.redelivered
        Object.keys(msg.properties).forEach((key) => {
          options[key] = msg.properties[key];
        });

        return channel.publish(ttlxName, routingKey, content, options);
      }

      return Promise.resolve();
    },

    _getTTLQName(options) {
      const queue = options.queue;
      const attempt = options.attempt || 1;

      return config.retry_ttl_queues[attempt - 1].name.replace(
        "<queue_name>",
        queue
      );
    },

    _getTTLRoutingKey(options) {
      const attempt = options.attempt || 1;

      return `retry-${attempt}`;
    },

    _getDLXName(options) {
      const queue = options.queue;

      return `DLX-${queue}`.replace(/-/g, "_").toUpperCase();
    },

    _getTTLXName(options) {
      const queue = options.queue;

      return `TTL-${queue}`.replace(/-/g, "_").toUpperCase();
    },
  };

  return amqp;
}
