import amqp, { type Channel } from 'amqplib'

export type SimpleQueueType = "durable" | "transient";

/**
 *Creates a new queue and binds it to an exchange.

 @param {amqp.ChannelModel} conn - TCP connection between node and RabbitMQ.
 @param {string} exchange - Exchange where routing occurs.
 @param {string} queueName - Name of the queue where the exchange routes the messages sent.
 @param {string} key - Routing key.
 @param {SimpleQueueType} queueType - Either durable or transient.
*/
export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {

  const channel = await conn.createConfirmChannel()

  const durable = true ? queueType === "durable" : false
  const autoDelete = true ? queueType === "transient" : false
  const exclusive = true ? queueType === "transient" : false

  const queue = await channel.assertQueue(queueName, { durable: durable, autoDelete: autoDelete, exclusive: exclusive })

  channel.bindQueue(queueName, exchange, key)

  return [channel, queue]
}

/**
  Subscribes consumer to the provided queue.
  
  @param {amqp.ChannelModel} conn - connection.
  @param {string} exchange - exchange that routes the message to the queue.
  @param {string} queueName - name of the queue.
  @param {string} key - routing key of the exchange.
  @param {SimpleQueueType} queueType - type of queue (durable or transient).
  @param {(data: T) => void} handler that uses the parsed message as its argument.
*/
export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
  const [ch, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
  );

  await ch.consume(queue.queue, function(msg: amqp.ConsumeMessage | null) {
    if (!msg) return;

    let data: T;
    try {
      data = JSON.parse(msg.content.toString());
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    handler(data);
    ch.ack(msg);
  });
}

