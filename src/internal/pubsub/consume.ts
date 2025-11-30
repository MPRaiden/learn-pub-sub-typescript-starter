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

