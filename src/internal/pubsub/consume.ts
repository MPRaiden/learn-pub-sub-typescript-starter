import amqp, { type Channel } from 'amqplib'
import { decode } from "@msgpack/msgpack";

// export type SimpleQueueType = "durable" | "transient";
export enum SimpleQueueType {
  Durable,
  Transient
}

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

  const queue = await channel.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType !== SimpleQueueType.Durable,
    exclusive: queueType !== SimpleQueueType.Durable, arguments: { "x-dead-letter-exchange": "peril_dlx" }
  })

  channel.bindQueue(queueName, exchange, key)

  return [channel, queue]
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard
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
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString()))
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
export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => decode(data) as T)
}

/**
  Subscribes consumer to the provided queue.

  @param {amqp.ChannelModel} conn - connection.
  @param {string} exchange - exchange that routes the message to the queue.
  @param {string} queueName - name of the queue.
  @param {string} routingKey - routing key of the exchange.
  @param {SimpleQueueType} simpleQueueType - type of queue (durable or transient).
  @param {(data: T) => Promise<AckType> | AckType} handler that uses the parsed message as its argument.
  @param {(data: Buffer) => T} unmarshaller -
*/
export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  unmarshaller: (data: Buffer) => T,
): Promise<void> {
  const [ch, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
  )

  await ch.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (!msg) return

    let data: T
    try {
      data = unmarshaller(msg.content)
    } catch (err) {
      console.error("Could not unmarshal message:", err)
      return
    }

    try {
      const result = await handler(data)
      switch (result) {
        case AckType.Ack:
          ch.ack(msg)
          console.log("Ack")
          break
        case AckType.NackDiscard:
          ch.nack(msg, false, false)
          console.log("NackDiscard")
          break
        case AckType.NackRequeue:
          ch.nack(msg, false, true)
          console.log("NackRequeue")
          break
        default:
          const unreachable: never = result
          console.error("Unexpected ack type:", unreachable)
          return
      }
    } catch (err) {
      console.error("Error handling message:", err)
      ch.nack(msg, false, false)
      return
    }
  })
}

