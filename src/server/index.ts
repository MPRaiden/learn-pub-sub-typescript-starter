import amqp from 'amqplib'
import { getInput, printServerHelp } from '../internal/gamelogic/gamelogic.js'
import { SimpleQueueType, subscribeMsgPack } from '../internal/pubsub/consume.js'
import { publishJSON } from '../internal/pubsub/publish.js'
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from '../internal/routing/routing.js'
import { handlerLogs } from './handlers.js'


async function main() {
  const connectionStr = "amqp://guest:guest@localhost:5672/"
  const connection = await amqp.connect(connectionStr)

  printServerHelp()

  process.on("SIGINT", async () => {
    try {
      await connection.close()
      console.log("Stopping connection to RabbitMQ")
    } catch (error) {
      console.error("Error while closing connection to RabbitMQ", error)
    } finally {
      process.exit(0)
    }
  })

  const confirmChannel = await connection.createConfirmChannel()

  await subscribeMsgPack(connection, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable, handlerLogs())

  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.")
    return
  }

  while (true) {
    const userInput = await getInput()

    if (!userInput.length) continue

    const command = userInput[0]

    if (command === "pause") {
      console.log("Pause message sent")
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: true
        })
      } catch (err) {
        console.error(`Error publishing Pause message -> ${err}`)
      }
    } else if (command === "resume") {
      console.log("Resume message sent")
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: false
        })
      } catch (err) {
        console.error(`Error publishing Resume message -> ${err}`)
      }
    } else if (command === "quit") {
      console.log("Exiting server")
      process.exit(0)
    } else {
      console.log("Unknown command")
    }
  }
}


main().catch((err) => {
  console.error("Fatal error:", err)
  process.exit(1)
})

