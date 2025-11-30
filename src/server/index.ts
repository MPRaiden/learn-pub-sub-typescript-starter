import amqp from 'amqplib'
import { getInput, printServerHelp } from '../internal/gamelogic/gamelogic.js'
import { publishJSON } from '../internal/pubsub/publish.js'
import { ExchangePerilDirect, PauseKey, ResumeKey } from '../internal/routing/routing.js'

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

  while (true) {
    const userInput = await getInput()

    if (userInput.length === 0) continue

    const firstWord = userInput[0]

    if (firstWord === "pause") {
      console.log("Pause message sent")
      const ps = {
        isPaused: true
      }
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, ps)
      } catch (err) {
        console.error(`Error publishing Pause message -> ${err}`)
      }
    } else if (firstWord === "resume") {
      console.log("Resume message sent")
      const ps = {
        isPaused: false
      }
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, ResumeKey, ps)
      } catch (err) {
        console.error(`Error publishing Resume message -> ${err}`)
      }
    } else if (firstWord === "quit") {
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

