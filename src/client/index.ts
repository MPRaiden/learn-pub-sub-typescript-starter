import amqp from 'amqplib'
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from '../internal/gamelogic/gamelogic.js';
import { GameState } from '../internal/gamelogic/gamestate.js';
import { commandMove } from '../internal/gamelogic/move.js';
import { commandSpawn } from '../internal/gamelogic/spawn.js';
import { declareAndBind } from '../internal/pubsub/consume.js';
import { ExchangePerilDirect, PauseKey } from '../internal/routing/routing.js';

async function main() {
  const clientConnStr = "amqp://guest:guest@localhost:5672/"
  const connection = await amqp.connect(clientConnStr)

  const username = await clientWelcome()

  const [channel, queue] = await declareAndBind(connection, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, "transient")

  const gameState = new GameState(username)

  while (true) {
    const userInput = await getInput()
    if (!userInput.length) continue

    const command = userInput[0]

    if (command === "spawn") {
      try {
        commandSpawn(gameState, userInput)
      } catch (err) {
        console.log((err as Error).message)
      }
    } else if (command === "move") {
      try {
        commandMove(gameState, userInput)
      } catch (err) {
        console.log((err as Error).message)
      }
    } else if (command === "status") {
      commandStatus(gameState)
    } else if (command === "help") {
      printClientHelp()
    } else if (command === "spam") {
      console.log("Spamming not allowed yet!")
    } else if (command === "quit") {
      printQuit()
    } else {
      console.log("Unknown command")
      continue
    }

  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
