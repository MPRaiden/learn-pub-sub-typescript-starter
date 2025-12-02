import amqp from 'amqplib'
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from '../internal/gamelogic/gamelogic.js';
import { GameState } from '../internal/gamelogic/gamestate.js';
import { commandMove } from '../internal/gamelogic/move.js';
import { commandSpawn } from '../internal/gamelogic/spawn.js';
import { SimpleQueueType, subscribeJSON } from '../internal/pubsub/consume.js';
import { publishJSON } from '../internal/pubsub/publish.js';
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey } from '../internal/routing/routing.js';
import { handlerMove, handlerPause } from './handlers.js';


async function main() {
  const clientConnStr = "amqp://guest:guest@localhost:5672/"
  const connection = await amqp.connect(clientConnStr)

  const username = await clientWelcome()
  const gameState = new GameState(username)
  const publishCh = await connection.createConfirmChannel()

  await subscribeJSON(connection, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState))
  await subscribeJSON(connection, ExchangePerilTopic, `army_moves.${username}`, 'army_moves.*', SimpleQueueType.Transient, handlerMove(gameState))


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
        const move = commandMove(gameState, userInput)
        publishJSON(
          publishCh,
          ExchangePerilTopic,
          `army_moves.${username}`,
          move,
        );
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
