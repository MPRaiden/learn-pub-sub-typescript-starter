import amqp, { ConfirmChannel } from 'amqplib'
import { clientWelcome, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit } from '../internal/gamelogic/gamelogic.js';
import { GameState } from '../internal/gamelogic/gamestate.js';
import { GameLog } from '../internal/gamelogic/logs.js';
import { commandMove } from '../internal/gamelogic/move.js';
import { commandSpawn } from '../internal/gamelogic/spawn.js';
import { SimpleQueueType, subscribeJSON } from '../internal/pubsub/consume.js';
import { publishJSON, publishMsgPack } from '../internal/pubsub/publish.js';
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from '../internal/routing/routing.js';
import { handlerMove, handlerPause, handlerWar } from './handlers.js';


export function publishGameLog(ch: ConfirmChannel, userName: string, msg: string) {
  const gl: GameLog = {
    username: userName,
    message: msg,
    currentTime: new Date()
  }

  publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${userName}`, gl)

}

async function main() {
  const clientConnStr = "amqp://guest:guest@localhost:5672/"
  const connection = await amqp.connect(clientConnStr)

  const username = await clientWelcome()
  const gameState = new GameState(username)
  const publishCh = await connection.createConfirmChannel()

  await subscribeJSON(connection, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState))
  await subscribeJSON(connection, ExchangePerilTopic, `army_moves.${username}`, 'army_moves.*', SimpleQueueType.Transient, handlerMove(gameState, publishCh))
  await subscribeJSON(connection, ExchangePerilTopic, WarRecognitionsPrefix, `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerWar(gameState, publishCh),
  )


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
      if (userInput.length < 2) {
        console.log("Please provide number of spam > ")
        continue
      }
      const secondCommandInput = userInput[1]
      const numOfSpam = Number(secondCommandInput)
      if (isNaN(numOfSpam)) {
        console.log("Please provide a valid number for spam command > ")
        continue
      }
      let spammed = 0
      while (numOfSpam > spammed) {
        spammed++
        const malicLogs = getMaliciousLog()
        publishGameLog(publishCh, username, malicLogs)
      }

      // console.log("Spamming not allowed yet!")
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
