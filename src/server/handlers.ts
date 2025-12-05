import { GameLog, writeLog } from "../internal/gamelogic/logs.js";
import { AckType } from "../internal/pubsub/consume.js";


export function handlerLogs() {
  return async (gameLog: GameLog): Promise<AckType> => {
    try {
      writeLog(gameLog);
      return AckType.Ack
    } catch (err) {
      console.error("Error while writting logs -> ", err)
      return AckType.NackDiscard
    } finally {
      process.stdout.write("> ")
    }
  }
}

