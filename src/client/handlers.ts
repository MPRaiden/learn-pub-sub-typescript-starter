import { type ArmyMove } from "../internal/gamelogic/gamedata.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";

export function handlerPause(gameState: GameState): (playingState: PlayingState) => AckType {
  return function(playingState: PlayingState) {
    handlePause(gameState, playingState)
    process.stdout.write("> ");
    
    return AckType.Ack
  }
}

export function handlerMove(gameState: GameState): (move: ArmyMove) => AckType {
  return function(move: ArmyMove) {
    const moveOutcome = handleMove(gameState, move)
    process.stdout.write("> ");

    const ackType = (moveOutcome === MoveOutcome.Safe || moveOutcome === MoveOutcome.MakeWar) ? AckType.Ack : AckType.NackDiscard

    return ackType
  }
}

