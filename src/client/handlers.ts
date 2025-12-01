import { type ArmyMove } from "../internal/gamelogic/gamedata.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export function handlerPause(gameState: GameState): (playingState: PlayingState) => void {
  return function(playingState: PlayingState) {
    handlePause(gameState, playingState)
    process.stdout.write("> ");
  }
}

export function handlerMove(gameState: GameState): (move: ArmyMove) => MoveOutcome {
  return function(move: ArmyMove) {
    const moveOutcome = handleMove(gameState, move)
    process.stdout.write("> ");

    return moveOutcome
  }
}

