import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export function handlerPause(gameState: GameState): (playingState: PlayingState) => void {
  return function(playingState: PlayingState) {
    handlePause(gameState, playingState)
    process.stdout.write("> ");
  }
}

