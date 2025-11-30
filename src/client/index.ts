import amqp from 'amqplib'
import { clientWelcome } from '../internal/gamelogic/gamelogic.js';
import { declareAndBind } from '../internal/pubsub/consume.js';
import { ExchangePerilDirect, PauseKey } from '../internal/routing/routing.js';

async function main() {
  const clientConnStr = "amqp://guest:guest@localhost:5672/"
  const connection = await amqp.connect(clientConnStr)

  const username = await clientWelcome()

  const [channel, queue] = await declareAndBind(connection, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, "transient")
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
