import amqp from 'amqplib'
import { clientWelcome } from '../internal/gamelogic/gamelogic.js';

async function main() {
  const clientConnStr = "amqp://guest:guest@localhost:5672/"
  const connection = await amqp.connect(clientConnStr)
  console.log("Starting Peril client...");

  const username = await clientWelcome()
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
