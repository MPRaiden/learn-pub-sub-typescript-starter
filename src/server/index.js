import amqp from 'amqplib';
import { publishJSON } from '../internal/pubsub/publish.js';
import { ExchangePerilDirect, PauseKey } from '../internal/routing/routing.js';
async function main() {
    const connectionStr = "amqp://guest:guest@localhost:5672/";
    const connection = await amqp.connect(connectionStr);
    console.log("Successful connected to RabbitMQ");
    const confirmChannel = await connection.createConfirmChannel();
    const ps = {
        isPaused: true
    };
    await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, ps);
    process.on("SIGINT", async () => {
        try {
            await connection.close();
            console.log("Stopping connection to RabbitMQ");
        }
        catch (error) {
            console.error("Error while closing connection to RabbitMQ", error);
        }
        finally {
            process.exit(0);
        }
    });
}
main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
