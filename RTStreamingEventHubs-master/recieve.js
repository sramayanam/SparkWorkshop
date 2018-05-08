const { EventHubClient, EventPosition } = require('azure-event-hubs');
const connectionString = "Endpoint=sb://srramstreaming.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=RzLafRLXxH6WDuaFrotYsQSGqfCafCTa9aCI+Wc+VBM=";
const entityPath = "realtimestream";

const client = EventHubClient.createFromConnectionString(connectionString,entityPath);

async function main() {
  const onError = (err) => {
    console.log("An error occurred on the receiver ", err);
  };
  
  const onMessage = (eventData) => {
    console.log(eventData.body);
    const enqueuedTime = eventData.annotations["x-opt-enqueued-time"];
    console.log("Enqueued Time: ", enqueuedTime);
  };

  const receiveHandler = client.receive("1", onMessage, onError, { eventPosition: EventPosition.fromEnqueuedTime(Date.now()) });

  // To stop receiving events later on...
  await receiveHandler.stop();
}

main().catch((err) => {
  console.log(err);
});