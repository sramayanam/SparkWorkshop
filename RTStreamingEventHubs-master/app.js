const eh = require("azure-event-hubs");
var RTM = require("satori-rtm-sdk");

var endpoint = "wss://open-data.api.satori.com";
var appKey = "6Ed2AafDC5A3C8D105DaB5dcAE9FAC92";
var channel = "wiki-rc-feed";
const connectionString = "Endpoint=sb://srramstreaming.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=RzLafRLXxH6WDuaFrotYsQSGqfCafCTa9aCI+Wc+VBM=";
const entityPath = "realtimestream";

var client = new RTM(endpoint, appKey);

client.on('enter-connected', function () {
    console.log('Connected to Satori RTM!');
});

var subscription = client.subscribe(channel, RTM.SubscriptionMode.SIMPLE, {
    //history: { age: 60 /* seconds */ },
    filter: 'SELECT * FROM `wiki-rc-feed` where change_size > 100'
    //WHERE is_bot = 0'
});

subscription.on('rtm/subscription/data', function (pdu) {
    pdu.body.messages.forEach(function (msg) {
        const ehclient = eh.EventHubClient.createFromConnectionString(connectionString, entityPath);
        if (msg.is_minor === false & msg.is_bot === false & (typeof msg.geo_ip != 'undefined')) {
            msg.recordReadDttm = Date(msg.time);
            console.log('Got message:', JSON.stringify(msg));
            //utf8Body = Buffer.from(JSON.stringify(msg)).toString("UTF8")
            //utf8Body = Buffer.from("Hello World!!!").toString("UTF8")
            //.toString('utf8')
            //var eventData = new EventData(JSON.stringify(msg));           
            async function sendEventData() {
                const data = {
                    body: msg
                };
               // payload["body"] = "hello world";               
                var delivery = await ehclient.send(data);
                //console.log("message sent successfully.", data);
            }

            sendEventData().catch((err) => {
                console.log(err);
            });
            ehclient.close();
        }
        //console.log('Got message:', JSON.stringify(msg));     
    });

});

client.start();