# EnOS_MessageIntegrationClient
An python MQTT client based on PAHO that connects to an EnOS Message Integration Endpoint to backfill historical data into EnOS TSDB.

To use the client, a Message Integration Endpoint has to be created on an EnOS Dev Portal:
![image](https://github.com/s1e9y0ng/EnOS_MessageIntegrationClient/assets/1212238/8bb011ef-1671-40fc-8e46-d1e44c9ca6c6)

With Message Integration endpoint created, copy the information including channelid, username, password and publish/subscribe topic into the config.json file.
![image](https://github.com/s1e9y0ng/EnOS_MessageIntegrationClient/assets/1212238/bf31e9cf-7a18-48bb-b434-f0f96f4ec42b)

For MQTT broker endpoint, input into config.json file without the port number

Copy the device key of the device to backfill data for into the config.json file (devicekey).

In the config.json file, specific the CSV file with the data to be ingested. the format of the CSV file should be 
column 1: timestamp in "yyyy-MM-dd hh:mm:ss" format 
column 2 and above: measurement point values to be updated.

the header of each column should reflect the measurement point of the device that should receive the data.
