### Python Azure Functions App
This Azure Functions project uses the Azure Functions v2 runtime and Python 3.11. 

#### Functions include:

##### **function_app.py**:
- *httpHelloWorld*: A simple HTTP trigger function that returns a message.
- *timerRandomSqlQueries*: A timer trigger function that executes a random SQL query against a SQL database to create simulated load. 

##### **cycle_times_blueprint.py**: 
- *timerCycleTimeSimulator*: A timer trigger function that simulates start and stop events for a manufacturing process. The function writes the events to an Azure IoT Hub. 

##### **iot_hub_trigger.py**: 
- *iothubUpdateTwins*: This function is triggered by IoT Hub (Event Hub) events. If the messages contain telemetry data from the MX Chip, it updates the digital twin model. 