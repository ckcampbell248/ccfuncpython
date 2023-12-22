import azure.functions as func
import logging

cycle_times_blueprint = func.Blueprint()

@cycle_times_blueprint.timer_trigger(schedule="*/1 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timerCycleTimeSimulator(myTimer: func.TimerRequest) -> None:
    import os
    import random
    import time
    import uuid
    import pyodbc
    from azure.iot.device import IoTHubDeviceClient, Message

    # Get connection strings
    DB_CONN_STRING = os.getenv("db_conn_str")
    IOT_HUB_CONNECTION_STRING =  os.getenv("iot_hub_conn_str")

    # Define the JSON message to send to IoT Hub.
    MSG_TXT = '{{"line": {line},"operation": {operation},"event": "{event}","op_corr_id": "{op_corr_id}","line_corr_id": "{line_corr_id}"}}'

    # Array to manage line status
    TOTAL_LINES = 5   # Total number of lines
    OPS_PER_LINE = 5  # Number of operations on each line
    DELAY = True # If True, introduce a random delay between events. 
    ln = 0
    lines = []

    # Class to keep track of current operation and status for each line
    class line:
        line_id = 0
        curr_op = 0
        status = False
        op_corr_id = ''
        line_corr_id = ''
        event = ''

    try:
        # Select a line at random and get its current status from the state database
        ln = random.randint(1, TOTAL_LINES)
        query = f"SELECT * FROM [functions].[line_state] WHERE [line_id] = {ln}"

        with pyodbc.connect(DB_CONN_STRING) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            curr_line = line()
            curr_line.line_id = rows[0][0]
            curr_line.curr_op = rows[0][1]
            curr_line.status = rows[0][2]
            curr_line.op_corr_id = rows[0][3]
            curr_line.line_corr_id = rows[0][4]
            curr_line.event = rows[0][5]

        # Update the line status    
        if curr_line.status == False:    # If the current state is stop
            curr_line.status = True

            if curr_line.curr_op < OPS_PER_LINE:    # If the operation number is less than the total ops per line
                curr_line.curr_op = curr_line.curr_op + 1    # Increment the operation

                curr_line.op_corr_id = uuid.uuid1().hex     # Assign a correlation id to the new operation cycle

                if curr_line.curr_op == 1:       # If this is the first operation, assign a correlation id to the line cycle
                    curr_line.line_corr_id = uuid.uuid1().hex

                # Build a new start message
                curr_line.event = 'start'
            else:
                curr_line.curr_op = 0
                curr_line.status = False
        else:   # Build a stop message
            line = curr_line.line_id
            curr_line.event = 'stop'
            curr_line.status = False            

        # Send the message to IoT Hub
        try: 
            msg_txt_formatted = MSG_TXT.format(line=curr_line.line_id, operation=curr_line.curr_op, event=curr_line.event, op_corr_id=curr_line.op_corr_id, line_corr_id=curr_line.line_corr_id)
            message = Message(msg_txt_formatted)
            client = IoTHubDeviceClient.create_from_connection_string(IOT_HUB_CONNECTION_STRING)
            client.send_message(message)
            client.disconnect()
            logging.info("Message sent: {}".format(message) )
        except Exception as e:
            logging.error(f"Failed to send message: {e}")

        # Update the state database
        query = f'''UPDATE [functions].[line_state] 
                    SET [curr_op] = {curr_line.curr_op}, 
                    [status] = {int(curr_line.status)}, 
                    [op_corr_id] = '{curr_line.op_corr_id}', 
                    [line_corr_id] = '{curr_line.line_corr_id}', 
                    [event] = '{curr_line.event}' 
                    WHERE [line_id] = {ln}'''
        
        with pyodbc.connect(DB_CONN_STRING) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

        # Add a random delay between messages
        if DELAY:
            if random.randint(1, 10) == 1:     # Periodically add extra time between messages
                time.sleep(random.randint(5, 10))
            else:
                time.sleep(random.randint(1, 3))

    except Exception as e:
            logging.error(f"An error occurred: {e}")
