import json
import sys
import time
from secrets import token_hex
from utility_module import RMQMessageSender, Utility
import pika
import sqlite3
from datetime import datetime, timezone, timedelta

totalfoundrows = 0
deviceinformation = ""
fulldeviceinformation = ""

class Send_EventRaw(RMQMessageSender):
    """Send data via backhaul rewrite module."""
    def __init__(self):
        self.chip_serial = Utility.get_serial()
        self.parameters = pika.ConnectionParameters(
            host='localhost',
            credentials=pika.PlainCredentials('guest', 'guest')
        )
        self.message_sender = RMQMessageSender()
        self.message_sender.init(self.parameters)

    def send_persistent_data(self, data, endtimestamp):
        """Send single record to RMQ."""
        # Convert integer or string timestamp to datetime
        if isinstance(endtimestamp, (int, float)):
            local_dt = datetime.fromtimestamp(endtimestamp)
            utc_dt = datetime.fromtimestamp(endtimestamp, tz=timezone.utc)
        else:
            try:
                local_dt = datetime.strptime(endtimestamp, "%Y-%m-%d %H:%M:%S")
                utc_dt = local_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                # If stored as UNIX timestamp string
                local_dt = datetime.fromtimestamp(float(endtimestamp))
                utc_dt = datetime.fromtimestamp(float(endtimestamp), tz=timezone.utc)

        data.update({
            'CameraSerial': self.chip_serial,
            'UploadedLocalDateTime': local_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'UploadedUTCDateTime': utc_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'EventStartLocalTime': local_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'EventStartUTCTime': utc_dt.strftime('%Y-%m-%d %H:%M:%S'),
        })

        body = {
            "type": "json",
            "data": json.dumps(data),
            "topic": 'ffc-eventrawdata'
        }
        
        
        if deviceinformation[:3] == '15F':
            self.message_sender.send_message(
                body=json.dumps(body),
                prop=pika.BasicProperties(
                    content_type="application/json"
                ),
                exchange_name='data',
                routing_key='backhaul.data'
            )
            print(f"[INFO-15F] Sent data for RoiId={data.get('RoiId')} MetricId={data.get('MetricId')}")
            
        elif deviceinformation[:3] in ('25F', '24J', '25X'):
            self.message_sender.send_message(
                body=json.dumps(body),
                prop=pika.BasicProperties(
                    content_type="application/json"
                ),
                exchange_name='data',
                routing_key='persistent.backhaul.data'
            )
            print(f"[INFO-25F] Sent data for RoiId={data.get('RoiId')} MetricId={data.get('MetricId')}")

        time.sleep(0.01)

    def close_connection(self):
        """Close RMQ connection."""
        self.message_sender.stop()
        print("[INFO] RabbitMQ connection closed.")

    def generate_data(self, start, end):
        """Select and send records between given start and end timestamps."""
        self.db_raw_path = "/home/pi/Raspicam/eventRaw.db"
        conn = sqlite3.connect(self.db_raw_path)
        cursor = conn.cursor()

        print(f"[INFO] Fetching records between {start} and {end}...")

        cursor.execute('''
            SELECT RegionID, MetricID, PeopleTypeID, PeopleID, EventStartTimeStamp, EventEndTimeStamp, CombineObjectTypeID
            FROM EventRaw
            WHERE EventStartTimeStamp >= ? AND EventStartTimeStamp <= ?
        ''', (start, end))

        rows = cursor.fetchall()

        global totalfoundrows
        totalfoundrows = len(rows)
        
        print(f"[INFO] Found {len(rows)} records in range.")

        for row in rows:
            data = {
                'RoiId': row[0],
                'MetricId': row[1],
                'PeopleTypeId': row[2],
                'PeopleId': row[3],
                'EventStartTime': row[4],
                'EventEndTime': row[5],
                'CombineObjectTypeId': row[6]
            }
            print(f"[DEBUG] Sending data: {data}")
            self.send_persistent_data(data, row[5])  # row[5] = EventEndTimeStamp
            time.sleep(0.01)  # optional delay between messages

        conn.close()
        print("[INFO] Database connection closed.")


    def get_device_data(self):
        self.db_raw_path = "/home/pi/Raspicam/raspicam"
        conn = sqlite3.connect(self.db_raw_path)
        cursor = conn.cursor()

        global deviceinformation, fulldeviceinformation
        
        cursor.execute('''
            SELECT * FROM camera;
        ''')

        rows = cursor.fetchall()
        
        for row in rows:
            fulldeviceinformation = row
            
        cursor.execute('''
            SELECT companyserial FROM camera;
        ''')

        rows = cursor.fetchall()
        
        for row in rows:
            deviceinformation = row[0]

        conn.close()
        print("[INFO] Database connection closed.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 send_eventraw.py <start_time> <end_time>")
        print('Example: python3 test.py 1761868800 1762127999')
        sys.exit(1)

    start_time = sys.argv[1]
    end_time = sys.argv[2]

    sender = Send_EventRaw()
    
    sender.get_device_data()
    
    print(f'[INFO] Device Info: {deviceinformation}')
    
    if deviceinformation[:3] == '15F':
        print(f'[INFO] 15F Device Detected')
    elif deviceinformation[:3] in ('25F', '24J', '25X'):
        print(f'[INFO] 25F Device Detected')
    
    try:
        sender.generate_data(start_time, end_time)
    finally:
        sender.close_connection()
        
    print(f'[INFO] Total rows of records found: {totalfoundrows}')
    print(f'[INFO] Device Info: {fulldeviceinformation}')
    print(f'[INFO] Device Company Serial: {deviceinformation}')
