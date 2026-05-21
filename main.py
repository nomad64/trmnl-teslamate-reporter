import os
import logging
import schedule
import time
import threading
import requests
import paho.mqtt.client as mqtt

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("trmnl-teslamate-reporter")

# Load dev environment variables if available
try:
    from dotenv import load_dotenv
    load_dotenv()  # Development mode
except ImportError:
    pass           # Production mode

# Get configuration from environment variables
FETCH_FREQUENCY = int(os.environ.get("FETCH_FREQUENCY", "15"))
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
CAR_ID = int(os.environ.get("CAR_ID", "1"))

def fetch_data_mqtt():
    """Fetch data from MQTT with a timeout"""
    results = {}

    # Define topics and their default values if not received
    topic_defaults = {
        "state": "unknown",
        "battery_level": "0",
        "rated_battery_range_km": "0",
        "version": "unknown",
        "odometer": "0",
        "display_name": "Tesla",
        "charger_power": "0",
        "charger_voltage": "0"
    }
    
    topics = [f"teslamate/cars/{CAR_ID}/{k}" for k in topic_defaults.keys()]
    finished = threading.Event()

    def on_message(client, userdata, msg):
        try:
            if msg.payload:
                key = msg.topic.split("/")[-1]
                results[key] = msg.payload.decode().strip()
                logger.debug(f"Received {key}: {results[key]}")
            
            # If we have all topics, we can stop early
            if len(results) == len(topic_defaults):
                finished.set()
        except Exception as e:
            logger.error(f"Error processing message on {msg.topic}: {e}")

    try:
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_USER and MQTT_PASSWORD:
            client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        
        for topic in topics:
            client.subscribe(topic)

        client.loop_start()
        
        # Wait for messages (retained messages usually arrive almost instantly)
        # We wait up to 3 seconds to be safe
        finished.wait(timeout=3.0)
        
        client.loop_stop()
        client.disconnect()

        # Fill in defaults for any missing topics
        if len(results) < len(topic_defaults):
            for key, default_value in topic_defaults.items():
                if key not in results:
                    results[key] = default_value
                    logger.debug(f"Using default value for {key}: {default_value}")
            
            missing = set(topic_defaults.keys()) - set(results.keys())
            if missing: # Should be empty now, but for safety:
                logger.warning(f"MQTT fetch timed out. Defaulted: {', '.join(missing)}")

        # Convert km to miles
        try:
            if "rated_battery_range_km" in results:
                km = float(results["rated_battery_range_km"])
                results["rated_battery_range_mi"] = f"{km * 0.621371:.1f}"
            
            if "odometer" in results:
                km = float(results["odometer"])
                results["odometer_mi"] = f"{km * 0.621371:.1f}"
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting units: {e}")

    except Exception as e:
        logger.error(f"Error fetching data from MQTT: {e}")

    return results

def post_to_webhook(data):
    """Post data to webhook endpoint"""
    if not data:
        logger.warning("No data to send")
        return

    payload = {
        "merge_variables": data
    }

    try:
        logger.debug(f"POSTing payload: {payload}")
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            logger.info(f"Successfully posted {len(data)} records to webhook")
        else:
            logger.error(f"Webhook post failed with status {response.status_code}: {response.text}")
    except Exception as e:
        logger.error(f"Error posting to webhook: {e}")

def report_data():
    """Main function to report data from MQTT to webhook"""
    logger.info("Starting data reporter job")
    data = fetch_data_mqtt()
    if data:
        post_to_webhook(data)
        logger.info("Data reporter job completed")
    else:
        logger.warning("Data reporter job failed")

def start_scheduler():
    """Set up and start the scheduler"""
    logger.info("Starting scheduler")
    schedule.every(FETCH_FREQUENCY).minutes.do(report_data)

    # Run once immediately on startup
    report_data()

    # Keep the script running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute for pending jobs
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (CTRL+C). Exiting gracefully.")
        exit(0)

if __name__ == "__main__":
    if not WEBHOOK_URL:
        logger.critical("WEBHOOK_URL environment variable is not set. Exiting.")
        exit(1)

    logger.info("Reporter service starting up")
    start_scheduler()
