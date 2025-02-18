"""
consumer_shellenberger.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

Example Kafka message format:
{
    'datetime': 1/1/2012 0:00,
    'temperature_C': -1.8,
    'dewpoint_C': -3.9,
    'rel_humidity': 86,
    'wind_speed_km/h': 4,
    'visibility_km': 8,
    'pressure_kPa': 101.24,
    'weather': Fog
}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
import sqlite3 # used for data storage
import pandas as pd
import seaborn as sns

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("WEATHER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("WEATHER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures (empty lists)
#####################################

custom_df = pd.DataFrame(columns=['pressure_kPa', 'windspeed_km/h', 'weather'])

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()


#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """
    Update temperature vs. time chart.
    """
    # Clear the previous chart
    ax1.clear()
    ax2.clear()
    
    # Calculates the average windspeed and pressure for each weather type.
    avg_windpeed = custom_df.groupby(['weather'])['windspeed_km/h'].mean().reset_index()
    avg_pressure = custom_df.groupby(['weather'])['pressure_kPa'].mean().reset_index()

    # Create a bar chart using the plot() method
    # Use the weather for the x-axis and average pressure and average windspeed for the y-axis
    # There will be two y-axis tick marks for the different labels
    # This is for the average pressure plot
    sns.barplot(data = avg_pressure, x='weather', y='pressure_kPa', color="blue")
    ax1.set_ylabel('Average Pressure (kPa)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    
    # This is for the average windspeed plot
    sns.barplot(data = avg_windpeed, x='weather', y='windspeed_km/h', color="green")
    ax2.set_ylabel('Average Windspeed (km/h)', color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    # Use the built-in axes methods to set the labels and title
    ax1.set_xlabel("Type of Weather")
    ax1.set_title("Pressure and Windspeed for different weather types")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)  


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a JSON-transferred CSV message and check for stalls.

    Args:
        message (str): JSON message received from Kafka.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        wind_speed = data.get("wind_speed_km/h")
        pressure = data.get("pressure_kPa")
        weather = data.get('weather')
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if wind_speed is None or pressure is None or weather is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the timestamp and temperature to the chart data
        custom_df.loc[len(custom_df)] = ['pressure_kPa', 'windspeed_km/h', 'weather']

        # Update chart after processing this message
        update_chart()

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # Clear previous run's data
    custom_df.drop(custom_df.index, inplace=True)

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
    plt.ioff()  # Turn off interactive mode after completion
    plt.show()