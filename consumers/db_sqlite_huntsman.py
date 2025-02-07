import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Database path
DB_PATH = "alerts.db"

def get_alerts():
    """Fetch alerts from SQLite database."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM temperature_alerts ORDER BY timestamp DESC")
        alerts = cursor.fetchall()
    return alerts

def display_alerts():
    """Display the alerts in a Streamlit dashboard."""
    st.title("Smart Home - Real-Time Temperature Alerts")

    alerts = get_alerts()

    if not alerts:
        st.write("No alerts yet.")
    else:
        # Convert the alerts into a DataFrame for easier manipulation
        alert_df = pd.DataFrame(alerts, columns=["id", "timestamp", "temperature", "alert_type"])

        # Convert the timestamp to a readable format
        alert_df['timestamp'] = pd.to_datetime(alert_df['timestamp'], unit='s')

        # Display the alert data in a table
        st.subheader("Temperature Alerts Data")
        st.write(alert_df)

        # ---------------------------------------
        # Line Chart of Temperatures Over Time
        # ---------------------------------------
        st.subheader("Temperature Over Time")

        # Smooth the data by using a rolling window
        alert_df['smoothed_temp'] = alert_df['temperature'].rolling(window=5).mean()

        # Create a matplotlib figure and axis for the chart
        fig, ax = plt.subplots(figsize=(10, 6))

        # Plot the original temperature and smoothed temperature data
        ax.plot(alert_df['timestamp'], alert_df['smoothed_temp'], label='Smoothed Temperature', color='tab:blue', linestyle='-', marker='o', markersize=5)
        ax.plot(alert_df['timestamp'], alert_df['temperature'], label='Raw Temperature', color='tab:orange', linestyle='--', alpha=0.6)

        # Add titles, labels, and grid
        ax.set_title("Temperature Alerts Over Time", fontsize=16, weight='bold')
        ax.set_xlabel("Timestamp", fontsize=14)
        ax.set_ylabel("Temperature (°C)", fontsize=14)
        ax.grid(True, linestyle='--', alpha=0.5)

        # Rotate the timestamps for better readability
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Add a legend
        ax.legend()

        # Display the chart using Streamlit
        st.pyplot(fig)

        # -------------------------------
        # Display Additional Stats
        # -------------------------------
        st.subheader("Alert Summary")
        st.write(f"Total number of alerts: {len(alert_df)}")
        st.write(f"Max temperature recorded: {alert_df['temperature'].max()}°C")
        st.write(f"Min temperature recorded: {alert_df['temperature'].min()}°C")
        st.write(f"Average temperature: {alert_df['temperature'].mean():.2f}°C")

if __name__ == "__main__":
    display_alerts()  # Run the Streamlit dashboard
