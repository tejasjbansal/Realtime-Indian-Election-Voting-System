# Realtime Indian Election Voting System

## Description
This project is a Realtime Election Voting System built using Python, Kafka, Spark Streaming, Postgres, and Streamlit. The system is containerized using Docker Compose, making it easy to set up and run all required services in Docker containers.

## Project Architecture
![Blank diagram](https://github.com/tejasjbansal/realtime-voting-data-engineering/assets/56173595/29376361-60d6-4ea6-afcd-7c68fe8bd4a2)


## System Flow
![System Flow](https://github.com/tejasjbansal/realtime-voting-data-engineering/assets/56173595/dd251fa7-67c4-4e9a-aaa7-2afae6720315)

### System Components
- **main.py**: This Python script initializes the required tables on Postgres (candidates, voters, and votes), creates a Kafka topic, and copies the votes table to the Kafka topic. It also handles consuming votes from the Kafka topic and producing data to the `voters_topic` on Kafka.
- **voting.py**: This script manages the logic for consuming votes from the Kafka topic (`voters_topic`), generates voting data, and produces data to the `votes_topic` on Kafka.
- **spark-streaming.py**: Here, the logic is implemented to consume votes from the Kafka topic (`votes_topic`), enrich the data from Postgres, aggregate the votes, and produce data to specific topics on Kafka.
- **streamlit-app.py**: This script handles consuming the aggregated voting data from the Kafka topic and Postgres, displaying the voting data in real-time using Streamlit.

## Technologies
- Python 3.9 or above
- Docker
- Postgres
- Kafka
- Pyspark
- Streamlit

## Screenshots

### Dashboard

This project is designed to provide a seamless real-time voting experience, and the technologies used ensure scalability, reliability, and performance.
