# Serverside_mobileriz

This project is the server-side application for the **Serverside_mobileriz** case study. It provides a unified product catalog sourced from multiple vendors and is built using FastAPI, Kafka, Flink, and PostgreSQL, all containerized using Docker.

## Technologies Used

- **FastAPI**: A modern web framework for building APIs with Python 3.7+ based on standard Python type hints.
- **Kafka**: A distributed streaming platform used for building real-time data pipelines and streaming applications.
- **Flink**: A stream processing framework for real-time data processing and transformation.
- **PostgreSQL**: An open-source relational database management system.
- **Docker**: A platform for developing, shipping, and running applications in containers.

## Project Structure

The project consists of several key components:

- **FastAPI Application**: Handles API requests and integrates with Kafka and PostgreSQL.
- **Kafka**: Streams data between vendors and the FastAPI application.
- **Flink**: Processes and transforms streaming data in real time.
- **PostgreSQL Database**: Stores long-term data and analytics.

## Getting Started

### Prerequisites

- Docker installed on your machine.
- Docker Compose (optional, but recommended for managing multi-container applications).

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/abdullahghonaiem/Serverside_mobileriz.git
   cd Serverside_mobileriz
2. Build and start the containers:
   docker-compose up --build


