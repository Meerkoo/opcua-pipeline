# opcua-pipeline

A Python-based pipeline for discovering and interacting with OPC UA servers, containerized with Docker for easy deployment. The stack also includes an InfluxDB time-series database for logging OPC UA data, enabling efficient storage, querying, and visualization of industrial data.

## Features

- **OPC UA Server Discovery:** Automatically scan and list available OPC UA servers on your network.
- **OPC UA Client:** Connect to OPC UA servers for reading, writing, and subscribing to node data.
- **InfluxDB Integration:** Store OPC UA data in a time-series database for analysis and visualization.
- **Containerized Deployment:** Easily run the entire stack using Docker and Docker Compose.
- **Configurable:** Manage settings via `.env` and JSON configuration files.

## Project Structure
opcua-pipeline/  
│  
├── configs/ # Configuration files (endpoints, nodes, credentials)  
├── .env # Environment variables  
├── Dockerfile # Python application Docker image  
├── docker-compose.yml # Multi-container orchestration (Python app + InfluxDB)  
├── discover.py # OPC UA server discovery script  
├── opcua_client.py # OPC UA client implementation  
├── requirements.txt # Python dependencies  


## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Python 3.8+ (only if running scripts outside Docker)

### Installation & Quick Start

1. **Clone the repository:**

    ```bash
    git clone https://github.com/Meerkoo/opcua-pipeline.git
    cd opcua-pipeline
    ```

2. **Configure environment variables:**

    - Edit the `.env` file to set OPC UA and InfluxDB connection parameters as needed.

3. **Start the stack with Docker Compose:**

    ```bash
    docker-compose up --build
    ```

    This command will build the Python app image and start both the OPC UA pipeline and InfluxDB services.

## InfluxDB Integration

- **InfluxDB** is included as a service in `docker-compose.yml`.
- The database is initialized with admin credentials, organization, and a bucket for OPC UA data.
- Data from OPC UA servers can be written to InfluxDB directly by the Python client or via an intermediary such as Telegraf (not included by default).
- InfluxDB UI is accessible at [http://localhost:8086](http://localhost:8086) using the credentials from your `.env` or `docker-compose.yml`.

**Example InfluxDB service in `docker-compose.yml`:**

```yaml
influxdb:
  image: influxdb:latest
  ports:
    - "8086:8086"
  environment:
    - DOCKER_INFLUXDB_INIT_MODE=setup
    - DOCKER_INFLUXDB_INIT_USERNAME=admin
    - DOCKER_INFLUXDB_INIT_PASSWORD=adminpassword
    - DOCKER_INFLUXDB_INIT_ORG=YourOrg
    - DOCKER_INFLUXDB_INIT_BUCKET=opcua_data
    - DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=yourtoken
  volumes:
    - ./influxdb-data:/var/lib/influxdb2
```

## Dependencies
- python-opcua: Python OPC UA client library.
- InfluxDB: Time-series database.

Other dependencies as specified in requirements.txt.

## Usage
- Use discover.py to scan for available OPC UA servers on your network.
- Use opcua_client.py to connect to a server and interact with its nodes (read/write/subscribe).

Collected data can be automatically or manually written to InfluxDB for storage and analysis.
