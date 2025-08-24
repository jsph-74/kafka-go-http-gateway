# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Assistant Configuration
- Claude Code's name for this project: Pepe

## Project Overview

This is a Kafka HTTP Gateway project that sets up a multi-broker Kafka cluster with Schema Registry, Kafka Connect, and a Go HTTP gateway service. The architecture consists of containerized services orchestrated via Docker Compose.

## Development Commands

### Infrastructure Management
- **Start all services**: `docker-compose up -d`
- **Stop all services**: `docker-compose down`
- **View logs**: Use `bin/logs.sh` for service-specific logs
- **Build Go service**: Use `bin/build.sh` to build the Go HTTP gateway Docker image
- **Run Go service standalone**: Use `bin/run.sh` to run the Go container with hot-reloading via Air

### Kafka Operations
The `commands.sh` file contains comprehensive Kafka CLI commands for:
- Topic management (create, list, describe, delete)
- Producer/consumer operations with console tools
- Consumer group management and offset resets
- Kafka Connect connector management
- Schema Registry operations

Key Kafka connection details:
- Brokers: localhost:9092, localhost:9093, localhost:9094
- Schema Registry: localhost:8081
- Kafka Connect: localhost:8083

### Go Application Development
- **Container port**: 6969 (mapped from container to host)
- **Hot reload**: Enabled via Air (golang live-reloading tool)
- **Source directory**: `src/` (currently empty, ready for Go code)
- **Docker context**: Go 1.24 base image with Air for development

## Architecture

### Service Dependencies
1. **Zookeeper** (zk:2181) - Kafka coordination
2. **Kafka Brokers** (3 instances: broker0, broker1, broker2) - Message streaming
3. **Schema Registry** - Avro schema management
4. **Kafka Connect** - Data integration with MongoDB and DataGen connectors
5. **Go HTTP Gateway** - HTTP-to-Kafka bridge service
6. **CLI Tools** - Kafka administration utilities

### Key Configuration
- **Replication factor**: 3 (for fault tolerance)
- **Min in-sync replicas**: 2
- **Default partitions**: 15
- **Schema format**: Avro with Schema Registry integration
- **Connect plugins**: MongoDB connector and DataGen for testing

## Development Workflow

1. Use `docker-compose up -d` to start the Kafka ecosystem
2. Develop Go code in the `src/` directory
3. The Go service will auto-reload changes via Air
4. Use `commands.sh` examples for Kafka operations and testing
5. Access services via their respective ports for debugging and monitoring

The `src/` directory is volume-mounted for hot development, allowing real-time code changes without container rebuilds.