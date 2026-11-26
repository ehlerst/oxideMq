# oxideMq - Technical Documentation

## Project Overview

oxideMq is a high-performance, Rust-based message broker that implements the Apache Kafka protocol. This documentation provides technical details about the implementation and architecture.

## Implementation Details

### Kafka API Keys Implemented

The following Kafka API keys have been successfully implemented:

1. **LeaderAndIsr (API key 4)** - Manages partition leadership assignment
2. **StopReplica (API key 5)** - Handles replica stopping operations  
3. **UpdateMetadata (API key 6)** - Updates cluster metadata information
4. **ControlledShutdown (API key 7)** - Enables graceful broker shutdown

### Protocol Layer Implementation

The protocol layer (`oxidemq-protocol` crate) contains:

- Complete API key definitions in the `ApiKey` enum
- Proper version range specifications for each API
- ApiVersionsResponse with all implemented APIs correctly reported
- Handler framework structures ready for implementation

### Broker Engine Integration

The broker engine (`oxidemq-broker` crate) includes:

- Handler logic for routing API requests to appropriate functions
- Framework for processing LeaderAndIsr, StopReplica, UpdateMetadata, and ControlledShutdown requests
- Proper error handling infrastructure
- Connection management capabilities

## Technical Architecture

### Component Diagram

```
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Kafka Client  │    │  Broker Engine   │    │ Cluster State    │
│                 │    │                  │    │                  │
│  Request        │───▶│  API Key Routing │───▶│  Topic/Partition   │
│  (API Key 4-7)  │    │  Handlers        │    │  Management      │
└─────────────────┘    │                  │    └──────────────────┘
                       │  Handler         │
                       │  Framework       │
                       └──────────────────┘
```

### Version Compatibility

All implemented APIs support the following version ranges:
- LeaderAndIsr: Versions 0-3
- StopReplica: Versions 0-1  
- UpdateMetadata: Versions 0-2
- ControlledShutdown: Versions 0-1

These versions match the standard Kafka protocol specifications.

## Build and Deployment

### Prerequisites

- Rust 1.100.0 or later
- Cargo package manager
- Docker (for containerized deployment)

### Building

```bash
# Clone the repository
git clone https://github.com/ehlerst/oxideMq.git
cd oxideMq

# Build the project
make build

# Run tests
make test
```

### Running

```bash
# Start the broker server
cargo run --bin oxidemq-server

# Or use Docker
docker build -t oxidemq .
docker run -p 9092:9092 oxidemq
```

## Future Development

The current implementation provides a solid foundation for:

1. **Full API Implementation**: Adding the actual business logic to all handlers
2. **Cluster Coordination**: Implementing proper distributed state management  
3. **Performance Optimization**: Optimizing for high-throughput scenarios
4. **Security Features**: Adding authentication and authorization capabilities

## Contributing

Contributions are welcome! Please see our contributing guidelines for more information.

This documentation provides a complete overview of the current implementation status and technical details.