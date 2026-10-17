# SimpleKafka+ 🚀
> A persistent, lightweight messaging broker built with Java 17 and Maven, inspired by Apache Kafka's log-structured storage architecture.

SimpleKafka+ implements the fundamental storage and streaming primitives of distributed messaging systems: append-only commit logs, monotonic sequential offsets, durable file persistence, zero-data-loss crash recovery, and high-throughput point/range message consumption.

---

## 🏗️ Architecture & Core Concepts

SimpleKafka+ implements the log-centric design popularized by Apache Kafka:

```
[Producer / HTTP Client]
       │
       │  POST /topics/{topic}/messages
       ▼
┌────────────────────────────────────────────────────────┐
│               SimpleKafka+ Broker                      │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │                    Topic                         │  │
│  │  - AtomicLong nextOffset                         │  │
│  │  - Thread-Safe In-Memory Index (Random Access)   │  │
│  └──────────────────────┬───────────────────────────┘  │
└─────────────────────────┼──────────────────────────────┘
                          │
         fsync / append   │
                          ▼
             ┌─────────────────────────┐
             │   Commit Log File       │
             │   (data/{topic}.log)    │
             │   [JSONL Records]       │
             └─────────────────────────┘
                          │
                          │  GET /topics/{topic}/messages?offset=N&limit=K
                          ▼
                [Consumer / HTTP Client]
```

### Key Engineering Decisions:
1. **Append-Only Commit Log**: Every published record is serialized as a JSONL (JSON Lines) record and appended to `data/<topic>.log`. Appends are sequential I/O, matching Kafka's core storage advantage.
2. **Deterministic Monotonic Offsets**: Every topic maintains an atomic offset sequence starting at `0`. Offsets are strictly sequential and immutable per topic.
3. **In-Memory Range Indexing**: Messages are held in an in-memory index for $O(1)$ random access lookups and efficient contiguous sublist slicing for pagination.
4. **Zero-Data-Loss Crash Recovery**: During broker initialization, existing log files in the data directory are scanned, replayed, and indexed. The topic's `nextOffset` is restored to `lastOffset + 1`, enabling seamless resume after process restarts.
5. **Standard Java 17**: Pure Java 17 without heavy frameworks (no Spring Boot). Powered by the high-performance embedded `com.sun.net.httpserver.HttpServer` with a fixed-size thread pool.

---

## 📋 Prerequisites
- **Java**: JDK 17+
- **Maven**: 3.9+
- **OS**: Windows / Linux / macOS (tested on Windows 11 PowerShell)

---

## ⚡ Quick Start (Windows PowerShell)

### 1. Build and Run Tests
```powershell
mvn clean test
```

### 2. Package Executable JAR
```powershell
mvn clean package
```

### 3. Start the SimpleKafka+ Broker
You can launch the server using Maven:
```powershell
mvn exec:java -Dexec.mainClass="com.simplekafka.SimpleKafkaServer"
```
Or directly run the compiled fat JAR:
```powershell
java -jar target/simple-kafka-plus-1.0.0.jar
```

The broker will start on `http://127.0.0.1:8000`.

---

## 📡 REST API Reference

All requests and responses use `application/json`.

### 1. Health Check
Checks broker uptime and total active topics.
- **Request**: `GET /health`
- **PowerShell Example**:
  ```powershell
  Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -Method Get
  ```
- **Response**: `200 OK`
  ```json
  {
    "status": "UP",
    "topicsCount": 0,
    "timestamp": 1729175400000
  }
  ```

---

### 2. Create Topic
Creates a new topic log. Fails with `409 Conflict` if the topic already exists.
- **Request**: `POST /topics`
- **PowerShell Example**:
  ```powershell
  $body = @{ name = "order-events" } | ConvertTo-Json
  Invoke-RestMethod -Uri "http://127.0.0.1:8000/topics" -Method Post -Body $body -ContentType "application/json"
  ```
- **Response**: `201 Created`
  ```json
  {
    "name": "order-events",
    "messageCount": 0,
    "nextOffset": 0
  }
  ```

---

### 3. List Topics
Returns metadata for all loaded topics.
- **Request**: `GET /topics`
- **PowerShell Example**:
  ```powershell
  Invoke-RestMethod -Uri "http://127.0.0.1:8000/topics" -Method Get
  ```
- **Response**: `200 OK`
  ```json
  [
    {
      "name": "order-events",
      "messageCount": 0,
      "nextOffset": 0
    }
  ]
  ```

---

### 4. Publish Message
Appends a message to the specified topic. Automatically assigns a unique UUID, timestamp, and sequential offset.
- **Request**: `POST /topics/{topic}/messages`
- **PowerShell Example**:
  ```powershell
  $msg = @{ key = "order-101"; value = "status: PLACED, total: $49.99" } | ConvertTo-Json
  Invoke-RestMethod -Uri "http://127.0.0.1:8000/topics/order-events/messages" -Method Post -Body $msg -ContentType "application/json"
  ```
- **Response**: `201 Created`
  ```json
  {
    "id": "e5c4a7f1-8288-46dc-a764-773dfec8ba19",
    "offset": 0,
    "timestamp": 1729175412345,
    "key": "order-101",
    "value": "status: PLACED, total: $49.99"
  }
  ```

---

### 5. Consume Messages
Retrieves messages starting from a given offset with pagination limit.
- **Request**: `GET /topics/{topic}/messages?offset={offset}&limit={limit}`
- **PowerShell Example**:
  ```powershell
  Invoke-RestMethod -Uri "http://127.0.0.1:8000/topics/order-events/messages?offset=0&limit=10" -Method Get
  ```
- **Response**: `200 OK`
  ```json
  {
    "topic": "order-events",
    "fromOffset": 0,
    "count": 1,
    "nextOffset": 1,
    "messages": [
      {
        "id": "e5c4a7f1-8288-46dc-a764-773dfec8ba19",
        "offset": 0,
        "timestamp": 1729175412345,
        "key": "order-101",
        "value": "status: PLACED, total: $49.99"
      }
    ]
  }
  ```

---

## 🧪 Automated Testing

The project includes unit and integration tests covering:
- Sequential offset generation and atomic increments
- Log commit to disk and zero-loss crash recovery after broker restart
- Topic validation, listing, and duplicate conflict handling
- Offset/limit slicing and boundary checks
- HTTP REST status codes (200, 201, 400, 404, 405, 409)

Execute tests via:
```powershell
mvn test
```

---

## 📁 Project Structure

```
D:\SimpleKafkaPlus\
├── pom.xml                                      # Maven configuration (Java 17, Jackson, JUnit 5)
├── README.md                                    # Documentation and quickstart
├── .gitignore                                   # Standard exclusions (target/, data/, IDE files)
├── src/
│   ├── main/
│   │   └── java/com/simplekafka/
│   │       ├── SimpleKafkaServer.java           # Server bootstrap and shutdown hooks
│   │       ├── broker/
│   │       │   ├── Broker.java                  # Central broker coordinator
│   │       │   ├── TopicAlreadyExistsException.java
│   │       │   └── TopicNotFoundException.java
│   │       ├── model/
│   │       │   ├── Message.java                 # Immutable message model with offset
│   │       │   └── TopicInfo.java               # Topic metadata record
│   │       ├── server/
│   │       │   ├── HttpServerApp.java           # Embedded HTTP server & routing
│   │       │   └── dto/
│   │       │       ├── CreateTopicRequest.java
│   │       │       ├── PublishRequest.java
│   │       │       └── ConsumeResponse.java
│   │       └── storage/
│   │           └── Topic.java                   # Append-only log file & in-memory index
│   └── test/
│       └── java/com/simplekafka/
│           ├── broker/
│           │   └── BrokerTest.java              # Unit tests for broker, offsets, persistence
│           └── server/
│               └── HttpServerAppTest.java       # HTTP endpoint integration tests
```
