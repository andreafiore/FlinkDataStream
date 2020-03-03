# FlinkDataStream
This is a sample project to show the usage of Apache Flink.
In this example a stream is used to read data from sensor measurements (temperature, humidity, light, co2, occupancy) 
and aggregate them using a temporal window.

##Getting Started

###Prerequisites

In order to run the program you must have on your local machine Maven and Scala 2.11

###Installing

To install the program type in the root folder

```
mvn compile
```

###Testing

To run the tests type in the root folder

```
mvn test
```

###Run

To run the program type in the root folder

```
mvn exec:java -Dexec.mainClass="SensorDataJob" -Dexec.cleanupDaemonThreads=false
```

##Author
Andrea Fiore