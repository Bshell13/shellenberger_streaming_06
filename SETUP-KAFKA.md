## Set Up Kafka 

Follow the steps carefully. 
If anything doesn't work, do a web search, ask your favorite AI, and post what you learn in our discussion. 

## Step 0. If Windows, Install WSL (Windows Subsystem for Linux)

Install WSL and the Ubuntu distribution of Linux.

```shell
wsl --install -d Ubuntu
```

After installation, create a username and password for Ubuntu.

IMPORTANT: You will need your username and password later. Remember them or make a note and keep them safe. 

### Start WSL

Launch WSL. Open PowerShell and run the following command. 

```shell
wsl
```

-----

Use your terminal (Mac/Linux) or WSL terminal (Windows) to complete the following steps. 

## Step 1. Install Java 

Kafka requires Java to run. Install it with the following commands.

```zsh
sudo apt update
sudo apt install openjdk-17-jdk
java --version
```

If java is version 21 or higher than 17, run

```zsh
sudo update-alternatives --config java
```
And select the option for version 17. 

### Verify Java 17

In your WSL or Mac/Linux terminal, run the following.
Take a verification screenshot that clearly shows have openjdk 17 on your machine.

```zsh
java --version
```

## Step 2: Download Kafka

1. Visit the official Kafka website: [Kafka Downloads](https://kafka.apache.org/downloads).
2. Download the most recent Binary Download for Scala 2.13: e.g. kafka_2.13-3.9.0.tgz. 
3. Save it to your home directory 
   1. ~ on Mac/Linux
   2. On Windows, save using File Explorer. Change the following to your wsl username and paste it into the Explorer address bar: \\wsl.localhost\Ubuntu\home\denisecase

## Step 3: Extract Kafka to home directory

In your terminal (WSL/Mac/Linux):

1. Navigate to your home directory where you downloaded the file. 
2. List the file contents.
3. Extract the contents of the zipfile.

```zsh
cd ~
ls
tar -xvzf kafka_2.13-3.9.0.tgz -C ~/
ls
```

## Step 4: Rename the Folder (to kafka)

To simplify future commands, rename/move the extracted folder to ~/kafka:

```bash
mv ~/kafka_2.13-3.9.0 ~/kafka
ls
```

## Step 5: Configure Zookeeper

Zookeeper’s default configuration should work fine. You can review its configuration file at:
 ~/kafka/config/zookeeper.properties.


## Step 6: Configure Kafka & If Windows, Forward
Open Kafka’s configuration file at ~/kafka/config/server.properties
Copy and paste the contents of [docs/server.properties](docs/server.properties)
Save the file and exit.

### OPTIONAL & NOT EXPECTED TO BE NEEDED

   If Windows, forward the wsl port to local host by running the following command in wsl:
   ```
   sudo iptables -t nat -A PREROUTING -p tcp --dport 9092 -j DNAT --to-destination $(hostname -I | awk '{print $1}'):9092
   ```

   If windows, allow responses to the forwarded traffic by running the following command in wsl:
   ```zsh
   sudo iptables -t nat -A POSTROUTING -j MASQUERADE
   ```

## Step 7: Start Zookeeper Service (Terminal 1)

In a terminal (WSL/Mac/Linux):

1. Navigate to the Kafka directory.
2. Start Zookeeper service. 

```zsh
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Keep this terminal open while working with Kafka.


## Step 8: Start Kafka (Terminal 2)

Open a NEW terminal. If Windows, open PowerShell and run `wsl` to get a WSL terminal first.

1. Navigate to the Kafka directory.
2. Start Kafka service. 

```zsh
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

Keep this terminal open while working with Kafka. 

## Step 9: Test Kafka Installation (By Creating a Topic)

Open a NEW terminal. If Windows, open PowerShell and run `wsl` to get a WSL terminal first.

1. Navigate to the Kafka directory.
2. Create a topic named test-topic. 
3. List all available topics. 

```zsh
cd ~/kafka

bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

```

### Verify Kafka

Verify Kafka works by checking to see that the new test-topic has been created.

```
cd ~/kafka
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

You should see test-topic in the output.


## Recommended Resources

- [Kafka Quickstart Guide](https://kafka.apache.org/quickstart)