import re
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

def parse_log_line(line):
    log_pattern = re.compile(
        r'(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] "(?P<request>[^"]+)" (?P<status>\d{3}) (?P<size>\S+)'
    )
    match = log_pattern.match(line)
    if match:
        return match.groupdict()
    return None

def read_log_file(file_path):
    log_data = []
    with open(file_path, 'r') as file:
        for line in file:
            parsed_line = parse_log_line(line)
            if parsed_line:
                parsed_line['size'] = int(parsed_line['size'].replace('-', '0'))
                log_data.append(parsed_line)
    return pd.DataFrame(log_data)

def query_logs_from_elasticsearch(index, query):
    es = Elasticsearch(['http://localhost:9200'])  # Replace with your Elasticsearch instance URL
    logs = scan(es, index=index, query=query)
    log_data = []
    for log in logs:
        source = log['_source']
        log_data.append({
            'ip': source.get('ip', ''),
            'timestamp': source.get('timestamp', ''),
            'request': source.get('request', ''),
            'status': source.get('status', ''),
            'size': int(source.get('size', 0))
        })
    return pd.DataFrame(log_data)

def get_data(source_type, source):
    if source_type == 'file':
        return read_log_file(source)
    elif source_type == 'elasticsearch':
        index = source
        query = {
            "_source": ["ip", "timestamp", "request", "status", "size"],
            "query": {
                "match_all": {}
            }
        }
        return query_logs_from_elasticsearch(index, query)
    else:
        raise ValueError("Unsupported source type. Use 'file' or 'elasticsearch'.")

def main(source_type, source):
    df = get_data(source_type, source)

    # Feature engineering: convert timestamp to datetime, extract hour
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', format='%d/%b/%Y:%H:%M:%S %z')
    df['hour'] = df['timestamp'].dt.hour

    # Anomaly detection using Isolation Forest
    features = ['hour', 'size']
    X = df[features].fillna(0)

    model = IsolationForest(contamination=0.01)
    df['anomaly'] = model.fit_predict(X)

    # Plot the results
    fig, ax = plt.subplots(figsize=(12, 6))
    anomalies = df[df['anomaly'] == -1]

    ax.plot(df['timestamp'], df['size'], label='Normal')
    ax.scatter(anomalies['timestamp'], anomalies['size'], color='red', label='Anomaly')
    plt.xlabel('Timestamp')
    plt.ylabel('Response Size')
    plt.legend()
    plt.show()

    # Print detected anomalies
    print("Detected anomalies:")
    print(anomalies)

# Example usage:
# For local log file: main('file', 'access.log')
# For Elasticsearch: main('elasticsearch', 'logs_index')

if __name__ == "__main__":
    # Example: Change 'file' and 'access.log' to your needs
    main('file', 'access.log')
    # For Elasticsearch, use: main('elasticsearch', 'logs_index')
