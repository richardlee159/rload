import matplotlib.pyplot as plt

data = open('results.txt').read()

timestamps = []
latencies = []

for line in data.strip().split("\n"):
    timestamp, latency = line.split("\t")
    timestamps.append(int(timestamp))
    latencies.append(int(latency))

# Plotting the scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(timestamps, latencies, color='blue')
plt.title("Latency Over Time")
plt.xlabel("Time (us)")
plt.ylabel("Latency (us)")
plt.show()
