import socket
import os
import time
import pandas as pd
import multiprocessing

def connect_to_worker(worker_ip, worker_port):
    # Function to connect to a worker
    while True:
        try:
            worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            worker_socket.connect((worker_ip, worker_port))
            return worker_socket
        except OSError as e:
            if e.errno == 10048:  # WinError 10048
                print(f"Port {worker_port} is already in use. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print("An error occurred:", e)
                return None

def send_tasks_to_worker(worker_socket, tasks):
    # Function to send tasks to a worker
    for task in tasks:
        worker_socket.send(task.encode())
    worker_socket.send("DONE".encode())

def worker_finished(queue):
    # Function to signal that a worker has finished
    queue.put("Finished")

def main():
    # IP addresses and port numbers for workers
    worker1_ip = "192.168.0.102"  # Change this to worker1's IP address
    worker2_ip = "192.168.0.102"  # Change this to worker2's IP address
    port_worker1 = 3000  # Port for worker1
    port_worker2 = 3001  # Port for worker2

    # Measure total execution time for worker connections
    start_time_workers = time.time()

    # Connect to workers
    worker1_socket = connect_to_worker(worker1_ip, port_worker1)
    worker2_socket = connect_to_worker(worker2_ip, port_worker2)

    if worker1_socket is None or worker2_socket is None:
        print("Failed to connect to workers.")
        return

    # Send file location to workers
    fileloc = "C:\\Users\\vanashree g hegde\\OneDrive\\Desktop\\threads\\NCRB_Table_1C.csv"
    worker1_socket.send(fileloc.encode())
    worker2_socket.send(fileloc.encode())

    # Load data
    dafr = pd.read_csv("C:\\Users\\vanashree g hegde\\OneDrive\\Desktop\\threads\\NCRB_Table_1C.csv")
    numerical_data = dafr.select_dtypes(include=['float64', 'int64'])
    column_names = list(numerical_data.columns)

    # Divide tasks between workers
    num_columns = len(column_names)
    columns_per_worker = num_columns // 2

    tasks_worker1 = column_names[:columns_per_worker]
    tasks_worker2 = column_names[columns_per_worker:]

    # Send tasks to workers
    send_tasks_to_worker(worker1_socket, tasks_worker1)
    send_tasks_to_worker(worker2_socket, tasks_worker2)

    # Close connections
    worker1_socket.close()
    worker2_socket.close()

    # Measure total execution time for worker connections
    end_time_workers = time.time()

    # Calculate total execution time for worker connections
    execution_time_workers = end_time_workers - start_time_workers

    # Print total execution time for worker connections
    print("Total execution time for worker connections: {:.2f} seconds".format(execution_time_workers))

    # Measure total execution time for master script
    start_time_master = time.time()

    # Create multiprocessing Queue to receive signals from workers
    queue = multiprocessing.Queue()

    # Start a separate process to wait for worker1 to finish
    p1 = multiprocessing.Process(target=worker_finished, args=(queue,))
    p1.start()

    # Start a separate process to wait for worker2 to finish
    p2 = multiprocessing.Process(target=worker_finished, args=(queue,))
    p2.start()

    # Wait for both workers to finish
    p1.join()
    p2.join()

    # Calculate total execution time for master script
    end_time_master = time.time()

    # Calculate total execution time for master script
    execution_time_master = end_time_master - start_time_master

    # Print total execution time for master script
    print("Total execution time for master script: {:.2f} seconds".format(execution_time_master))

    # Save execution time to a file
    with open('execution_time_master.txt', 'w') as f:
        f.write("Total execution time for master script: {:.2f} seconds\n".format(execution_time_master))
        f.write("Total execution time for worker connections: {:.2f} seconds".format(execution_time_workers))

if __name__ == "__main__":
    main()
