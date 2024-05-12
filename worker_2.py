#worker2.py
import socket
import multiprocessing

def process_task(queue):
    while True:
        task = queue.get()
        if task == "DONE":
            break
        # Process the task (example: just return the task itself)
        result = "Processed task " + task
        print("Result:", result)

def main():
    # Port number for worker2
    port_worker2 = 3001  # Port for worker2

    # Create socket object for worker2
    worker2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Bind the socket to any available interface and port
        worker2_socket.bind(('0.0.0.0', port_worker2))

        # Listen for incoming connections
        worker2_socket.listen(1)
        print("Waiting for connection from master...")

        # Accept connection from master
        conn_master, addr_master = worker2_socket.accept()
        print("Connected to master:", addr_master)

        # Create a multiprocessing Queue
        queue = multiprocessing.Queue()

        # Start the task processing function in a separate process
        process = multiprocessing.Process(target=process_task, args=(queue,))
        process.start()

        while True:
            # Receive tasks from the master
            task = conn_master.recv(1024).decode()
            if not task:
                break
            print("Received task from master:", task)

            # Add task to the queue
            queue.put(task)

        print("Execution completed")

    except Exception as e:
        print("Error:", e)

    finally:
        # Close the connection and socket
        conn_master.close()
        worker2_socket.close()

if __name__ == "__main__":
    main()
