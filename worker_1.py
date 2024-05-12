import socket
import multiprocessing
import time

def process_task(queue):
    while True:
        task = queue.get()
        if task == "DONE":
            break
        # Process the task (example: just return the task itself)
        result = "Processed task " + task
        print("Result:", result)

def main():
    # Port number for worker1
    port_worker1 = 3000  # Port for worker1

    # Create socket object for worker1
    worker1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Bind the socket to any available interface and port
        worker1_socket.bind(('0.0.0.0', port_worker1))

        # Listen for incoming connections
        worker1_socket.listen(1)
        print("Waiting for connection from master...")

        # Accept connection from master
        conn_master, addr_master = worker1_socket.accept()
        print("Connected to master:", addr_master)

        # Create a multiprocessing Queue
        queue = multiprocessing.Queue()

        # Start the task processing function in a separate process
        process = multiprocessing.Process(target=process_task, args=(queue,))
        process.start()

        # Record start time
        start_time = time.time()

        while True:
            # Receive tasks from the master
            task = conn_master.recv(1024).decode()
            if not task:
                break
            print("Received task from master:", task)

            # Add task to the queue
            queue.put(task)

        # Send a termination signal to the task processing function
        queue.put("DONE")

        # Wait for the task processing function to finish
        process.join()

        # Record end time
        end_time = time.time()

        # Calculate execution time
        execution_time = end_time - start_time
        print("Execution completed in {:.2f} seconds".format(execution_time))

    except Exception as e:
        print("Error:", e)

    finally:
        # Close the connection and socket if they are defined
        if 'conn_master' in locals():
            conn_master.close()
        if 'worker1_socket' in locals():
            worker1_socket.close()

if __name__ == "__main__":
    main()
