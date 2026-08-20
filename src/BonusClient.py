import socket
import threading

HOST = '127.0.0.1'
PORT = 5000

def receive_messages(sock):
    while True:
        try:
            data = sock.recv(1024)
            if not data: break
            print(f"\n says: {data.decode('utf-8').strip()}\nYou: ", end="")
        except: break

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print("Connected to Java Client!")

        # Start a thread to listen so input() doesn't block it
        threading.Thread(target=receive_messages, args=(s,), daemon=True).start()

        while True:
            msg = input("You: ")
            if msg:
                s.sendall((msg + "\n").encode('utf-8')) # Unicode
except ConnectionRefusedError:
    print("ERROR: Java program isn't listening!")
