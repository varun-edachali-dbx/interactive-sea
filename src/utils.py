import threading

print_lock = threading.Lock()

def atomic_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)