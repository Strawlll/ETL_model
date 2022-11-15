from functools import wraps

# Вывод log-инфорамции в консоль
def logger(fn):
    from datetime import datetime, timezone

    @wraps(fn)
    def inner(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        print(f"» {fn.__doc__!r}. Logged at {called_at}")
        to_execute = fn(*args, **kwargs)
        print(f"        Выполнено. Logged at {called_at}")
        return to_execute
    return inner