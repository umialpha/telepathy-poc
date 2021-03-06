from functools import wraps
import time
def profile(logger):

    def wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            logger.info(
                "{0}.duration.ms {1}".format(func.__name__, int((time.time() - start_time) * 1000)),
                )
            return result

        return wrapper

    return wrap