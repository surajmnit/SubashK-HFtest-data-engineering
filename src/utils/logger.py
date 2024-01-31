import logging
import functools
import traceback
from time import perf_counter
from colorama import Fore, Style


class Logger:
    """
    The Logger class is a wrapper around the standard Python logging module.
    """

    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        date_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        formatter = logging.Formatter(log_format, datefmt=date_format)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

    def info(self, message: str) -> None:
        """
        Logs an info message
        """
        self.logger.info(message)

    def error(self, message: str) -> None:
        """
        Logs an error message
        """
        self.logger.error(message)

    def warning(self, message: str) -> None:
        """
        Logs a warning message
        """
        self.logger.warning(message)

    def exception(self, message: str, ex: Exception) -> None:
        """
        Logs an exception message along with its stack trace
        """
        stacktrace = "".join(traceback.TracebackException.from_exception(ex).format())
        self.logger.exception(
            f"{Fore.RED}{message}{Style.RESET_ALL} Exception: {str(ex)}. Stacktrace: {stacktrace}"
        )

    def benchmark(self, func):
        """Decorator to measure the execution time of a function."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                self.exception(f"Exception occurred in {func.__name__}", e)
                raise
            finally:
                end_time = perf_counter()
                run_time = end_time - start_time
                self.info(
                    f"Function {func.__name__} executed in {run_time:.4f} seconds"
                )
            return result

        return wrapper
