# utils/decorators.py
import functools
from typing import Any, Callable, List
from colorama import Fore, Style
from .logger import Logger
from time import perf_counter


def benchmark(func: Callable[..., Any]) -> Callable[..., Any]:
    logger = Logger("HelloFresh Recipes Processing")

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = perf_counter()
        value = func(*args, **kwargs)
        end_time = perf_counter()
        run_time = end_time - start_time
        logger.info(
            f"{Fore.YELLOW}Execution of {func.__name__}({args_summary(args)}) took {run_time:.2f} seconds.{Style.RESET_ALL}"
        )
        return value

    return wrapper


def args_summary(args: List[Any]) -> str:
    return ", ".join([str(arg)[:20] if arg else "" for arg in args])


def with_logging(func: Callable[..., Any]) -> Callable[..., Any]:
    logger = Logger("HelloFresh Recipes Processing")

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        summary = args_summary(args)
        logger.info(f"{Fore.GREEN}Calling {func.__name__}({summary}){Style.RESET_ALL}")
        try:
            value = func(*args, **kwargs)
        except Exception as ex:
            logger.exception(
                f"{Fore.RED}Exception raised in {func.__qualname__}.{func.__name__}",
                ex,
            )
            raise ex
        else:
            logger.info(f"{Fore.GREEN}Finished {func.__name__}{Style.RESET_ALL}")
            return value

    return wrapper
