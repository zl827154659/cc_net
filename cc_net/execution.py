import itertools
import multiprocessing
import os
import warnings
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Sized

from typing_extensions import Protocol


class Executor(Protocol):
    def __call__(self, function: Callable[..., Optional[str]], *args: Iterable) -> None:
        ...


def get_executor(
    name: str,
    log_dir: Path,
    execution: str,
    timeout_hour: int = 1,
    mem_gb: int = 1,
    cpus: int = 1,
    task_parallelism: int = -1,
    options: dict = {},
) -> Executor:

    execution_mode = execution.split(",")[0]
    options.update(
        {kv.split("=", 1)[0]: kv.split("=", 1)[1] for kv in execution.split(",")[1:]}
    )

    if execution_mode == "mp":
        return MpExecutor(log_dir, cpus, task_parallelism)

    return debug_executor


def debug_executor(function: Callable[..., Optional[str]], *args: Iterable):
    approx_length = _approx_length(*args)
    for i, x in enumerate(zip(*args)):
        message = function(*x)
        print(message, f"({i + 1} / {approx_length})")


def _approx_length(*args: Iterable):
    for a in args:
        if isinstance(a, Sized):
            return len(a)
    return -1


GLOBAL_FUNCTIONS: Dict[str, Callable[..., Optional[str]]] = {}


def global_fn(args) -> Optional[str]:
    f_name = args[0]
    f = GLOBAL_FUNCTIONS[f_name]
    return f(*args[1:])


class MpExecutor(Executor):
    def __init__(self, log_dir: Path, cpus: int, task_parallelism: int):
        self.log_dir = log_dir
        if task_parallelism < 0:
            task_parallelism = os.cpu_count() or 1
        self.processes = min(task_parallelism // cpus, os.cpu_count())

    def __call__(self, function: Callable[..., Optional[str]], *args: Iterable):

        f_name = function.__name__
        global GLOBAL_FUNCTIONS
        if f_name in GLOBAL_FUNCTIONS:
            assert (
                function == GLOBAL_FUNCTIONS[f_name]
            ), f"Conflicting name between {function} and {GLOBAL_FUNCTIONS[f_name]}"
        else:
            GLOBAL_FUNCTIONS[f_name] = function

        approx_length = _approx_length(*args)

        print(
            f"Starting {f_name} over {self.processes} processes ({approx_length} tasks)."
        )
        with multiprocessing.Pool(processes=self.processes) as pool:
            i = 0
            for message in pool.imap_unordered(
                global_fn, zip(itertools.repeat(f_name), *args)
            ):
                i += 1
                print(message, f"({i} / {approx_length})")
