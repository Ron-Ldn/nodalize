"""Static tool functions."""
import logging
import random
import string
from typing import Callable

already_logged_items = set()


def log_warning_once(key: str, message: str) -> None:
    """
    Log warning only once.

    Args:
        key: key to identify the warning
        message: actual message
    """
    # Note: it is technically possible to have 2 threads logging at the same time, but it will be unlikely and we
    # can accept a duplicated message. This will be more acceptable than slowing down the execution with a lock.
    if key not in already_logged_items:
        logging.warning(message)
        already_logged_items.add(key)


def generate_random_name(length: int = 10) -> str:
    """
    Generate a random name. Can be used in file names.

    Args:
        length: length of the name to create

    Returns:
        random name
    """
    # Make sure it starts with a letter
    name = random.choices(string.ascii_lowercase, k=1)[0]

    if length > 1:
        name += "".join(
            random.choices(string.ascii_lowercase + string.digits, k=length - 1)
        )

    return name


def generate_possible_name(validate_func: Callable, length: int = 10) -> str:
    """
    Generate a random name which satisfies a condition, expressed in an input function.

    Args:
        validate_func: function to validate the name
        length: length of the name to create

    Returns:
        random name
    """
    counter = 0
    name = generate_random_name(length)

    while not validate_func(name):
        name = generate_random_name(length)
        counter += 1

        if counter == 100:
            raise AssertionError("Failed to generate name")

    return name
