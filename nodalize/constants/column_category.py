"""Enum to define category of column."""
from enum import Enum


class ColumnCategory(Enum):
    """Define category of column."""

    KEY = "Key"
    VALUE = "Value"
    PARAMETER = "Parameter"
    GENERIC = "Generic"
