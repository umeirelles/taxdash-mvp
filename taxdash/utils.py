"""
Utility functions for DataFrame operations.

This module provides helper functions to reduce code duplication throughout
the application, particularly for common DataFrame transformations.
"""

import pandas as pd


def convert_numeric_columns(df, columns, inplace=True):
    """
    Convert specified columns to numeric type, coercing errors to NaN.

    Args:
        df: DataFrame to modify
        columns: List of column names to convert
        inplace: If True, modify df in place. If False, return modified copy.

    Returns:
        Modified DataFrame (or None if inplace=True)

    Example:
        convert_numeric_columns(df, ['4', '5', '6'])
    """
    if not inplace:
        df = df.copy()

    df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')

    if not inplace:
        return df


def clean_decimal_separators(df, columns, inplace=True):
    """
    Replace comma decimal separators with periods in specified columns.

    Args:
        df: DataFrame to modify
        columns: List of column names to clean
        inplace: If True, modify df in place. If False, return modified copy.

    Returns:
        Modified DataFrame (or None if inplace=True)

    Example:
        clean_decimal_separators(df, ['4', '5', '6'])
    """
    if not inplace:
        df = df.copy()

    df[columns] = df[columns].replace(',', '.', regex=True)

    if not inplace:
        return df


def clean_and_convert_numeric(df, columns, inplace=True):
    """
    Clean decimal separators and convert to numeric in one operation.
    Combines clean_decimal_separators() and convert_numeric_columns().

    Args:
        df: DataFrame to modify
        columns: List of column names to process
        inplace: If True, modify df in place. If False, return modified copy.

    Returns:
        Modified DataFrame (or None if inplace=True)

    Example:
        clean_and_convert_numeric(df, ['4', '5', '6', '8', '9'])
    """
    if not inplace:
        df = df.copy()

    # First clean decimal separators
    df[columns] = df[columns].replace(',', '.', regex=True)
    # Then convert to numeric
    df[columns] = df[columns].apply(pd.to_numeric, errors='coerce')

    if not inplace:
        return df
