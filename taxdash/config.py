"""
Configuration constants for TaxDash application.

This module centralizes all configuration values that were previously hardcoded
throughout the codebase.
"""

# File encoding and parsing
ENCODING = 'latin-1'
DELIMITER = '|'
CHUNK_SIZE = 200_000

# Column counts for different file types
COLUMN_COUNT_CONTRIB = 40  # SPED Contribuições
COLUMN_COUNT_FISCAL = 42   # SPED Fiscal
COLUMN_COUNT_ECD = 40      # ECD

# Tax rates
PIS_COFINS_RATE = 0.0925  # Combined PIS/COFINS rate (9.25%)

# Parent register codes for SPED Contribuições
PARENT_REG_CONTRIB = [
    "0000", "0140", "A100", "C100", "C180", "C190", "C380", "C400", "C500",
    "C600", "C800", "D100", "D500", "F100", "F120", "F130", "F150", "F200",
    "F500", "F600", "F700", "F800", "I100", "M100", "M200", "M300", "M350",
    "M400", "M500", "M600", "M700", "M800", "P100", "P200", "1010", "1020",
    "1050", "1100", "1200", "1300", "1500", "1600", "1700", "1800", "1900"
]

# Parent register codes for SPED Fiscal
PARENT_REG_FISCAL = [
    "0000",
    "C100", "C300", "C350", "C400", "C495", "C500", "C600", "C700", "C800", "C860",
    "D100", "D300", "D350", "D400", "D500", "D600", "D695", "D700", "D750",
    "E100", "E200", "E300", "E500",
    "G110",
    "H005",
    "K100", "K200", "K210", "K220", "K230", "K250", "K260", "K270", "K280", "K290", "K300",
    "1100", "1200", "1300", "1350", "1390", "1400", "1500", "1600", "1601", "1700", "1800",
    "1900", "1960", "1970", "1980"
]

# Parent register codes for ECD
PARENT_REG_ECD = [
    "0000", "0001", "C001", "C040", "C050", "C150", "C600", "I001", "I010", "I050", "I150"
]
