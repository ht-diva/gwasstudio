"""
String utility helpers.
"""


def lower_and_replace(text: str) -> str:
    """
    Replaces spaces in the input string with underscores and converts it to lowercase.

    Args:
        text (str): The input string to be modified.

    Returns:
        str: The modified string with spaces replaced by underscores and converted to lowercase.
    """
    return f"{text.lower().replace(' ', '_')}"


def is_multiallelic(snpid: str) -> bool:
    """
    Check if a SNP is multi-allelic.

    A SNPID is multi-allelic if either EA or NEA has length > 1.

    Args:
        snpid (str): A SNPID with format CHR:POS:EA:NEA.

    Returns:
        bool: True if the SNPID is multi-allelic, False otherwise.

    Raises:
        ValueError: If SNPID has an unexpected format.
    """
    parts = snpid.split(":")
    if len(parts) != 4:
        raise ValueError(f"Invalid SNPID format '{snpid}'. Expected 'CHR:POS:EA:NEA'.")
    _, _, EA, NEA = parts

    return len(EA) > 1 or len(NEA) > 1
