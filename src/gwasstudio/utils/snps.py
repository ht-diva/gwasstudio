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
