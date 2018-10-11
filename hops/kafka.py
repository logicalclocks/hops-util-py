"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

def getEndpoint():
    """

    Returns:

    """
    return os.environ['KAFKA_ENDPOINT']