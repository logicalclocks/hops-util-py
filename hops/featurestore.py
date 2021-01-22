"""
A feature store client. This module exposes an API for interacting with feature stores in Hopsworks.
"""

import warnings


def fs_formatwarning(message, category, filename, lineno, line=None):
    return "{}:{}: {}: {}\n".format(filename, lineno, category.__name__, message)


class FeatureStoreDeprecationWarning(Warning):
    """A Warning to be raised when the featurestore module is imported."""
    pass

warnings.formatwarning = fs_formatwarning
warnings.simplefilter("always", FeatureStoreDeprecationWarning)

raise FeatureStoreDeprecationWarning("The `featurestore` module was deprecated with the"
    " introduction of the new `HSFS` client libraries to interact with the Hopsworks "
    " Feature Store. All functionality has been removed from this module.")
