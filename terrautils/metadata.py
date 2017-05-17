"""Metadata

This module provides useful reference methods for cleaning metadata.
"""

def get_fixed_metadata(sensor):
    """Get fixed sensor metadata from Clowder.
    """
    pass

def clean_metadata(json):
    """Return cleaned json object with updated structure and names.
    """
    pass

def _validate(metadata):
    """Validate metadata JSON object or notify of errors (e.g. via Slack).
    """

def get_metadata(clowderhost, clowderkey, datasetid):
    """Return Clowder metadata + fixed metadata + any other stuff
    """
    pass

def get_preferred_synonym(variable):
    """Execute a thesaurus check to see if input variable has alternate preferred name.
    """
