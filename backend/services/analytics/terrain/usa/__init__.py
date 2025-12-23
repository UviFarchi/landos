# This is the orchestration layer that will check for the data layers in the db.
# if it doesn't find it, check if there is a cache for it.
# If there is check the validity of the cache.
# If there is not cache or it is expired, download it.
# Once downloaded cache the raw responses.
# Do the necessary transformations for the data layer.
# Load to the terrain collection on the db.
# The checks, download and transformations for each layer must be in a specific file for that layer.
# This file only orchestrates the calls to the checks, extractions, transformation and load functions.
