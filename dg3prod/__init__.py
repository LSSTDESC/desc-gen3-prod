# descprod
import importlib.metadata
__version__ = importlib.metadata.version('desc-gen3-prod')

def version(show=False):
  if show: print(__version__)
  return __version__
