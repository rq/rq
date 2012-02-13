from __future__ import absolute_import
try:
    import cPickle as pickle
except ImportError:
    import pickle  # noqa

loads = pickle.loads
dumps = pickle.dumps
UnpicklingError = pickle.UnpicklingError
