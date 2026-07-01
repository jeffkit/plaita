"""plaita.logger — shared logger for the Plaita runtime.

Library logging policy:

* Do **not** attach any output handler (no ``StreamHandler``, no
  ``FileHandler``) at import time — writing ``plaita.log`` into the caller's
  CWD is a library anti-pattern.
* Do **not** force a log level — let the application decide via the standard
  ``logging`` configuration (root logger, ``plaita`` logger, or ``basicConfig``).
* Attach a single ``NullHandler`` to the top-level ``plaita`` logger so that
  applications which haven't configured logging see no spurious output, per
  the Python logging convention for libraries.

Application developers can enable output with, for example::

    import logging
    logging.basicConfig(level=logging.INFO)

or by configuring the ``plaita`` logger specifically.
"""
import logging

logger = logging.getLogger("plaita")

# Attach a NullHandler exactly once. Never add output handlers here.
if not any(isinstance(h, logging.NullHandler) for h in logger.handlers):
    logger.addHandler(logging.NullHandler())
