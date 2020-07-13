"""Python 2/3 Compatibility."""

import sys
import vine.five

sys.modules[__name__] = vine.five
