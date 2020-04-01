"""Read the changelog for a given version."""

import re
import sys

if len(sys.argv) < 2:
    sys.exit(1)

with open("CHANGELOG.md", "r") as f:
    m = re.search("## v" + re.escape(sys.argv[1]) + "\n+(.+?)(?:##|$)", f.read(), re.DOTALL)
    if m is None:
        sys.exit(1)
    else:
        print(m.group(1).rstrip())
