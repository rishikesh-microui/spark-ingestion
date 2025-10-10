
import sys
import importlib
import ingestion  # make sure it's imported once

def reload_package(pkg_name: str):
    # collect already-imported modules that belong to the package
    to_reload = [
        name for name in sys.modules
        if name == pkg_name or name.startswith(pkg_name + ".")
    ]
    # reload leaves submodules first so parents see refreshed defs
    for name in sorted(to_reload, key=len, reverse=True):
        importlib.reload(sys.modules[name])

reload_package("ingestion")
reload_package("salam_ingest")
ingestion.run_cli(["--config", "/home/informaticaadmin/rishikesh/conf/brm.json"])

import ingestion
ingestion.run_cli(["--config", "/home/informaticaadmin/rishikesh/conf/brm.json"])