"""Chapter 73 helpers: build small packages, zips and archives to ship to Python workers.

Nothing here is Spark. It writes a package called `messytools` in a chosen version, zips it with
the package folder at the top of the archive, and builds a site folder with `pip install --target`
for the executors' platform.
"""
import os
import subprocess
import sys
import tarfile
import tempfile
import zipfile

WORK = os.environ.get("CH42_WORK") or tempfile.mkdtemp(prefix="ch73-")


def package(version, name="messytools", root=None):
    """Write <root>/<name>/__init__.py with VERSION = version. Returns root."""
    root = root or tempfile.mkdtemp(prefix=f"{name}-{version}-", dir=WORK)
    os.makedirs(f"{root}/{name}", exist_ok=True)
    with open(f"{root}/{name}/__init__.py", "w") as f:
        f.write(f'VERSION = "{version}"\n')
    return root


def zip_package(version, zip_name, name="messytools"):
    """A zip with <name>/__init__.py at the top, the layout zipimport needs."""
    root = package(version, name)
    path = os.path.join(WORK, zip_name)
    with zipfile.ZipFile(path, "w") as z:
        z.write(f"{root}/{name}/__init__.py", f"{name}/__init__.py")
    return path


def site_folder(packages, platform="manylinux2014_x86_64", py="3.12"):
    """pip install --target for the executors' platform and Python, wheels only."""
    target = os.path.join(WORK, "deps", "site-packages")
    subprocess.run([sys.executable, "-m", "pip", "--version"], check=False, capture_output=True)
    subprocess.run(["uv", "pip", "install", "--quiet", "--target", target,
                    "--python-platform", "x86_64-manylinux2014", "--python-version", py,
                    "--only-binary", ":all:", *packages], check=True)
    return target


def tar_folder(folder, out_name, arcname):
    path = os.path.join(WORK, out_name)
    with tarfile.open(path, "w:gz") as t:
        t.add(folder, arcname=arcname)
    return path


def zip_folder(folder, out_name):
    path = os.path.join(WORK, out_name)
    with zipfile.ZipFile(path, "w") as z:
        for d, _, files in os.walk(folder):
            for f in files:
                full = os.path.join(d, f)
                z.write(full, os.path.relpath(full, folder))
    return path
