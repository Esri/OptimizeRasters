# ------------------------------------------------------------------------------
# Copyright 2024 Esri
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------
# Name: CleanMRFCache.py
# Description: Cleans MRF Cache files by oldest access-time until free space
# requested has been achieved.
# Version: 20250317
# Requirements: Python
# Required Arguments: -input
# Optional Arguments: -mode -ext -size
# e.g.: -mode = [del,scan], -ext=txt,mrfcache -input=d:/mrfcache_folder -percentcleanup=50
# Usage: python.exe CleanMRFCache.py <arguments>
# Author: Esri Imagery Workflows team
# ------------------------------------------------------------------------------
#!/usr/bin/env python

import sys
import argparse
import os
import ctypes


def log_message(msg):
    """Prints message and flushes stdout for real-time output."""
    print(msg)
    sys.stdout.flush()


class Cleaner:
    """
    Scans and cleans files in a directory based on access time.

    Attributes:
        input_path (str): Target directory.
        extensions (set): File extensions to filter.
        file_info (list): Metadata of scanned files (path, size, access time).

    Methods:
        get_free_disk_space(path): Returns total and available disk space in bytes.
        get_file_info(root_only=False): Scans for matching files and stores metadata.
    """

    def __init__(self, input_path, extensions=None):
        self.input_path = os.path.normpath(input_path) + os.sep
        self.extensions = set(extensions or [])
        self.file_info = []

    @staticmethod
    def get_free_disk_space(path):
        """Returns total and available disk space in bytes."""
        try:
            total_space = ctypes.c_ulonglong(0)
            free_space = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(path), None,
                ctypes.pointer(total_space),
                ctypes.pointer(free_space)
            )
            return total_space.value, free_space.value
        except Exception:
            return -1, -1

    def get_file_info(self, root_only=False):
        """Scans the directory and collects file information."""
        log_message("[Scanning files]...")
        for root, _, files in os.walk(self.input_path):
            if root_only and root != self.input_path:
                continue

            for file in files:
                _, ext = os.path.splitext(file)
                if ext.lstrip(".").lower() in self.extensions:
                    file_path = os.path.join(root, file)
                    try:
                        self.file_info.append({
                            "f": file_path,
                            "s": os.path.getsize(file_path),
                            "at": os.path.getatime(file_path)
                        })
                    except Exception as exp:
                        log_message(f"Err: {exp}")
        return bool(self.file_info)


def main():
    __program_ver__ = "v1.0"
    __program_name__ = f"CleanMRFCache.py {__program_ver__}"

    parser = argparse.ArgumentParser(
        description="Cleans MRF Cache files based on access-time until the required free space is achieved."
    )
    parser.add_argument("-input", required=True, help="Input directory", dest="input_path")
    parser.add_argument("-mode", help="Processing mode. Valid mode: [del]", dest="mode", default="scan")
    parser.add_argument("-ext", help="Extensions to filter (comma-separated). e.g., -ext=mrfcache,txt", dest="ext")
    parser.add_argument("-size", type=int, help="Free space required in GB (default: 2GB)", dest="size", default=2)
    parser.add_argument("-percentcleanup", type=int, help="Percentage of disk to clean (1-100)", dest="percentcleanup")

    args = parser.parse_args()

    log_message(__program_name__)
    log_message(parser.description)

    # Validate extensions
    extensions = {"mrfcache"}
    if args.ext:
        extensions.update(map(str.strip, args.ext.lower().split(",")))

    # Initialize Cleaner
    cleaner = Cleaner(args.input_path, extensions)

    # Retrieve disk space
    total_disk_space, space_available = cleaner.get_free_disk_space(os.path.dirname(args.input_path))
    if space_available == -1:
        log_message(f"Err: Unable to determine free disk space for {args.input_path}")
        exit(1)
        
    if total_disk_space == -1:
        log_message(f"Err: Unable to determine total disk space for {args.input_path}")
        exit(1)

    # Calculate space to free
    if args.percentcleanup:
        if not (1 <= args.percentcleanup <= 100):
            log_message("Err: -percentcleanup must be between 1 and 100.")
            exit(1)
        space_to_free = (args.percentcleanup / 100) * total_disk_space
    else:
        space_to_free = args.size * 1_000_000_000  # Convert GB to bytes

    if space_available >= space_to_free:
        log_message("The disk already has the requested free space.")
        log_message(f"Total space available: {space_available / (1024 * 1024):.2f} MB")
        exit(0)

    log_message(f"Mode: {args.mode.lower()}")

    if not cleaner.get_file_info():
        log_message("Err: No matching files found.")
        exit(1)

    # Sort files by access time (oldest first)
    process = sorted(cleaner.file_info, key=lambda x: x["at"])
    total_savings = 0

    for file_info in process:
        file_path, size, atime = file_info["f"], file_info["s"], file_info["at"]
        print(f"{file_path} [{size} bytes] [atime: {atime}]")
        total_savings += size

        if args.mode.lower() == "del":
            try:
                os.remove(file_path)
                space_available += size
                log_message(f"[Deleted] {file_path}")

                if space_available >= space_to_free:
                    log_message("\nRequired disk space has been freed.")
                    break
            except OSError as e:
                log_message(f"Err: Unable to delete {file_path}. Skipping... ({e})")

    log_message(f"\nPotential savings: {total_savings} bytes.")
    if args.mode.lower() == "del":
        log_message(f"Total space freed: {space_available} bytes")
        if space_available < space_to_free:
            log_message("\nUnable to free the requested space.")

    log_message("\nDone.")


if __name__ == "__main__":
    main()
