import os
import sys

import gzip

DELETED = 0
PRESENT = 1

file_prefix = "file_statistics."
file_suffix = ".data.gz"

if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.stdout.write("Not enough arguments.\n")
        sys.exit(-1)

    dir = sys.argv[1]
    path = os.path.join(dir, "rdiff-backup-data")

    if not os.path.exists(path):
        sys.stdout.write("Path does not look like a valid backup.\n")
        sys.exit(-1)

    rev_list = []
    raw_list = os.listdir(path)

    sys.stdout.write("Loading file list: ")

    for file in raw_list:
        sys.stdout.write(".")
        if file.find("file_statistics") == 0:
            raw_date = file.replace(file_prefix, "").replace(file_suffix, "")
            rev_list.append(raw_date)

    sys.stdout.write(" done loading.\n")

    sys.stdout.write("Sorting list: .")
    rev_list.sort()
    sys.stdout.write(" done sorting.\n")
    backup_dict = {}

    sys.stdout.write("Scanning file statistics: ")

    for revision in rev_list:

        sys.stdout.write(".")
        ##sys.stdout.write("Reading %s: " % revision)

        stats_file = gzip.open(os.path.join(path, "%s%s%s" % (file_prefix, revision, file_suffix)))
        ##stats_file = gzip.open("./test.gz")

        for raw_line in stats_file:
            ##sys.stdout.write(".")
            line = raw_line.strip()
            if not line.startswith("# "):
                temp_line = line.rsplit(" ", 4)
                backup_file = temp_line[0]
                if temp_line[2] == "NA":
                    temp_var, last_present = backup_dict.get(backup_file, (None, None))
                    file_status = (DELETED, last_present)
                else:
                    file_status = (PRESENT, revision)
                backup_dict[backup_file] = file_status

        stats_file.close()
        ##sys.stdout.write(" done revision.\n")

    sys.stdout.write(" done scanning.\n\n")

    for backup_file in backup_dict.keys():
        file_status, last_present = backup_dict[backup_file]
        if file_status == DELETED:
            sys.stdout.write("%s deleted, last present %s.\n" % (backup_file, last_present))

    ##print(backup_dict)
