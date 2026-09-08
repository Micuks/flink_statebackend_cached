"""Copy entries without mutating the still-open source archive's ZipInfo offsets."""
import copy


def copy_entry(source, destination, info):
    destination.writestr(copy.copy(info), source.read(info.filename))
