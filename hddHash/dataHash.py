import json
import hashlib
import time
import os

class DataStore(object):
    def __init__(self, input_dir = "", output_dir_path = ""):
        in_dir_fix = input_dir
        out_dir_fix = output_dir_path
        if not isinstance(input_dir, bytes):
            in_dir_fix = str(input_dir).encode(encoding="utf-8")
        if not isinstance(output_dir_path, bytes):
            out_dir_fix = str(output_dir_path).encode(encoding="utf-8")
        self._index_name = os.path.basename(in_dir_fix.decode("utf-8"))
        self._in_dir = os.path.abspath(os.path.normpath(in_dir_fix.decode("utf-8")))
        self._out_dir = os.path.abspath(os.path.normpath(out_dir_fix.decode("utf-8")))
        self._my_data = {"files": [], "file_count" : 0, "GB" : 0.0}
        self._bad_data = {"bad_files" : [], "bad_files_count" : 0 }
        
    def _scan_dir(self, in_dir):
        has_dirs = False
        if os.path.isdir(in_dir):
            has_dirs = True
            
        while has_dirs:
            try:
                walker = os.walk(in_dir, topdown=True, onerror=None, followlinks=False)
            except Exception as exc:
                raise exc
            # run files first
            if walker:
                for base_path, dirs, files in walker:
                    # print("Checking basepath", base_path)
                    # print("Dirs", dirs)
                    # print("Files",files)
                    for file in files:
                        # has the files in multiple ways
                        if ":" in file or file.startswith("_"):
                            print("*************************", os.path.join(base_path, file))
                        full_path = os.path.join(base_path, file)
                        try:
                            path_hash = hashlib.md5(full_path).hexdigest()
                            file_hash = hashlib.md5(file).hexdigest()
                            file_size = os.path.getsize(full_path)
                            size_hash = file_size
                            file_in_mb = file_size / 1024.0 / 1024.0
                            file_create_date = os.path.getctime(full_path)
                            file_creation = time.ctime(file_create_date)
                            create_hash = file_create_date
                            file_modify_date = os.path.getmtime(full_path)
                            file_modified = time.ctime(file_modify_date)
                            modify_hash = file_modify_date
                            #
                            file_data = {"full_path" : full_path,
                                         "file_name" : file,
                                         "file_hash" : file_hash,
                                         "size" : (str(file_in_mb) + " MB"),
                                         "size_hash" : size_hash,
                                         "creation" : file_creation,
                                         "create_hash" : create_hash,
                                         "modified" : file_modified,
                                         "modify_hash" : modify_hash,
                                         }
                            self._my_data["files"].append(path_hash)
                            self._my_data["file_count"] += 1
                            self._my_data["GB"] += (file_in_mb / 1024)
                            self._write_data(file_data, str(path_hash))
                        except:
                            self._bad_data["bad_files"].append(full_path)
                            self._bad_data["bad_files_count"] += 1
                    if (not dirs):
                        # kill the process if no dirs exist
                        has_dirs = False
                    else:
                        # print(dirs)
                        for dir in dirs:
                            new_path = os.path.abspath(os.path.normpath(os.path.join(base_path, dir)))
                            # print("Checking new dir",new_path)
                            try:
                                self._scan_dir(new_path)
                            except:
                                # print("bad file",new_path)
                                self._bad_data["bad_files"].append(new_path)
                                self._bad_data["bad_files_count"] += 1
                
    def _write_data(self, input_data, file_name):
        try:
            with open(os.path.join(self._out_dir, (file_name + ".json")), "w") as f:
                json.dump(input_data, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            raise exc
        
    def buildData(self):
        self._scan_dir(self._in_dir)
        # print(self._my_data)
        # write out final dataset
        with open(os.path.join(self._out_dir, (self._index_name + ".json")), "w") as f:
            json.dump(self._my_data, f, ensure_ascii= False, indent=2)
            print("Data written {0}".format(os.path.join(self._out_dir, (self._index_name + ".json"))))
        # write bad data
        with open(os.path.join(self._out_dir, (self._index_name + "_bad.txt")), "wb") as f:
            f.write(b"bad_files:\n")
            # f.write(str(self._bad_data["bad_files"]))
            for data in self._bad_data["bad_files"]:
                f.write(bytes(str(data).encode(encoding="utf-8")))
                f.write(b"\n")
            f.write(b"\nbad_files_count: ")
            f.write(bytes(self._bad_data["bad_files_count"]) + b"\n")
            print("Data written {0}".format(os.path.join(self._out_dir, (self._index_name + "_bad.txt"))))
