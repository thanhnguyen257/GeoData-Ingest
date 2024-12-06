import os
import shutil

def partition_file(directory):

    for root, _, files in os.walk(directory):
        if "TEMP" not in root and "HHOT" not in root:
            continue
        new_root1 = root.replace('json','json_with_partition')
        new_root2 = root.replace('json','json_without_partition')
        if not new_root1 or not new_root2:
            continue
        for file in files:
            filename_no_ext = file.replace('.json', '')
            station = "", "", ""
            if "TEMP" in root:
                _, station = filename_no_ext.split('_')
                destination1 = os.path.join(new_root1, f"station={station}")
            elif "HHOT" in root:
                _, station = filename_no_ext.split('_')
                destination1 = os.path.join(new_root1, f"station={station}")
            source = os.path.join(root, file)
            os.makedirs(destination1, exist_ok=True)
            destination1 = os.path.join(destination1, file)
            shutil.copy2(source, destination1)

            os.makedirs(new_root2, exist_ok=True)
            destination2 = os.path.join(new_root2, file)
            shutil.copy2(source, destination2)

if __name__ == "__main__":
    directory = "/mnt/mycephfs/demo/json"
    partition_file(directory)
