import os

def print_tree(path, max_children=5, indent=""):
    try:
        entries = os.listdir(path)
        dirs = [entry for entry in entries if os.path.isdir(os.path.join(path, entry))]
        files = [entry for entry in entries if os.path.isfile(os.path.join(path, entry))]

        for d in dirs[:max_children]:
            print(f"{indent}├── {d}/")
            print_tree(os.path.join(path, d), max_children, indent + "│   ")

        for f in files[:max_children]:
            print(f"{indent}├── {f}")

        if len(dirs) > max_children:
            print(f"{indent}├── ... ({len(dirs) - max_children} more directories)")
        if len(files) > max_children:
            print(f"{indent}├── ... ({len(files) - max_children} more files)")
    except PermissionError:
        print(f"{indent}├── [Permission Denied]")
    except Exception as e:
        print(f"{indent}├── [Error: {str(e)}]")

if __name__ == "__main__":
    root_dir = "/mnt/mycephfs/demo/parquet_without_partition"
    print(f"{root_dir}/")
    print_tree(root_dir)
    print()
    root_dir = "/mnt/mycephfs/demo/parquet_with_partition"
    print(f"{root_dir}/")
    print_tree(root_dir)
